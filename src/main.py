import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import psycopg2

DATA_DIR = Path("/data")
TMP_DIR = Path("/app/tmp_pipeline")
ZIP_PATTERN = "Empresas*.zip"

# Processa um zip por vez (extrai -> transforma -> carrega -> apaga) pra manter o
# cache de página do container baixo — orçamento é 1 GB e RAM pesa no score (peso 0.25).
DUCKDB_MEMORY_LIMIT = "250MB"
DUCKDB_THREADS = 2

COPY_COLUMNS = (
    "cnpj_basico", "razao_social", "natureza_juridica",
    "qualificacao_responsavel", "capital_social", "porte_codigo",
    "porte_descricao", "ente_federativo", "capital_social_faixa",
    "is_mei", "natureza_juridica_grupo", "ente_federativo_presente",
    "data_processamento",
)


def get_env():
    required = ["PARTICIPANTE", "PG_USER", "PG_PASSWORD"]
    env = {}
    for key in required:
        env[key] = os.environ[key]
    env["PG_TABLE"] = os.environ.get("PG_TABLE", f"{env['PARTICIPANTE']}_empresas")
    env["PG_HOST"] = os.environ.get("PG_HOST", "postgres_db")
    env["PG_PORT"] = os.environ.get("PG_PORT", "5432")
    env["PG_DB"] = os.environ.get("PG_DB", "db_empresas")
    return env


def create_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                cnpj_basico VARCHAR(8),
                razao_social VARCHAR,
                natureza_juridica VARCHAR(4),
                qualificacao_responsavel VARCHAR,
                capital_social DOUBLE PRECISION,
                porte_codigo VARCHAR(2),
                porte_descricao VARCHAR,
                ente_federativo VARCHAR,
                capital_social_faixa VARCHAR,
                is_mei BOOLEAN,
                natureza_juridica_grupo VARCHAR,
                ente_federativo_presente BOOLEAN,
                data_processamento TIMESTAMP
            )
        """)
        conn.commit()


def extract_csv(zip_path):
    """DuckDB não lê de dentro de .zip — extrai o .EMPRECSV pra disco (I/O puro, sem parsing)."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".EMPRECSV")), None)
        if not csv_name:
            return None
        out_path = TMP_DIR / f"{zip_path.stem}.csv"
        with zf.open(csv_name, "r") as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst, length=4 * 1024 * 1024)
        return out_path


def transform_with_duckdb(extracted_file, now, out_path):
    """Lê o CSV original (ISO-8859-1) com o parser vetorizado do DuckDB e aplica todas as
    regras de negócio em SQL. all_varchar evita que o DuckDB infira tipos e derrube zeros
    à esquerda. O dedup de cnpj_basico entre arquivos é feito depois, uma vez, no Postgres."""
    sql = f"""
    COPY (
        WITH raw AS (
            SELECT * FROM read_csv(
                '{extracted_file.as_posix()}',
                delim=';', quote='"', header=false, encoding='latin-1',
                all_varchar=true, null_padding=true, ignore_errors=true,
                names=['cnpj_basico','razao_social','natureza_juridica','qualificacao_responsavel','capital_social','porte_codigo','ente_federativo']
            )
        ),
        parsed AS (
            SELECT
                lpad(trim(COALESCE(cnpj_basico, '')), 8, '0') AS cnpj_basico,
                upper(trim(COALESCE(razao_social, ''))) AS razao_social,
                lpad(trim(COALESCE(natureza_juridica, '')), 4, '0') AS natureza_juridica,
                trim(COALESCE(qualificacao_responsavel, '')) AS qualificacao_responsavel,
                COALESCE(TRY_CAST(replace(replace(trim(COALESCE(capital_social, '')), '.', ''), ',', '.') AS DOUBLE), 0.0) AS capital_social,
                CASE WHEN trim(COALESCE(porte_codigo, '')) IN ('00','01','03','05') THEN trim(porte_codigo) ELSE '00' END AS porte_codigo,
                NULLIF(trim(COALESCE(ente_federativo, '')), '') AS ente_federativo
            FROM raw
        )
        SELECT
            cnpj_basico,
            razao_social,
            natureza_juridica,
            qualificacao_responsavel,
            capital_social,
            porte_codigo,
            CASE porte_codigo
                WHEN '00' THEN 'NÃO INFORMADO'
                WHEN '01' THEN 'MICRO EMPRESA'
                WHEN '03' THEN 'EMPRESA DE PEQUENO PORTE'
                WHEN '05' THEN 'DEMAIS'
            END AS porte_descricao,
            ente_federativo,
            CASE
                WHEN capital_social = 0 THEN 'SEM CAPITAL'
                WHEN capital_social <= 1000 THEN 'ATÉ 1K'
                WHEN capital_social <= 10000 THEN '1K A 10K'
                WHEN capital_social <= 100000 THEN '10K A 100K'
                WHEN capital_social <= 1000000 THEN '100K A 1M'
                ELSE 'ACIMA DE 1M'
            END AS capital_social_faixa,
            (length(razao_social) >= 11 AND regexp_matches(right(razao_social, 11), '^[0-9]{{11}}$')) AS is_mei,
            CASE substr(natureza_juridica, 1, 1)
                WHEN '1' THEN 'ADMINISTRAÇÃO PÚBLICA'
                WHEN '2' THEN 'ENTIDADES EMPRESARIAIS'
                WHEN '3' THEN 'ENTIDADES SEM FINS LUCRATIVOS'
                WHEN '4' THEN 'PESSOAS FÍSICAS'
                WHEN '5' THEN 'ORGANIZAÇÕES INTERNACIONAIS'
                ELSE 'OUTROS'
            END AS natureza_juridica_grupo,
            (ente_federativo IS NOT NULL) AS ente_federativo_presente,
            TIMESTAMP '{now}' AS data_processamento
        FROM parsed
    ) TO '{out_path.as_posix()}' (FORMAT CSV, DELIMITER E'\\t', HEADER false, QUOTE '"', ESCAPE '"', NULLSTR '\\N')
    """
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads={DUCKDB_THREADS}")
    con.execute("SET preserve_insertion_order=false")
    con.execute(sql)
    con.close()


def load_into_postgres(conn, table_bare, csv_path):
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8", newline="") as f:
        cols = ", ".join(COPY_COLUMNS)
        cur.copy_expert(
            f"COPY {table_bare} ({cols}) FROM STDIN WITH "
            f"(FORMAT csv, DELIMITER E'\\t', NULL '\\N', QUOTE '\"', ESCAPE '\"')",
            f,
        )
    conn.commit()


def dedup_cnpj_basico(conn, table_name):
    """cnpj_basico deve ser único (DQ-09). Duplicatas entre arquivos são raras (dado real:
    1 em 68,6M) — em vez de gerar/manter índice (custa storage), faz uma única passada de
    window function no Postgres (sort + partition, sem self-join) pra achar e remover as
    repetidas. Roda no servidor Postgres, não conta na RAM do container do participante."""
    with conn.cursor() as cur:
        cur.execute(f"""
            DELETE FROM {table_name} t USING (
                SELECT ctid, ROW_NUMBER() OVER (PARTITION BY cnpj_basico ORDER BY ctid) AS rn
                FROM {table_name}
            ) d
            WHERE t.ctid = d.ctid AND d.rn > 1
        """)
        removed = cur.rowcount
    conn.commit()
    return removed


def main():
    t0 = time.time()
    env = get_env()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    pg_table = env["PG_TABLE"].replace("-", "_")
    table_name = f"public.{pg_table}"
    table_bare = pg_table

    print("=== Ingestão no Limite (DuckDB, por arquivo) ===")
    print(f"Tabela       : {table_name}")
    print()

    zip_files = sorted(DATA_DIR.glob(ZIP_PATTERN))
    if not zip_files:
        print(f"[ERRO] Nenhum arquivo {ZIP_PATTERN} em {DATA_DIR}")
        sys.exit(1)

    print(f"Arquivos: {len(zip_files)}")
    for zf in zip_files:
        print(f"  - {zf.name}")
    print()

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(
        host=env["PG_HOST"], port=env["PG_PORT"],
        user=env["PG_USER"], password=env["PG_PASSWORD"],
        dbname=env["PG_DB"],
    )
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit TO off")
        conn.commit()

    create_table(conn, table_name)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {table_name}")
        cur.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = false)")
        conn.commit()

    for zip_path in zip_files:
        t_file = time.time()
        print(f"[{zip_path.name}]")

        extracted = extract_csv(zip_path)
        if extracted is None:
            print("      [aviso] .EMPRECSV não encontrado, pulando")
            continue

        transformed_path = TMP_DIR / f"{zip_path.stem}_out.csv"
        transform_with_duckdb(extracted, now, transformed_path)
        extracted.unlink(missing_ok=True)

        load_into_postgres(conn, table_bare, transformed_path)
        transformed_path.unlink(missing_ok=True)

        print(f"      -> concluído em {time.time() - t_file:.1f}s")

    print()
    print("Removendo duplicatas de cnpj_basico entre arquivos (DQ-09)...")
    t_dedup = time.time()
    removed = dedup_cnpj_basico(conn, table_name)
    print(f"      -> {removed} duplicata(s) removida(s) em {time.time() - t_dedup:.1f}s")

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_linhas = cur.fetchone()[0]
    conn.commit()

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = true)")
        cur.execute(f"VACUUM (ANALYZE) {table_name}")
    conn.autocommit = False
    conn.close()

    elapsed = time.time() - t0
    print()
    print("=== Finalizado ===")
    print(f"Total de linhas : {total_linhas:,}")
    print(f"Tempo total     : {elapsed:.1f}s")
    print(f"Taxa média      : {total_linhas / elapsed:>8,.0f} linhas/s")


if __name__ == "__main__":
    main()
