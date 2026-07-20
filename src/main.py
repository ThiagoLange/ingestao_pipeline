import csv
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

DATA_DIR = Path("/data")
BATCH_SIZE = 100_000
ZIP_PATTERN = "Empresas*.zip"

_PORTE = {"00":"NÃO INFORMADO","01":"MICRO EMPRESA","03":"EMPRESA DE PEQUENO PORTE","05":"DEMAIS"}
_NJ = {"1":"ADMINISTRAÇÃO PÚBLICA","2":"ENTIDADES EMPRESARIAIS","3":"ENTIDADES SEM FINS LUCRATIVOS","4":"PESSOAS FÍSICAS","5":"ORGANIZAÇÕES INTERNACIONAIS"}


def get_env():
    required = ["PARTICIPANTE", "PG_USER", "PG_PASSWORD"]
    env = {}
    for key in required:
        env[key] = os.environ[key]
    env["PG_TABLE"] = os.environ.get("PG_TABLE", f"{env['PARTICIPANTE']}_empresas")
    env["PG_HOST"] = os.environ.get("PG_HOST", "localhost")
    env["PG_PORT"] = os.environ.get("PG_PORT", "5432")
    env["PG_DB"] = os.environ.get("PG_DB", "db_empresas")
    return env


def esc(v):
    if v is None:
        return "\\N"
    s = str(v)
    s = s.replace("\\", "\\\\")
    s = s.replace("\t", " ")
    s = s.replace("\n", " ")
    s = s.replace("\r", " ")
    return s


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


def process_zip(zip_path, conn, table_bare, now, t_start):
    total = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".EMPRECSV")), None)
        if not csv_name:
            return 0
        with zf.open(csv_name, "r") as raw:
            text = io.TextIOWrapper(raw, encoding="iso-8859-1")
            reader = csv.reader(text, delimiter=";", quotechar='"')
            buf = io.StringIO()
            for row in reader:
                if not row or len(row) < 7:
                    continue
                try:
                    cnpj = row[0]
                    rs = row[1].strip().upper()
                    nj = row[2]
                    qr = row[3]
                    cs_raw = row[4]
                    pc = row[5]
                    ef_raw = row[6]

                    cs = 0.0
                    if cs_raw:
                        cs = float(cs_raw.strip().replace(".", "").replace(",", "."))
                    pc = pc.strip() if pc else "00"
                    ef = ef_raw.strip() if ef_raw else ""

                    imi = len(rs) >= 11 and rs[-11:].isdigit()
                    njg = _NJ.get(nj[:1] if nj else "", "OUTROS")

                    if cs == 0:
                        csf = "SEM CAPITAL"
                    elif cs <= 1000:
                        csf = "ATÉ 1K"
                    elif cs <= 10000:
                        csf = "1K A 10K"
                    elif cs <= 100000:
                        csf = "10K A 100K"
                    elif cs <= 1000000:
                        csf = "100K A 1M"
                    else:
                        csf = "ACIMA DE 1M"

                    pd = _PORTE.get(pc, "NÃO INFORMADO")
                    cs_s = f"{cs:.2f}"
                    ef_s = esc(ef) if ef else "\\N"
                    efp = "t" if ef else "f"

                    buf.write(
                        f"{esc(cnpj)}\t{esc(rs)}\t{esc(nj)}\t{esc(qr)}\t"
                        f"{cs_s}\t{esc(pc)}\t{esc(pd)}\t{ef_s}\t{esc(csf)}\t"
                        f"{imi}\t{esc(njg)}\t{efp}\t{now}\n"
                    )
                    total += 1
                    if total % BATCH_SIZE == 0:
                        buf.seek(0)
                        with conn.cursor() as cur:
                            cur.copy_from(buf, table_bare, sep="\t", null="\\N", columns=(
                                "cnpj_basico", "razao_social", "natureza_juridica",
                                "qualificacao_responsavel", "capital_social", "porte_codigo",
                                "porte_descricao", "ente_federativo", "capital_social_faixa",
                                "is_mei", "natureza_juridica_grupo", "ente_federativo_presente",
                                "data_processamento",
                            ))
                        conn.commit()
                        buf.close()
                        buf = io.StringIO()
                        elapsed = time.time() - t_start
                        rate = total / elapsed if elapsed > 0 else 0
                        print(f"    {total:>10,} linhas | taxa: {rate:>8,.0f} linhas/s", end="\r")
                        sys.stdout.flush()
                except Exception:
                    continue
            if buf.tell() > 0:
                buf.seek(0)
                with conn.cursor() as cur:
                    cur.copy_from(buf, table_bare, sep="\t", null="\\N", columns=(
                        "cnpj_basico", "razao_social", "natureza_juridica",
                        "qualificacao_responsavel", "capital_social", "porte_codigo",
                        "porte_descricao", "ente_federativo", "capital_social_faixa",
                        "is_mei", "natureza_juridica_grupo", "ente_federativo_presente",
                        "data_processamento",
                    ))
                conn.commit()
            buf.close()
    return total


def main():
    t0 = time.time()
    env = get_env()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    pg_table = env["PG_TABLE"].replace("-", "_")
    table_name = f"public.{pg_table}"
    table_bare = pg_table

    print(f"=== Ingestão no Limite ===")
    print(f"Tabela       : {table_name}")
    print(f"Batch size   : {BATCH_SIZE:,} linhas")
    print()

    zip_files = sorted(DATA_DIR.glob(ZIP_PATTERN))
    if not zip_files:
        print(f"[ERRO] Nenhum arquivo {ZIP_PATTERN} em {DATA_DIR}")
        sys.exit(1)

    print(f"Arquivos: {len(zip_files)}")
    for zf in zip_files:
        print(f"  - {zf.name}")
    print()

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

    total_linhas = 0
    for zip_path in zip_files:
        size_mb = zip_path.stat().st_size / (1024 * 1024)
        print(f"[{zip_path.name}] ({size_mb:.1f} MB)")
        t_zip = time.time()
        n = process_zip(zip_path, conn, table_bare, now, t0)
        elapsed_zip = time.time() - t_zip
        total_linhas += n
        print(f"    -> {n:>10,} linhas em {elapsed_zip:.1f}s")

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table_name} t1 USING {table_name} t2 WHERE t1.cnpj_basico = t2.cnpj_basico AND t1.ctid > t2.ctid")
        dup = cur.rowcount
        conn.commit()

    conn.close()

    elapsed = time.time() - t0
    print()
    if dup:
        total_linhas -= dup
        print(f"Duplicatas removidas: {dup}")
    print(f"=== Finalizado ===")
    print(f"Total de linhas : {total_linhas:,}")
    print(f"Tempo total     : {elapsed:.1f}s")
    print(f"Taxa média      : {total_linhas / elapsed:>8,.0f} linhas/s")


if __name__ == "__main__":
    main()
