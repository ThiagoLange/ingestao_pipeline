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

_PORTE = {"00": "NÃO INFORMADO", "01": "MICRO EMPRESA", "03": "EMPRESA DE PEQUENO PORTE", "05": "DEMAIS"}
_PORTE_VALID = ("00", "01", "03", "05")
_NJ = {"1": "ADMINISTRAÇÃO PÚBLICA", "2": "ENTIDADES EMPRESARIAIS", "3": "ENTIDADES SEM FINS LUCRATIVOS", "4": "PESSOAS FÍSICAS", "5": "ORGANIZAÇÕES INTERNACIONAIS"}

# cnpj_basico é numérico de 8 dígitos (00000000-99999999) -> bitset cabe em ~12,5 MB
CNPJ_SPACE = 100_000_000

# remove bytes de controle residuais (fora \t\n\r, já tratados por esc()) sem mexer em acentuação
_CONTROL_TABLE = str.maketrans("", "", "".join(chr(c) for c in list(range(0, 9)) + list(range(11, 13)) + list(range(14, 32)) + [127]))

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


def _flush(conn, table_bare, buf):
    if buf.tell() == 0:
        return
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(buf, table_bare, sep="\t", null="\\N", columns=COPY_COLUMNS)
    conn.commit()
    buf.close()


def process_zip(zip_path, conn, table_bare, now, t_start, seen):
    total = 0
    dup = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".EMPRECSV")), None)
        if not csv_name:
            return 0, 0
        with zf.open(csv_name, "r") as raw:
            text = io.TextIOWrapper(raw, encoding="iso-8859-1", newline="")
            reader = csv.reader(text, delimiter=";", quotechar='"')
            buf = io.StringIO()
            for row in reader:
                if not row:
                    continue
                if len(row) < 7:
                    row = row + [""] * (7 - len(row))

                cnpj = (row[0] or "").strip()
                if cnpj.isdigit():
                    cnpj = cnpj.zfill(8)
                rs = (row[1] or "").strip().upper().translate(_CONTROL_TABLE).strip()
                nj = (row[2] or "").strip()
                if nj.isdigit():
                    nj = nj.zfill(4)
                qr = (row[3] or "").strip()
                cs_raw = (row[4] or "").strip()
                pc = (row[5] or "").strip()
                ef_raw = row[6] if len(row) > 6 else ""

                try:
                    cs = float(cs_raw.replace(".", "").replace(",", ".")) if cs_raw else 0.0
                except ValueError:
                    cs = 0.0

                if pc not in _PORTE_VALID:
                    pc = "00"
                ef = (ef_raw or "").strip()

                is_dup = False
                if len(cnpj) == 8 and cnpj.isdigit():
                    idx = int(cnpj)
                    byte_i, bit = idx >> 3, 1 << (idx & 7)
                    if seen[byte_i] & bit:
                        is_dup = True
                    else:
                        seen[byte_i] |= bit
                if is_dup:
                    dup += 1
                    continue

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

                pd = _PORTE[pc]
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
                    _flush(conn, table_bare, buf)
                    buf = io.StringIO()
                    elapsed = time.time() - t_start
                    rate = total / elapsed if elapsed > 0 else 0
                    print(f"    {total:>10,} linhas | taxa: {rate:>8,.0f} linhas/s", end="\r")
                    sys.stdout.flush()
            _flush(conn, table_bare, buf)
    return total, dup


def main():
    t0 = time.time()
    env = get_env()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    pg_table = env["PG_TABLE"].replace("-", "_")
    table_name = f"public.{pg_table}"
    table_bare = pg_table

    print("=== Ingestão no Limite ===")
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

    # cnpj_basico deve ser único por design da origem, mas dados reais da Receita já
    # mostraram duplicidade pontual entre arquivos. Em vez de um DELETE por auto-join
    # sobre 68M linhas sem índice (arriscava estourar o timeout de 60 min), o dedup
    # acontece em O(1)/linha via bitset em memória (~12,5 MB) durante a carga.
    seen = bytearray(CNPJ_SPACE // 8 + 1)

    total_linhas = 0
    total_dup = 0
    for zip_path in zip_files:
        size_mb = zip_path.stat().st_size / (1024 * 1024)
        print(f"[{zip_path.name}] ({size_mb:.1f} MB)")
        t_zip = time.time()
        n, dup = process_zip(zip_path, conn, table_bare, now, t0, seen)
        elapsed_zip = time.time() - t_zip
        total_linhas += n
        total_dup += dup
        print(f"    -> {n:>10,} linhas em {elapsed_zip:.1f}s")

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = true)")
        cur.execute(f"VACUUM (ANALYZE) {table_name}")
    conn.autocommit = False
    conn.close()

    elapsed = time.time() - t0
    print()
    if total_dup:
        print(f"Duplicatas de cnpj_basico descartadas na carga: {total_dup}")
    print("=== Finalizado ===")
    print(f"Total de linhas : {total_linhas:,}")
    print(f"Tempo total     : {elapsed:.1f}s")
    print(f"Taxa média      : {total_linhas / elapsed:>8,.0f} linhas/s")


if __name__ == "__main__":
    main()
