use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::Utc;
use postgres::{Client, NoTls};

// cnpj_basico e' numerico de 8 digitos (00000000-99999999) -> bitset cabe em ~12,5 MB.
const CNPJ_SPACE: usize = 100_000_000;

struct EnvConfig {
    participante: String,
    pg_table: String,
    pg_host: String,
    pg_port: String,
    pg_user: String,
    pg_password: String,
    pg_db: String,
}

fn get_env() -> EnvConfig {
    let participante = env::var("PARTICIPANTE").expect("PARTICIPANTE nao definido");
    let pg_table = env::var("PG_TABLE").unwrap_or_else(|_| format!("{}_empresas", participante));
    EnvConfig {
        participante,
        pg_table,
        pg_host: env::var("PG_HOST").unwrap_or_else(|_| "postgres_db".to_string()),
        pg_port: env::var("PG_PORT").unwrap_or_else(|_| "5432".to_string()),
        pg_user: env::var("PG_USER").expect("PG_USER nao definido"),
        pg_password: env::var("PG_PASSWORD").expect("PG_PASSWORD nao definido"),
        pg_db: env::var("PG_DB").unwrap_or_else(|_| "db_empresas".to_string()),
    }
}

/// ISO-8859-1 -> Unicode e' mapeamento direto byte-a-codepoint (nao precisa de crate de encoding).
fn latin1_to_string(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect()
}

/// Remove bytes de controle residuais (fora \t\n\r, tratados por esc()) sem mexer em acentuacao.
fn strip_control(s: &str) -> String {
    s.chars()
        .filter(|&c| {
            let u = c as u32;
            !((u < 9) || (u == 11) || (u == 12) || (u >= 14 && u < 32) || (u == 127))
        })
        .collect()
}

/// Escapa campo pro formato COPY TEXT do Postgres (tab-separated, \N como NULL).
fn esc(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    s.replace('\\', "\\\\")
        .replace('\t', " ")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn parse_capital_social(raw: &str) -> f64 {
    let t = raw.trim();
    if t.is_empty() {
        return 0.0;
    }
    let no_dots: String = t.chars().filter(|&c| c != '.').collect();
    let normalized = no_dots.replace(',', ".");
    normalized.parse::<f64>().unwrap_or(0.0)
}

fn capital_social_faixa(cs: f64) -> &'static str {
    if cs == 0.0 {
        "SEM CAPITAL"
    } else if cs <= 1000.0 {
        "ATÉ 1K"
    } else if cs <= 10000.0 {
        "1K A 10K"
    } else if cs <= 100000.0 {
        "10K A 100K"
    } else if cs <= 1_000_000.0 {
        "100K A 1M"
    } else {
        "ACIMA DE 1M"
    }
}

fn porte_descricao(codigo: &str) -> &'static str {
    match codigo {
        "00" => "NÃO INFORMADO",
        "01" => "MICRO EMPRESA",
        "03" => "EMPRESA DE PEQUENO PORTE",
        "05" => "DEMAIS",
        _ => unreachable!("porte_codigo ja normalizado"),
    }
}

fn natureza_juridica_grupo(nj: &str) -> &'static str {
    match nj.chars().next() {
        Some('1') => "ADMINISTRAÇÃO PÚBLICA",
        Some('2') => "ENTIDADES EMPRESARIAIS",
        Some('3') => "ENTIDADES SEM FINS LUCRATIVOS",
        Some('4') => "PESSOAS FÍSICAS",
        Some('5') => "ORGANIZAÇÕES INTERNACIONAIS",
        _ => "OUTROS",
    }
}

fn is_mei(razao: &str) -> bool {
    let chars: Vec<char> = razao.chars().collect();
    if chars.len() < 11 {
        return false;
    }
    chars[chars.len() - 11..].iter().all(|c| c.is_ascii_digit())
}

struct Seen {
    bits: Vec<u8>,
}

impl Seen {
    fn new() -> Self {
        Seen {
            bits: vec![0u8; CNPJ_SPACE / 8 + 1],
        }
    }

    /// Retorna true se ja tinha visto essa chave (e marca como vista). cnpj deve ter 8 digitos.
    fn check_and_mark(&mut self, cnpj: &str) -> bool {
        if cnpj.len() != 8 || !cnpj.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        let idx: usize = cnpj.parse().unwrap_or(usize::MAX);
        if idx >= CNPJ_SPACE {
            return false;
        }
        let byte_i = idx / 8;
        let bit = 1u8 << (idx % 8);
        let was_set = self.bits[byte_i] & bit != 0;
        self.bits[byte_i] |= bit;
        was_set
    }
}

fn extract_csv(zip_path: &Path, tmp_dir: &Path) -> Option<PathBuf> {
    let file = File::open(zip_path).ok()?;
    let mut archive = zip::ZipArchive::new(file).ok()?;
    let idx = (0..archive.len()).find(|&i| {
        archive
            .by_index(i)
            .map(|f| f.name().ends_with(".EMPRECSV"))
            .unwrap_or(false)
    })?;
    let mut src = archive.by_index(idx).ok()?;
    let stem = zip_path.file_stem().unwrap().to_string_lossy();
    let out_path = tmp_dir.join(format!("{}.csv", stem));
    let mut dst = BufWriter::new(File::create(&out_path).ok()?);
    std::io::copy(&mut src, &mut dst).ok()?;
    dst.flush().ok()?;
    Some(out_path)
}

struct Row {
    cnpj_basico: String,
    razao_social: String,
    natureza_juridica: String,
    qualificacao_responsavel: String,
    capital_social: f64,
    porte_codigo: String,
    ente_federativo: Option<String>,
}

fn parse_record(record: &csv::ByteRecord) -> Option<Row> {
    let get = |i: usize| -> String {
        record
            .get(i)
            .map(latin1_to_string)
            .unwrap_or_default()
    };

    let mut cnpj = get(0).trim().to_string();
    if cnpj.bytes().all(|b| b.is_ascii_digit()) && cnpj.len() < 8 {
        cnpj = format!("{:0>8}", cnpj);
    }

    let mut razao = get(1).trim().to_uppercase();
    razao = strip_control(&razao);
    razao = razao.trim().to_string();

    let mut nj = get(2).trim().to_string();
    if nj.bytes().all(|b| b.is_ascii_digit()) && nj.len() < 4 {
        nj = format!("{:0>4}", nj);
    }

    let qr = get(3).trim().to_string();
    let cs_raw = get(4);
    let capital_social = parse_capital_social(&cs_raw);

    let mut porte = get(5).trim().to_string();
    if !matches!(porte.as_str(), "00" | "01" | "03" | "05") {
        porte = "00".to_string();
    }

    let ente_raw = get(6);
    let ente_trim = ente_raw.trim();
    let ente_federativo = if ente_trim.is_empty() {
        None
    } else {
        Some(ente_trim.to_string())
    };

    Some(Row {
        cnpj_basico: cnpj,
        razao_social: razao,
        natureza_juridica: nj,
        qualificacao_responsavel: qr,
        capital_social,
        porte_codigo: porte,
        ente_federativo,
    })
}

fn write_row(buf: &mut impl Write, row: &Row, now: &str) -> std::io::Result<()> {
    let pd = porte_descricao(&row.porte_codigo);
    let csf = capital_social_faixa(row.capital_social);
    let mei = is_mei(&row.razao_social);
    let njg = natureza_juridica_grupo(&row.natureza_juridica);
    let ente_s = match &row.ente_federativo {
        Some(v) => esc(v),
        None => "\\N".to_string(),
    };
    let efp = if row.ente_federativo.is_some() { "t" } else { "f" };

    writeln!(
        buf,
        "{}\t{}\t{}\t{}\t{:.2}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        esc(&row.cnpj_basico),
        esc(&row.razao_social),
        esc(&row.natureza_juridica),
        esc(&row.qualificacao_responsavel),
        row.capital_social,
        esc(&row.porte_codigo),
        pd,
        ente_s,
        csf,
        mei,
        njg,
        efp,
        now,
    )
}

const COPY_COLUMNS: &str = "cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, \
    capital_social, porte_codigo, porte_descricao, ente_federativo, capital_social_faixa, \
    is_mei, natureza_juridica_grupo, ente_federativo_presente, data_processamento";

fn create_table(client: &mut Client, table_name: &str) -> Result<(), postgres::Error> {
    client.batch_execute(&format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
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
        )"
    ))?;
    Ok(())
}

fn main() {
    let t0 = Instant::now();
    let cfg = get_env();
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let pg_table = cfg.pg_table.replace('-', "_");
    let table_name = format!("public.{}", pg_table);

    println!("=== Ingestão no Limite (Rust) ===");
    println!("Participante : {}", cfg.participante);
    println!("Tabela       : {}", table_name);
    println!();

    let data_dir = Path::new("/data");
    let mut zip_files: Vec<PathBuf> = fs::read_dir(data_dir)
        .expect("nao consegui ler /data")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().map(|e| e == "zip").unwrap_or(false)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("Empresas"))
                    .unwrap_or(false)
        })
        .collect();
    zip_files.sort();

    if zip_files.is_empty() {
        eprintln!("[ERRO] Nenhum arquivo Empresas*.zip em /data");
        std::process::exit(1);
    }

    println!("Arquivos: {}", zip_files.len());
    for z in &zip_files {
        println!("  - {}", z.file_name().unwrap().to_string_lossy());
    }
    println!();

    let tmp_dir = Path::new("/app/tmp_pipeline");
    fs::create_dir_all(tmp_dir).expect("nao consegui criar dir temp");

    let dsn = format!(
        "host={} port={} user={} password={} dbname={}",
        cfg.pg_host, cfg.pg_port, cfg.pg_user, cfg.pg_password, cfg.pg_db
    );
    let mut client = Client::connect(&dsn, NoTls).expect("falha ao conectar no Postgres");

    client
        .batch_execute("SET synchronous_commit TO off")
        .expect("falha ao setar synchronous_commit");

    create_table(&mut client, &table_name).expect("falha ao criar tabela");
    client
        .batch_execute(&format!(
            "TRUNCATE {table_name}; ALTER TABLE {table_name} SET (autovacuum_enabled = false);"
        ))
        .expect("falha ao truncar tabela");

    let mut seen = Seen::new();
    let mut total_linhas: u64 = 0;
    let mut total_dup: u64 = 0;

    {
        let copy_sql = format!("COPY {pg_table} ({COPY_COLUMNS}) FROM STDIN");
        let mut writer = client
            .copy_in(copy_sql.as_str())
            .expect("falha ao iniciar COPY");
        let mut buf = BufWriter::with_capacity(1024 * 1024, &mut writer);

        for zip_path in &zip_files {
            let t_file = Instant::now();
            print!("[{}] ", zip_path.file_name().unwrap().to_string_lossy());

            let extracted = match extract_csv(zip_path, tmp_dir) {
                Some(p) => p,
                None => {
                    println!("[aviso] .EMPRECSV nao encontrado, pulando");
                    continue;
                }
            };

            let file = File::open(&extracted).expect("falha ao abrir csv extraido");
            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b';')
                .quote(b'"')
                .has_headers(false)
                .flexible(true)
                .from_reader(file);

            let mut file_rows: u64 = 0;
            let mut file_dup: u64 = 0;
            let mut rec = csv::ByteRecord::new();
            loop {
                match reader.read_byte_record(&mut rec) {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(_) => continue,
                }
                if rec.is_empty() {
                    continue;
                }
                let row = match parse_record(&rec) {
                    Some(r) => r,
                    None => continue,
                };
                if seen.check_and_mark(&row.cnpj_basico) {
                    file_dup += 1;
                    continue;
                }
                write_row(&mut buf, &row, &now).expect("falha ao escrever linha no COPY");
                file_rows += 1;
            }

            fs::remove_file(&extracted).ok();
            total_linhas += file_rows;
            total_dup += file_dup;
            println!(
                "-> {} linhas em {:.1}s",
                file_rows,
                t_file.elapsed().as_secs_f64()
            );
        }

        buf.flush().expect("falha ao dar flush no buffer de COPY");
        drop(buf);
        writer.finish().expect("falha ao finalizar COPY");
    }

    let elapsed = t0.elapsed().as_secs_f64();
    println!();
    if total_dup > 0 {
        println!("Duplicatas de cnpj_basico descartadas na carga: {}", total_dup);
    }
    println!("=== Finalizado ===");
    println!("Total de linhas : {}", total_linhas);
    println!("Tempo total     : {:.1}s", elapsed);
    println!(
        "Taxa média      : {:.0} linhas/s",
        total_linhas as f64 / elapsed
    );
}
