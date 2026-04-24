"""
Coleta CSV do CamCarcacas (Carel Boss) para hoje e ontem e **acumula**
os registros em um único banco de dados (data/historico.csv).

Modelo: banco único acumulativo.
- Cada execução baixa ontem + hoje e mescla em `data/historico.csv`
- Deduplicação por timestamp (mantém o valor mais recente)
- Linhas sem nenhum dado util sao descartadas; linhas com sensores validos e
  estados digitais "---" sao mantidas
- Pastas legadas "Ciclo N" podem ser migradas via `migrate_legacy_folders()`
"""

import time
import datetime
import re
import os
import shutil
import tempfile
from pathlib import Path

# Selenium/requests são importados apenas nas funções de coleta, para permitir
# que a migração de pastas legadas funcione sem eles instalados.

# ─── Carregamento de variáveis de ambiente (.env) ────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    # python-dotenv não instalado: usa só variáveis de ambiente do sistema.
    pass


def _env(name: str, default: str | None = None, required: bool = False) -> str:
    """Lê configuração do ambiente, com fallback para st.secrets no Streamlit Cloud."""
    value = os.getenv(name, default)
    if value in (None, ""):
        try:
            import streamlit as st
            value = st.secrets.get(name, default)
        except Exception:
            value = default
    if required and (value is None or value == ""):
        raise RuntimeError(
            f"Variável de ambiente '{name}' não definida. "
            f"Configure no .env local ou nos Secrets do Streamlit Cloud."
        )
    return value or ""


# ─── Configurações (lidas do .env) ───────────────────────────────────────────
CAREL_HOST     = _env("CAREL_HOST")
CAREL_PORT     = _env("CAREL_PORT", "8080")
USERNAME       = _env("CAREL_USERNAME", required=False)  # validado no create_session()
PASSWORD       = _env("CAREL_PASSWORD", required=False)
REPORT_ID      = _env("CAREL_REPORT_ID", "158")
FREQUENCY      = _env("CAREL_FREQUENCY", "3")

BASE_URL   = f"http://{CAREL_HOST}:{CAREL_PORT}/boss"
DATA_DIR = Path(__file__).parent / "data"
MASTER_CSV = DATA_DIR / "historico.csv"
LEGACY_DIR = DATA_DIR / "_legado_pastas_ciclos"
SELENIUM_PROFILE_DIR = DATA_DIR / "_selenium_profiles"

DATA_DIR.mkdir(parents=True, exist_ok=True)
SELENIUM_PROFILE_DIR.mkdir(parents=True, exist_ok=True)


def _check_credentials():
    """Valida que credenciais foram carregadas. Chamado antes de operações de coleta."""
    if not CAREL_HOST or not USERNAME or not PASSWORD:
        raise RuntimeError(
            "Configuração do Carel Boss ausente. "
            "Defina CAREL_HOST, CAREL_USERNAME e CAREL_PASSWORD no .env local "
            "ou nos Secrets do Streamlit Cloud."
        )


def _attempt_login(opts_factory) -> "object":
    """Uma tentativa de login. Retorna o driver autenticado ou levanta exceção."""
    from selenium import webdriver
    driver = webdriver.Chrome(options=opts_factory())
    try:
        driver.get(BASE_URL + "/")
        time.sleep(2)

        driver.execute_script("""
            document.getElementById('txtUser').value = arguments[0];
            document.getElementById('txtPassword').value = arguments[1];
            document.getElementById('loginfrm').submit();
        """, USERNAME, PASSWORD)
        time.sleep(5)

        if "Login.js" in driver.page_source:
            raise RuntimeError("Login rejeitado pelo servidor (página de login persistiu).")

        return driver
    except Exception:
        try:
            driver.quit()
        except Exception:
            pass
        raise


def create_session(max_attempts: int = 3, backoff_seconds: float = 3.0):
    """
    Faz login via Selenium com retry e backoff exponencial.

    Args:
        max_attempts:    quantas tentativas antes de falhar definitivamente.
        backoff_seconds: tempo base entre tentativas (cresce exponencialmente).

    Levanta RuntimeError se todas as tentativas falharem.
    """
    _check_credentials()

    from selenium.webdriver.chrome.options import Options

    def _opts_factory():
        opts = Options()
        profile_dir = Path(tempfile.mkdtemp(prefix="chrome_", dir=SELENIUM_PROFILE_DIR))
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument(f"--user-data-dir={profile_dir}")
        opts.add_argument(f"--disk-cache-dir={profile_dir / 'cache'}")
        chrome_bin = os.getenv("CHROME_BIN")
        if not chrome_bin and Path("/usr/bin/chromium").exists():
            chrome_bin = "/usr/bin/chromium"
        if chrome_bin:
            opts.binary_location = chrome_bin
        return opts

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[Login] Tentativa {attempt}/{max_attempts}...")
            driver = _attempt_login(_opts_factory)
            print(f"[OK] Login realizado em {CAREL_HOST}:{CAREL_PORT}.")
            return driver
        except Exception as e:
            last_error = e
            print(f"[Login] Falhou: {type(e).__name__}: {e}")
            if attempt < max_attempts:
                wait = backoff_seconds * (2 ** (attempt - 1))
                print(f"[Login] Aguardando {wait:.1f}s antes de nova tentativa...")
                time.sleep(wait)

    raise RuntimeError(
        f"Login falhou após {max_attempts} tentativas. "
        f"Último erro: {type(last_error).__name__}: {last_error}"
    )


def _generate_report_once(driver, date_str: str) -> str:
    """Uma tentativa de geração do relatório. Retorna o caminho ou levanta exceção."""
    driver.switch_to.default_content()
    driver.execute_script("""
        canChangeLink('nop&folder=report&bo=BReport&type=menu&desc=report',
            document.getElementById('dvm7'));
    """)
    time.sleep(4)

    driver.switch_to.frame("body")
    driver.switch_to.frame("bodytab")
    time.sleep(3)

    driver.execute_script(f"selectedLineReport('{REPORT_ID}');")
    time.sleep(1)

    driver.execute_script(f"""
        if (document.getElementById('r_path')) {{
            document.getElementById('r_path').value = '';
        }}
        document.getElementById('date_from').value = '{date_str}';
        document.getElementById('date_to').value   = '{date_str}';
        document.getElementById('command').value   = 'printReport';
        document.getElementById('idReportSelect').value = '{REPORT_ID}';
        document.getElementById('frequency').value = '{FREQUENCY}';
        document.getElementById('frm_report').submit();
    """)

    print(f"  Gerando relatório de {date_str}...")
    for _ in range(30):
        time.sleep(2)
        driver.switch_to.default_content()
        driver.switch_to.frame("body")
        driver.switch_to.frame("bodytab")
        r_path = driver.execute_script(
            "return document.getElementById('r_path').value;"
        )
        if r_path:
            print(f"  [OK] Arquivo gerado: {r_path}")
            return r_path

    raise TimeoutError(f"Timeout ao gerar relatório de {date_str}.")


def generate_report(driver, date_str: str, max_attempts: int = 2) -> str:
    """
    Gera o relatório para a data (DD/MM/YYYY) e retorna o caminho do arquivo
    no servidor. Tenta novamente em caso de falha transitória.
    """
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return _generate_report_once(driver, date_str)
        except Exception as e:
            last_error = e
            print(f"  [FALHA tentativa {attempt}/{max_attempts}] {type(e).__name__}: {e}")
            if attempt < max_attempts:
                time.sleep(3)
    raise RuntimeError(
        f"Falha ao gerar relatório de {date_str} após {max_attempts} tentativas: "
        f"{type(last_error).__name__}: {last_error}"
    )


def download_csv(driver, server_path: str) -> str:
    """Baixa o CSV usando a sessão do Selenium e retorna o conteúdo."""
    import requests
    cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    for k, v in cookies.items():
        s.cookies.set(k, v)

    r = s.get(
        f"{BASE_URL}/servlet/document",
        params={"path": server_path},
        timeout=30,
    )
    r.raise_for_status()
    return r.text


def detect_cycles(data_lines: list[str]) -> list[dict]:
    """
    Detecta ciclos de Carregamento (OFF→ON→OFF).
    Retorna lista de dicts com início e fim de cada ciclo.
    """
    cycles = []
    current_start = None

    for line in data_lines:
        parts = line.split(";")
        if len(parts) < 2:
            continue
        ts   = parts[0].strip()
        carg = parts[1].strip() if len(parts) > 1 else ""

        if carg == "ON" and current_start is None:
            current_start = ts
        elif carg in ("OFF", "---") and current_start is not None:
            cycles.append({"inicio": current_start, "fim": ts})
            current_start = None

    # Ciclo ainda aberto no final do período
    if current_start is not None:
        cycles.append({"inicio": current_start, "fim": data_lines[-1].split(";")[0]})

    return cycles


# ═════════════════════════════════════════════════════════════════════════════
#  BANCO ÚNICO ACUMULATIVO (data/historico.csv)
# ═════════════════════════════════════════════════════════════════════════════

# Cabeçalho fixo do master CSV (linhas 1-5 são ignoradas pelo parser;
# linha 6 contém os nomes das colunas). Mantido compatível com
# `load_consolidated_csv()` em gerar_relatorio.py.
_COL_CATEGORIES = (
    '" ";'
    + ";".join([
        "CPCO 51 - Câmara de carcaças - Controle de temperatura 1"
    ] * 8)
    + ";"
)
_COL_NAMES = (
    '" ";Carregamento;Resfriamento;Saida Y1 - Ventiladores EC;'
    "Temp entrada glicol;Temp ref;Temp retorno ar;Temperatuda espeto;"
    "Umidade relativa da camara;"
)


def _extract_data_rows(csv_text: str) -> list[str]:
    """
    Extrai somente as linhas de dados válidas (pulando 6 linhas de cabeçalho).
    Descarta apenas linhas sem nenhum sensor útil.
    """
    lines = csv_text.replace("\r", "").split("\n")
    rows = []
    for line in lines[6:]:
        if not line.strip() or ";" not in line:
            continue
        parts = line.split(";")
        if len(parts) < 2:
            continue
        useful_values = [
            value.strip().strip('"')
            for value in parts[1:]
            if value.strip().strip('"') not in {"", "---"}
        ]
        if not useful_values:
            continue
        rows.append(line.rstrip("\r"))
    return rows


def _extract_csv_bounds(csv_text: str) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """
    Retorna o primeiro e o último timestamp válidos presentes no CSV baixado.
    """
    rows = _extract_data_rows(csv_text)
    timestamps: list[datetime.datetime] = []
    for row in rows:
        ts = _row_timestamp_key(row)
        if not ts:
            continue
        try:
            timestamps.append(datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            continue

    if not timestamps:
        return None, None

    timestamps.sort()
    return timestamps[0], timestamps[-1]


def _validate_daily_csv(csv_text: str, requested_date: datetime.date) -> None:
    """
    Valida se o CSV baixado parece coerente para a data solicitada.

    Para dias já encerrados, rejeita arquivos que terminem cedo demais, pois isso
    costuma indicar que o supervisório devolveu um relatório antigo/em cache.
    """
    first_ts, last_ts = _extract_csv_bounds(csv_text)
    if first_ts is None or last_ts is None:
        raise ValueError("CSV baixado sem timestamps válidos.")

    if first_ts.date() != requested_date or last_ts.date() != requested_date:
        raise ValueError(
            f"CSV de {requested_date:%d/%m/%Y} com intervalo inesperado: "
            f"{first_ts:%d/%m/%Y %H:%M} -> {last_ts:%d/%m/%Y %H:%M}."
        )

    today = datetime.date.today()
    if requested_date < today:
        expected_last = datetime.datetime.combine(requested_date, datetime.time(23, 50))
        if last_ts < expected_last:
            raise ValueError(
                f"CSV de {requested_date:%d/%m/%Y} parece incompleto: último registro em "
                f"{last_ts:%H:%M}. Como esse dia já terminou, o esperado seria um arquivo "
                "próximo de 23:59. Isso normalmente indica que o supervisório devolveu um "
                "relatório antigo ou em cache."
            )


def _build_master_header(min_ts: str, max_ts: str) -> list[str]:
    """Gera o cabeçalho de 6 linhas do master CSV."""
    return [
        "CamCarcacas;",
        f"{min_ts};{max_ts};",
        "12144 Frigorífico Thoms;",
        "usuario;",
        _COL_CATEGORIES,
        _COL_NAMES,
    ]


def _read_master_rows() -> list[str]:
    """Lê as linhas de dados existentes no master CSV (se houver)."""
    if not MASTER_CSV.exists():
        return []
    with open(MASTER_CSV, "r", encoding="utf-8-sig") as f:
        content = f.read()
    lines = content.replace("\r", "").split("\n")
    return [
        l for l in lines[6:]
        if l.strip() and ";" in l
    ]


def _row_timestamp(row: str) -> str:
    """Extrai o timestamp (primeira coluna) de uma linha de dados."""
    return row.split(";", 1)[0].strip()


def _canonical_timestamp(value: str) -> str:
    """Normaliza timestamps BR/ISO para chave estavel de minuto."""
    raw = value.strip().strip('"')
    formats = (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    )
    for fmt in formats:
        try:
            parsed = datetime.datetime.strptime(raw, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return raw


def _row_timestamp_key(row: str) -> str:
    """Chave canonica para deduplicar linhas que representam o mesmo minuto."""
    return _canonical_timestamp(_row_timestamp(row))


def _canonicalize_row_timestamp(row: str) -> str:
    """Reescreve a primeira coluna para o formato canonico do master CSV."""
    if ";" not in row:
        return row
    timestamp, rest = row.split(";", 1)
    return f"{_canonical_timestamp(timestamp)};{rest}"


def append_to_master(csv_text: str) -> tuple[int, int, int]:
    """
    Mescla `csv_text` no banco único `data/historico.csv`.

    Retorna (linhas_novas_adicionadas, linhas_atualizadas, total_no_master).
    """
    new_rows = _extract_data_rows(csv_text)
    existing_rows = _read_master_rows()

    # Mapa timestamp -> linha. Novos registros sobrescrevem existentes.
    row_by_ts: dict[str, str] = {}
    for r in existing_rows:
        ts = _row_timestamp_key(r)
        if ts:
            row_by_ts[ts] = _canonicalize_row_timestamp(r)

    added = 0
    updated = 0
    for r in new_rows:
        ts = _row_timestamp_key(r)
        if not ts:
            continue
        canonical_row = _canonicalize_row_timestamp(r)
        if ts in row_by_ts:
            if row_by_ts[ts] != canonical_row:
                updated += 1
            row_by_ts[ts] = canonical_row
        else:
            row_by_ts[ts] = canonical_row
            added += 1

    # Ordena por timestamp canonico (YYYY-MM-DD HH:MM:SS ordena lexicamente)
    sorted_rows = sorted(row_by_ts.values(), key=_row_timestamp_key)

    if sorted_rows:
        min_ts = _row_timestamp(sorted_rows[0])[:16]
        max_ts = _row_timestamp(sorted_rows[-1])[:16]
    else:
        min_ts = max_ts = ""

    header = _build_master_header(min_ts, max_ts)
    output = "\n".join(header + sorted_rows) + "\n"

    MASTER_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(MASTER_CSV, "w", encoding="utf-8-sig", newline="") as f:
        f.write(output)

    return added, updated, len(sorted_rows)


# ═════════════════════════════════════════════════════════════════════════════
#  MIGRAÇÃO DE PASTAS LEGADAS (Ciclo N - final DD-MM-YY/)
# ═════════════════════════════════════════════════════════════════════════════

def migrate_legacy_folders() -> dict:
    """
    Mescla todas as pastas `Ciclo N - final DD-MM-YY/` em `historico.csv`
    e move as pastas para `data/_legado_pastas_ciclos/` (preservadas, não deletadas).

    Retorna dict com estatísticas.
    """
    pattern = re.compile(r"^Ciclo\s+\d+", re.IGNORECASE)
    legacy_folders = sorted([
        d for d in DATA_DIR.iterdir()
        if d.is_dir() and pattern.match(d.name)
    ])

    stats = {
        "pastas_processadas": 0,
        "csvs_mesclados": 0,
        "linhas_adicionadas": 0,
        "linhas_atualizadas": 0,
        "total_no_master": 0,
        "erros": [],
    }

    if not legacy_folders:
        print("[INFO] Nenhuma pasta legada 'Ciclo N' encontrada.")
        stats["total_no_master"] = len(_read_master_rows())
        return stats

    LEGACY_DIR.mkdir(parents=True, exist_ok=True)

    for folder in legacy_folders:
        csv_files = sorted(folder.glob("*.csv"))
        for csv_file in csv_files:
            try:
                with open(csv_file, "r", encoding="utf-8-sig") as f:
                    csv_text = f.read()
                added, updated, total = append_to_master(csv_text)
                stats["csvs_mesclados"] += 1
                stats["linhas_adicionadas"] += added
                stats["linhas_atualizadas"] += updated
                stats["total_no_master"] = total
                print(f"  [OK] {folder.name}/{csv_file.name}: +{added} novas, {updated} atualizadas")
            except Exception as e:
                msg = f"{folder.name}/{csv_file.name}: {e}"
                stats["erros"].append(msg)
                print(f"  [ERRO] {msg}")
                continue

        # Move pasta inteira para _legado
        try:
            dest = LEGACY_DIR / folder.name
            if dest.exists():
                # Evita colisão — acrescenta timestamp
                dest = LEGACY_DIR / f"{folder.name}_{int(time.time())}"
            shutil.move(str(folder), str(dest))
            stats["pastas_processadas"] += 1
        except Exception as e:
            stats["erros"].append(f"mover {folder.name}: {e}")

    return stats


# ═════════════════════════════════════════════════════════════════════════════
#  FLUXO PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def fetch_dates(dates: list[datetime.date]) -> dict:
    """
    Baixa os CSVs das datas informadas e mescla no banco único.
    Retorna dict com estatísticas.
    """
    if not dates:
        raise ValueError("Lista de datas vazia.")

    dates = sorted(set(dates))
    driver = create_session()
    csv_texts = []
    falhas = []

    try:
        for requested_date in dates:
            date_str = requested_date.strftime("%d/%m/%Y")
            print(f"\n{'='*50}")
            print(f"Processando: {date_str}")
            last_error: Exception | None = None

            for attempt in range(1, 4):
                try:
                    if attempt > 1:
                        print("  Reabrindo sessão para evitar relatório antigo/em cache...")
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = create_session()

                    server_path = generate_report(driver, date_str)
                    csv_text = download_csv(driver, server_path)
                    if not _extract_data_rows(csv_text):
                        raise ValueError("CSV baixado sem linhas de dados válidas.")
                    _validate_daily_csv(csv_text, requested_date)
                    csv_texts.append(csv_text)
                    break
                except Exception as e:
                    last_error = e
                    print(f"  [FALHA tentativa {attempt}/3] {date_str}: {e}")
                    if attempt < 3:
                        time.sleep(3)
            else:
                falhas.append(f"{date_str}: {last_error}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print(f"\n{'='*50}")
    print(f"Mesclando dados em {MASTER_CSV.name}...")
    total_added = 0
    total_updated = 0
    total_rows = 0
    for csv_text in csv_texts:
        added, updated, total = append_to_master(csv_text)
        total_added += added
        total_updated += updated
        total_rows = total
    print(f"[OK] Linhas novas: {total_added} | atualizadas: {total_updated} | total no master: {total_rows}")
    print(f"\nConcluído! Banco único: {MASTER_CSV}")

    return {
        "datas_solicitadas": len(dates),
        "datas_baixadas": len(csv_texts),
        "linhas_adicionadas": total_added,
        "linhas_atualizadas": total_updated,
        "total_no_master": total_rows,
        "falhas": falhas,
    }


def fetch_date_range(start_date: datetime.date, end_date: datetime.date) -> dict:
    """
    Baixa todas as datas no intervalo [start_date, end_date] (inclusive)
    e mescla no banco único.
    """
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    dates = []
    d = start_date
    while d <= end_date:
        dates.append(d)
        d += datetime.timedelta(days=1)
    return fetch_dates(dates)


def main():
    """Coleta ontem+hoje do supervisório e anexa ao banco único."""
    today     = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    fetch_dates([yesterday, today])


if __name__ == "__main__":
    main()
