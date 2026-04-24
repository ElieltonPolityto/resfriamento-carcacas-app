from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Optional
import datetime
import os
import subprocess
import unicodedata

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st


class DataValidationError(ValueError):
    """Erro de integridade ou estrutura dos dados de entrada."""


# ============================================================
# CONFIGURAÇÃO GERAL DA APLICAÇÃO
# ============================================================

st.set_page_config(
    page_title="Câmara de Resfriados - Análise de Ciclos",
    layout="wide",
)

REQUIRED_COLUMNS = {
    "timestamp",
    "carregamento",
    "resfriamento",
    "ventiladores_ec",
    "temp_entrada_glicol",
    "temp_ref",
    "temp_retorno_ar",
    "temperatura_espeto",
    "umidade_relativa_camara",
}


PHASE_ORDER = [
    "1. Carregamento",
    "2. Resfriamento | retorno > 10 °C",
    "3. Resfriamento | 5 < retorno ≤ 10 °C",
    "4. Resfriamento | 0 ≤ retorno ≤ 5 °C",
    "5. Pós-meta | espeto ≤ 7 °C",
]

PHASE_RESFRIAMENTO = PHASE_ORDER[1:]

PHASE_DISPLAY = {
    "1. Carregamento": "Carregamento",
    "2. Resfriamento | retorno > 10 °C": "Retorno > 10 °C",
    "3. Resfriamento | 5 < retorno ≤ 10 °C": "5 < retorno ≤ 10 °C",
    "4. Resfriamento | 0 ≤ retorno ≤ 5 °C": "0 ≤ retorno ≤ 5 °C",
    "5. Pós-meta | espeto ≤ 7 °C": "Espeto ≤ 7 °C",
}

PHASE_COLORS = {
    "1. Carregamento": "#d9d9d9",
    "2. Resfriamento | retorno > 10 °C": "#cfe8ff",
    "3. Resfriamento | 5 < retorno ≤ 10 °C": "#a9d2ff",
    "4. Resfriamento | 0 ≤ retorno ≤ 5 °C": "#78b7ff",
    "5. Pós-meta | espeto ≤ 7 °C": "#bfe7d0",
}


# ============================================================
# ESTRUTURAS DE DADOS
# ============================================================

@dataclass
class CycleSummary:
    cycle_id: int
    inicio_ciclo: pd.Timestamp
    fim_ciclo: pd.Timestamp
    inicio_carregamento: pd.Timestamp
    fim_carregamento: pd.Timestamp
    inicio_resfriamento: Optional[pd.Timestamp]
    duracao_total_h: float
    duracao_carregamento_h: float
    duracao_resfriamento_h: Optional[float]
    espeto_inicial: Optional[float]
    espeto_final: Optional[float]
    tempo_ate_7h: Optional[float]



@dataclass
class PhaseBoundaries:
    inicio_ciclo: pd.Timestamp
    fim_ciclo: pd.Timestamp
    inicio_carregamento: Optional[pd.Timestamp]
    fim_carregamento: Optional[pd.Timestamp]
    inicio_resfriamento: Optional[pd.Timestamp]
    primeira_vez_retorno_le_10: Optional[pd.Timestamp]
    primeira_vez_retorno_le_5: Optional[pd.Timestamp]
    primeira_vez_espeto_le_7: Optional[pd.Timestamp]


# ============================================================
# FUNÇÕES DE NORMALIZAÇÃO E FORMATAÇÃO
# ============================================================

def normalize_text(value: str) -> str:
    """
    Normaliza textos para facilitar comparação de nomes de colunas.
    """
    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("\n", " ").replace("\r", " ")
    value = " ".join(value.split())
    return value


def canonical_column_name(raw_name: str, position: int) -> str:
    """
    Traduz nomes de colunas brutas para nomes internos padronizados.
    """
    normalized = normalize_text(raw_name)

    if position == 0 and (
        normalized == ""
        or normalized.startswith("unnamed")
        or normalized == "nan"
    ):
        return "timestamp"

    mapping = {
        "carregamento": "carregamento",
        "resfriamento": "resfriamento",
        "saida y1 - ventiladores ec": "ventiladores_ec",
        "saida y1 ventiladores ec": "ventiladores_ec",
        "ventiladores ec": "ventiladores_ec",
        "temp entrada glicol": "temp_entrada_glicol",
        "temp ref": "temp_ref",
        "temp retorno ar": "temp_retorno_ar",
        "temp retorno de ar": "temp_retorno_ar",
        "temperatuda espeto": "temperatura_espeto",
        "temperatura espeto": "temperatura_espeto",
        "temperatura do espeto": "temperatura_espeto",
        "umidade relativa da camara": "umidade_relativa_camara",
        "umidade relativa da camera": "umidade_relativa_camara",
    }

    return mapping.get(normalized, normalized)


def parse_numeric_series(series: pd.Series) -> pd.Series:
    """
    Converte texto numérico com vírgula decimal em float.
    """
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def parse_timestamp_series(series: pd.Series) -> pd.Series:
    """
    Converte timestamps ISO e BR sem deixar datas ambiguas para inferencia.
    """
    text = series.astype(str).str.strip().str.strip('"')
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    rules = (
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$", "%Y-%m-%d %H:%M:%S.%f"),
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", "%Y-%m-%d %H:%M:%S"),
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", "%Y-%m-%d %H:%M"),
        (r"^\d{4}-\d{2}-\d{2}$", "%Y-%m-%d"),
        (r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$", "%d/%m/%Y %H:%M:%S"),
        (r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$", "%d/%m/%Y %H:%M"),
        (r"^\d{2}/\d{2}/\d{4}$", "%d/%m/%Y"),
    )

    for pattern, fmt in rules:
        mask = parsed.isna() & text.str.match(pattern, na=False)
        if mask.any():
            parsed.loc[mask] = pd.to_datetime(text.loc[mask], format=fmt, errors="coerce")

    remaining = parsed.isna() & text.ne("") & text.ne("nan")
    if remaining.any():
        parsed.loc[remaining] = pd.to_datetime(
            text.loc[remaining],
            errors="coerce",
            dayfirst=True,
        )

    return parsed


def parse_on_off(value: object) -> Optional[bool]:
    """
    Converte ON/OFF/TRUE/FALSE/1/0 em boolean.
    """
    if pd.isna(value):
        return None

    normalized = normalize_text(str(value))

    truthy = {"on", "true", "1", "ligado", "sim"}
    falsy = {"off", "false", "0", "desligado", "nao", "não"}

    if normalized in truthy:
        return True
    if normalized in falsy:
        return False

    return None


def format_float(value: Optional[float], suffix: str = "", decimals: int = 2) -> str:
    """
    Formata valores numéricos de forma segura para exibição.
    """
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{decimals}f}{suffix}"


def format_hours(value: Optional[float]) -> str:
    return format_float(value, " h", 2)


def format_temp(value: Optional[float]) -> str:
    return format_float(value, " °C", 1)


# ============================================================
# LEITURA DOS ARQUIVOS
# ============================================================

def try_decode_file(file_path: Path) -> str:
    """
    Tenta ler arquivo com codificações comuns em export industrial.
    """
    for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return file_path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue

    raise ValueError(f"Não foi possível decodificar o arquivo: {file_path.name}")


def detect_header_row(lines: list[str]) -> int:
    """
    Detecta a linha real de cabeçalho do arquivo.
    """
    header_tokens = [
        "carregamento",
        "resfriamento",
        "temp entrada glicol",
        "temp retorno ar",
        "temperat",
        "umidade relativa",
    ]

    best_index = -1
    best_score = -1

    for idx, line in enumerate(lines):
        line_norm = normalize_text(line)
        score = sum(token in line_norm for token in header_tokens)

        if score > best_score:
            best_score = score
            best_index = idx

    if best_index == -1 or best_score < 3:
        raise ValueError("Não foi possível identificar a linha de cabeçalho.")

    return best_index


def detect_delimiter(header_line: str) -> str:
    """
    Detecta delimitador do arquivo.
    """
    if "\t" in header_line:
        return "\t"
    if ";" in header_line:
        return ";"

    raise ValueError("Delimitador não identificado. Esperado tabulação ou ';'.")


def format_missing_required_columns_error(
    file_name: str,
    missing_columns: set[str],
    available_columns: list[str],
) -> str:
    """
    Monta uma mensagem clara para arquivos sem as colunas minimas esperadas.
    """
    missing_label = ", ".join(sorted(missing_columns))
    available_label = ", ".join(available_columns) if available_columns else "nenhuma"
    return (
        f"O arquivo {file_name} nao pode ser usado porque faltam colunas obrigatorias: "
        f"{missing_label}. "
        "Sem essas colunas nao e possivel identificar corretamente os ciclos nem calcular "
        "os indicadores do relatorio. "
        "Atualize o banco em 'Coletar dados' ou exporte o CSV novamente incluindo essas "
        f"variaveis. Colunas encontradas: {available_label}."
    )


def load_single_file(file_path: Path) -> pd.DataFrame:
    """
    Lê um único arquivo e retorna DataFrame limpo e padronizado.
    """
    raw_text = try_decode_file(file_path)
    lines = raw_text.splitlines(keepends=True)

    header_idx = detect_header_row(lines)
    header_line = lines[header_idx]
    delimiter = detect_delimiter(header_line)

    data_text = "".join(lines[header_idx:])

    df = pd.read_csv(
        StringIO(data_text),
        sep=delimiter,
        engine="python",
        dtype=str,
    )

    df.columns = [
        canonical_column_name(col, position=i)
        for i, col in enumerate(df.columns)
    ]

    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise DataValidationError(
            format_missing_required_columns_error(
                file_name=file_path.name,
                missing_columns=missing,
                available_columns=list(df.columns),
            )
        )

    df["timestamp"] = parse_timestamp_series(df["timestamp"])

    numeric_columns = [
        "ventiladores_ec",
        "temp_entrada_glicol",
        "temp_ref",
        "temp_retorno_ar",
        "temperatura_espeto",
        "umidade_relativa_camara",
    ]
    for col in numeric_columns:
        df[col] = parse_numeric_series(df[col])

    df["carregamento"] = df["carregamento"].apply(parse_on_off)
    df["resfriamento"] = df["resfriamento"].apply(parse_on_off)

    df = df.dropna(subset=["timestamp"]).copy()
    df["arquivo_origem"] = file_path.name

    return df


@st.cache_data(show_spinner=False)
def load_folder_data(folder_path: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Lê todos os arquivos .csv e .txt da pasta, consolida e ordena os dados.
    """
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")

    if not folder.is_dir():
        raise NotADirectoryError(f"O caminho informado não é uma pasta: {folder}")

    master = folder / "historico.csv"
    if master.exists():
        df = load_single_file(master)
        df = (
            df.sort_values("timestamp")
            .drop_duplicates(subset=["timestamp"], keep="last")
            .reset_index(drop=True)
        )
        return df, [master.name]

    files = sorted(list(folder.glob("*.csv")) + list(folder.glob("*.txt")))

    if not files:
        raise FileNotFoundError("Nenhum arquivo .csv ou .txt encontrado na pasta.")

    file_names = [f.name for f in files]
    dfs = [load_single_file(file_path) for file_path in files]

    df = pd.concat(dfs, ignore_index=True)

    df = (
        df.sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
        .reset_index(drop=True)
    )

    return df, file_names


# ============================================================
# DETECÇÃO DE CICLOS
# ============================================================

def assign_cycle_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Define início de novo ciclo quando carregamento passa de False para True.
    Gatilho primário: sinal boolean do operador (carregamento ON).
    A validação por temp_espeto é feita em build_cycle_summaries().
    """
    df = df.sort_values("timestamp").copy()

    carregamento_on = _as_bool_mask(df["carregamento"])
    # Detecta transição: False -> True (operador apertou botão de carregamento)
    cycle_ids = []
    current_cycle_id = 0
    active_cycle = False
    previous_loading = False
    previous_timestamp = None
    reached_target_espeto = False
    max_sampling_gap = pd.Timedelta(hours=6)

    for idx, row in df.iterrows():
        timestamp = row["timestamp"]
        loading = bool(carregamento_on.loc[idx])
        espeto = row["temperatura_espeto"]
        has_gap = (
            previous_timestamp is not None
            and pd.notna(timestamp)
            and pd.notna(previous_timestamp)
            and (timestamp - previous_timestamp) > max_sampling_gap
        )

        if has_gap:
            active_cycle = False
            previous_loading = False
            reached_target_espeto = False

        if active_cycle and reached_target_espeto and pd.notna(espeto) and espeto > 30:
            active_cycle = False
            previous_loading = False
            reached_target_espeto = False

        if loading and not previous_loading:
            current_cycle_id += 1
            active_cycle = True
            reached_target_espeto = False

        if active_cycle and pd.notna(espeto) and espeto <= 7:
            reached_target_espeto = True

        cycle_ids.append(current_cycle_id if active_cycle else pd.NA)
        previous_loading = loading
        previous_timestamp = timestamp

    df["cycle_id"] = cycle_ids

    return df


def build_cycle_summaries(df: pd.DataFrame) -> list[CycleSummary]:
    """
    Gera resumo dos ciclos válidos.
    """
    summaries: list[CycleSummary] = []

    valid_df = df.dropna(subset=["cycle_id"]).copy()
    if valid_df.empty:
        return summaries

    for cycle_id, group in valid_df.groupby("cycle_id", sort=True):
        group = group.sort_values("timestamp").copy()

        inicio_ciclo_original = group["timestamp"].min()

        # ─── VALIDAÇÃO 1: Deve ter carregamento ON (obrigatório — gatilho boolean) ───
        carregamento_on = _as_bool_mask(group["carregamento"])
        if not carregamento_on.any():
            continue

        # ─── VALIDAÇÃO 2: Deve ter resfriamento ON (obrigatório) ───
        resfriamento_on = _as_bool_mask(group["resfriamento"])
        if not resfriamento_on.any():
            continue

        # ─── VALIDAÇÃO 3: Espeto deve passar por >38°C (confirma porcos novos) ───
        if not (group["temperatura_espeto"] > 38).any():
            continue

        # ─── REGRA: Ignorar ciclos que começam em sábado (5) ou domingo (6) ───
        if inicio_ciclo_original.weekday() in (5, 6):
            continue

        # ─── REGRA: Ciclos de sexta-feira (4) cortar 3h após espeto ≤ 7°C ───
        if inicio_ciclo_original.weekday() == 4:
            # Só considera 7°C após o início do resfriamento (evita falso positivo)
            resfriamento_rows_check = group[resfriamento_on]
            if not resfriamento_rows_check.empty:
                inicio_resfr_check = resfriamento_rows_check["timestamp"].min()
                pos_resfr = group[group["timestamp"] >= inicio_resfr_check]
                espeto_at_target = pos_resfr[pos_resfr["temperatura_espeto"].le(7)]
                if not espeto_at_target.empty:
                    target_time = espeto_at_target["timestamp"].iloc[0]
                    cutoff_time = target_time + pd.Timedelta(hours=3)
                    group = group[group["timestamp"] <= cutoff_time].copy()
                    # Recalcula máscaras após corte
                    carregamento_on = _as_bool_mask(group["carregamento"])
                    resfriamento_on = _as_bool_mask(group["resfriamento"])

        carregamento_rows = group[carregamento_on]
        resfriamento_rows = group[resfriamento_on]

        inicio_ciclo = group["timestamp"].min()
        fim_ciclo = group["timestamp"].max()

        # Como garantimos carregamento ON, sempre usamos as linhas de carregamento
        inicio_carregamento = carregamento_rows["timestamp"].min()
        fim_carregamento = carregamento_rows["timestamp"].max()

        # ─── VALIDAÇÃO 4: Duração total entre 1h e 48h ───
        duracao_total_h_check = (fim_ciclo - inicio_ciclo).total_seconds() / 3600
        if duracao_total_h_check < 1 or duracao_total_h_check > 48:
            continue

        inicio_resfriamento = (
            resfriamento_rows[resfriamento_rows["timestamp"] >= fim_carregamento]["timestamp"].min()
            if not resfriamento_rows.empty
            else pd.NaT
        )

        if pd.isna(inicio_resfriamento) and not resfriamento_rows.empty:
            inicio_resfriamento = resfriamento_rows["timestamp"].min()

        duracao_total_h = (fim_ciclo - inicio_ciclo).total_seconds() / 3600
        duracao_carregamento_h = (fim_carregamento - inicio_carregamento).total_seconds() / 3600

        duracao_resfriamento_h = None
        if pd.notna(inicio_resfriamento):
            duracao_resfriamento_h = (fim_ciclo - inicio_resfriamento).total_seconds() / 3600

        espeto_inicial = (
            group["temperatura_espeto"].dropna().iloc[0]
            if group["temperatura_espeto"].notna().any()
            else None
        )
        espeto_final = (
            group["temperatura_espeto"].dropna().iloc[-1]
            if group["temperatura_espeto"].notna().any()
            else None
        )

        tempo_ate_7h = None
        if pd.notna(inicio_resfriamento):
            after_resfriamento = group[group["timestamp"] >= inicio_resfriamento].copy()
            target_rows = after_resfriamento[
                after_resfriamento["temperatura_espeto"].le(7)
            ]
            if not target_rows.empty:
                target_time = target_rows["timestamp"].iloc[0]
                tempo_ate_7h = (target_time - inicio_resfriamento).total_seconds() / 3600

        summaries.append(
            CycleSummary(
                cycle_id=int(cycle_id),
                inicio_ciclo=inicio_ciclo,
                fim_ciclo=fim_ciclo,
                inicio_carregamento=inicio_carregamento,
                fim_carregamento=fim_carregamento,
                inicio_resfriamento=inicio_resfriamento if pd.notna(inicio_resfriamento) else None,
                duracao_total_h=duracao_total_h,
                duracao_carregamento_h=duracao_carregamento_h,
                duracao_resfriamento_h=duracao_resfriamento_h,
                espeto_inicial=espeto_inicial,
                espeto_final=espeto_final,
                tempo_ate_7h=tempo_ate_7h,
            )
        )

    return summaries


def build_cycle_label(summary: CycleSummary) -> str:
    """
    Rótulo amigável para seleção do ciclo.
    """
    return (
        f"Ciclo {summary.cycle_id} | "
        f"Início: {summary.inicio_ciclo:%d/%m/%Y %H:%M} | "
        f"Fim: {summary.fim_ciclo:%d/%m/%Y %H:%M}"
    )


def _date_span(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """Lista datas calendario em intervalo inclusivo."""
    days = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += datetime.timedelta(days=1)
    return days


def _missing_dates_for_cycle(cycle_df: pd.DataFrame, summary: CycleSummary) -> list[datetime.date]:
    """Datas calendario do ciclo que nao possuem nenhuma amostra no dataframe carregado."""
    if summary.inicio_ciclo is None or summary.fim_ciclo is None or cycle_df.empty:
        return []

    required = set(_date_span(summary.inicio_ciclo.date(), summary.fim_ciclo.date()))
    timestamps = pd.to_datetime(cycle_df["timestamp"], errors="coerce").dropna()
    present = set(timestamps.dt.date.unique())
    return sorted(required - present)


def _resolve_user_output_dir(raw_path: str) -> Path:
    """Resolve pasta informada pelo usuario; caminhos relativos ficam dentro do projeto."""
    cleaned = raw_path.strip()
    if not cleaned:
        raise ValueError("Informe uma pasta de saida.")

    path = Path(cleaned).expanduser()
    if not path.is_absolute():
        path = Path(__file__).parent / path
    return path


def render_single_cycle_pdf_export(
    cycle_df: pd.DataFrame,
    selected_summary: CycleSummary,
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """Exporta somente o ciclo selecionado em PDF."""
    st.subheader("Exportar PDF do ciclo selecionado")
    st.caption(
        "Gera um PDF apenas para o ciclo selecionado. "
        "Se faltar qualquer data entre o inicio e o fim do ciclo, a exportacao fica bloqueada."
    )

    missing_dates = _missing_dates_for_cycle(cycle_df, selected_summary)
    if missing_dates:
        missing_label = ", ".join(day.strftime("%d/%m/%Y") for day in missing_dates)
        st.error(
            "Nao e possivel gerar o PDF deste ciclo porque faltam dados para: "
            f"{missing_label}. Busque essas datas antes de exportar."
        )

    output_dir_raw = st.text_input(
        "Pasta de saida do PDF",
        value="reports",
        key=f"single_pdf_output_dir_{selected_summary.cycle_id}",
        help="Use um caminho absoluto local ou uma pasta relativa, como reports.",
    )

    disabled = bool(missing_dates) or not output_dir_raw.strip()
    if st.button(
        "Gerar PDF deste ciclo",
        type="primary",
        use_container_width=True,
        disabled=disabled,
        key=f"single_pdf_button_{selected_summary.cycle_id}",
    ):
        try:
            from gerar_relatorio import format_cycle_filename, generate_pdf_for_cycle

            output_dir = _resolve_user_output_dir(output_dir_raw)
            output_dir.mkdir(parents=True, exist_ok=True)

            report_df = add_derived_columns(cycle_df.copy(), rate_window_minutes)
            filename = (
                f"{selected_summary.cycle_id:03d}_"
                f"{format_cycle_filename(selected_summary.inicio_ciclo, selected_summary.fim_ciclo)}.pdf"
            )
            output_path = output_dir / filename

            with st.spinner(f"Gerando {filename}..."):
                generate_pdf_for_cycle(
                    output_path=output_path,
                    cycle_df=report_df,
                    summary=selected_summary,
                    tolerance_band=tolerance_band,
                    rate_window_minutes=rate_window_minutes,
                )

            st.success(f"PDF gerado em: `{output_path}`")
            st.download_button(
                label=f"Baixar {filename}",
                data=output_path.read_bytes(),
                file_name=filename,
                mime="application/pdf",
                key=f"single_pdf_download_{selected_summary.cycle_id}_{int(output_path.stat().st_mtime)}",
                use_container_width=True,
            )
        except Exception as exc:
            st.error(f"Erro ao gerar PDF: {exc}")


# ============================================================
# CÁLCULOS TÉRMICOS
# ============================================================

def select_cycle_df(df: pd.DataFrame, summary: CycleSummary) -> pd.DataFrame:
    """
    Seleciona somente as linhas do intervalo analitico consolidado do ciclo.
    """
    return (
        df[
            (df["cycle_id"] == summary.cycle_id)
            & (df["timestamp"] >= summary.inicio_ciclo)
            & (df["timestamp"] <= summary.fim_ciclo)
        ]
        .sort_values("timestamp")
        .reset_index(drop=True)
        .copy()
    )


def _as_bool_mask(series: pd.Series) -> pd.Series:
    """
    Converte série booleana com None/NaN para máscara booleana segura.
    """
    return series.map(lambda value: value is True).fillna(False)


def build_phase_boundaries(cycle_df: pd.DataFrame) -> PhaseBoundaries:
    """
    Determina os marcos temporais das 5 fases do ciclo.
    """
    df = cycle_df.sort_values("timestamp").copy()

    carregamento_on = _as_bool_mask(df["carregamento"])
    resfriamento_on = _as_bool_mask(df["resfriamento"])

    carregamento_df = df[carregamento_on]
    resfriamento_df = df[resfriamento_on]

    inicio_carregamento = (
        carregamento_df["timestamp"].min()
        if not carregamento_df.empty
        else None
    )
    fim_carregamento = (
        carregamento_df["timestamp"].max()
        if not carregamento_df.empty
        else None
    )

    if fim_carregamento is not None:
        resfriamento_apos_carreg = resfriamento_df[
            resfriamento_df["timestamp"] > fim_carregamento
        ]
    else:
        resfriamento_apos_carreg = resfriamento_df

    if not resfriamento_apos_carreg.empty:
        inicio_resfriamento = resfriamento_apos_carreg["timestamp"].min()
    elif not resfriamento_df.empty:
        inicio_resfriamento = resfriamento_df["timestamp"].min()
    else:
        inicio_resfriamento = None

    def first_timestamp(mask: pd.Series) -> Optional[pd.Timestamp]:
        valid = df.loc[mask, "timestamp"]
        return valid.iloc[0] if not valid.empty else None

    if inicio_resfriamento is not None:
        after_resfriamento = df["timestamp"] >= inicio_resfriamento
    else:
        after_resfriamento = pd.Series(False, index=df.index)

    primeira_vez_retorno_le_10 = first_timestamp(
        after_resfriamento
        & resfriamento_on
        & df["temp_retorno_ar"].le(10)
    )

    primeira_vez_retorno_le_5 = first_timestamp(
        after_resfriamento
        & resfriamento_on
        & df["temp_retorno_ar"].le(5)
    )

    primeira_vez_espeto_le_7 = first_timestamp(
        after_resfriamento
        & resfriamento_on
        & df["temperatura_espeto"].le(7)
    )

    return PhaseBoundaries(
        inicio_ciclo=df["timestamp"].min(),
        fim_ciclo=df["timestamp"].max(),
        inicio_carregamento=inicio_carregamento,
        fim_carregamento=fim_carregamento,
        inicio_resfriamento=inicio_resfriamento,
        primeira_vez_retorno_le_10=primeira_vez_retorno_le_10,
        primeira_vez_retorno_le_5=primeira_vez_retorno_le_5,
        primeira_vez_espeto_le_7=primeira_vez_espeto_le_7,
    )


def slice_target_window(
    valid_df: pd.DataFrame,
    boundaries: PhaseBoundaries,
) -> pd.DataFrame:
    """
    Recorta a janela do inicio do resfriamento ate a meta de espeto <= 7 C.

    Quando a meta ainda nao foi atingida, retorna um DataFrame vazio com o
    mesmo schema para permitir relatorios parciais sem quebrar o pipeline.
    """
    empty_like = valid_df.iloc[0:0].copy()
    if valid_df.empty:
        return empty_like
    if (
        boundaries.inicio_resfriamento is None
        or boundaries.primeira_vez_espeto_le_7 is None
    ):
        return empty_like

    return valid_df[
        (valid_df["timestamp"] >= boundaries.inicio_resfriamento)
        & (valid_df["timestamp"] <= boundaries.primeira_vez_espeto_le_7)
    ].copy()


def classify_phase(
    row: pd.Series,
    boundaries: Optional[PhaseBoundaries] = None,
) -> str:
    """
    Classifica a fase de cada linha conforme a lógica operacional do ciclo.
    """
    if boundaries is None:
        if bool(row["carregamento"]) is True:
            return "1. Carregamento"
        if bool(row["resfriamento"]) is True:
            return "2. Resfriamento | retorno > 10 °C"
        return "0. Indefinido"

    timestamp = row["timestamp"]

    if row["carregamento"] is True:
        return "1. Carregamento"

    if row["resfriamento"] is not True:
        return "0. Indefinido"

    if (
        boundaries.primeira_vez_espeto_le_7 is not None
        and timestamp >= boundaries.primeira_vez_espeto_le_7
    ):
        return "5. Pós-meta | espeto ≤ 7 °C"

    if (
        boundaries.primeira_vez_retorno_le_5 is not None
        and timestamp >= boundaries.primeira_vez_retorno_le_5
    ):
        return "4. Resfriamento | 0 ≤ retorno ≤ 5 °C"

    if (
        boundaries.primeira_vez_retorno_le_10 is not None
        and timestamp >= boundaries.primeira_vez_retorno_le_10
    ):
        return "3. Resfriamento | 5 < retorno ≤ 10 °C"

    return "2. Resfriamento | retorno > 10 °C"


def estimate_sampling_seconds(timestamps: pd.Series) -> float:
    """
    Estima o passo temporal da amostragem pela mediana dos intervalos.
    """
    deltas = timestamps.sort_values().diff().dt.total_seconds().dropna()

    if deltas.empty:
        return 60.0

    median_seconds = float(deltas.median())
    if median_seconds <= 0:
        return 60.0

    return median_seconds


def calculate_cooling_rate(
    series: pd.Series,
    timestamps: pd.Series,
    window_minutes: int,
) -> pd.Series:
    """
    Calcula taxa de queda em °C/h.

    Convenção adotada:
    - valor positivo = temperatura caiu
    - valor negativo = temperatura subiu
    """
    sampling_seconds = estimate_sampling_seconds(timestamps)
    periods = max(int(round((window_minutes * 60) / sampling_seconds)), 1)

    delta_temp = series - series.shift(periods)
    hours = window_minutes / 60

    rate = -(delta_temp / hours)
    return rate


def add_derived_columns(df: pd.DataFrame, rate_window_minutes: int) -> pd.DataFrame:
    """
    Adiciona colunas derivadas para análise térmica.
    """
    df = df.sort_values("timestamp").copy()
    boundaries = build_phase_boundaries(df)

    df["fase"] = df.apply(
        lambda row: classify_phase(row, boundaries),
        axis=1,
    )

    df["dt_sistema"] = df["temp_retorno_ar"] - df["temp_entrada_glicol"]
    df["erro_glicol"] = df["temp_entrada_glicol"] - df["temp_ref"]
    df["erro_glicol_abs"] = df["erro_glicol"].abs()

    espeto_valid = df["temperatura_espeto"].dropna()
    retorno_valid = df["temp_retorno_ar"].dropna()

    espeto_inicial = espeto_valid.iloc[0] if not espeto_valid.empty else None
    retorno_inicial = retorno_valid.iloc[0] if not retorno_valid.empty else None

    df["delta_espeto_vs_inicio"] = (
        df["temperatura_espeto"] - espeto_inicial
        if espeto_inicial is not None
        else pd.NA
    )
    df["delta_retorno_vs_inicio"] = (
        df["temp_retorno_ar"] - retorno_inicial
        if retorno_inicial is not None
        else pd.NA
    )

    df[f"taxa_espeto_{rate_window_minutes}m"] = calculate_cooling_rate(
        df["temperatura_espeto"],
        df["timestamp"],
        rate_window_minutes,
    )

    df[f"taxa_retorno_ar_{rate_window_minutes}m"] = calculate_cooling_rate(
        df["temp_retorno_ar"],
        df["timestamp"],
        rate_window_minutes,
    )

    return df


def build_overall_metrics_table(
    cycle_df: pd.DataFrame,
    tolerance_band: float,
    rate_window_minutes: int,
) -> pd.DataFrame:
    """
    Consolida os indicadores gerais do ciclo.
    """
    boundaries = build_phase_boundaries(cycle_df)
    valid_df = cycle_df[cycle_df["fase"] != "0. Indefinido"].copy()
    resfriamento_df = valid_df[valid_df["fase"].isin(PHASE_RESFRIAMENTO)].copy()

    rate_esp_col = f"taxa_espeto_{rate_window_minutes}m"
    rate_ret_col = f"taxa_retorno_ar_{rate_window_minutes}m"

    if valid_df.empty:
        return pd.DataFrame()

    target_window_df = slice_target_window(valid_df, boundaries)

    espeto_valid = valid_df["temperatura_espeto"].dropna()
    retorno_valid = target_window_df["temp_retorno_ar"].dropna()
    retorno_final_ciclo_valid = valid_df["temp_retorno_ar"].dropna()

    espeto_inicial = espeto_valid.iloc[0] if not espeto_valid.empty else None
    espeto_final = espeto_valid.iloc[-1] if not espeto_valid.empty else None
    retorno_inicial = retorno_valid.iloc[0] if not retorno_valid.empty else None
    retorno_final = retorno_valid.iloc[-1] if not retorno_valid.empty else None
    retorno_final_ciclo = (
        retorno_final_ciclo_valid.iloc[-1]
        if not retorno_final_ciclo_valid.empty
        else None
    )
    queda_espeto = (
        espeto_inicial - espeto_final
        if espeto_inicial is not None and espeto_final is not None
        else None
    )
    queda_retorno = (
        retorno_inicial - retorno_final
        if retorno_inicial is not None and retorno_final is not None
        else None
    )

    tempo_ate_7h = None
    if (
        boundaries.inicio_resfriamento is not None
        and boundaries.primeira_vez_espeto_le_7 is not None
    ):
        tempo_ate_7h = (
            boundaries.primeira_vez_espeto_le_7 - boundaries.inicio_resfriamento
        ).total_seconds() / 3600
    folga_ate_16h = None
    atraso_acima_16h = None
    if tempo_ate_7h is not None:
        folga_ate_16h = max(16 - tempo_ate_7h, 0)
        atraso_acima_16h = max(tempo_ate_7h - 16, 0)

    duracao_total_h = (
        valid_df["timestamp"].max() - valid_df["timestamp"].min()
    ).total_seconds() / 3600

    duracao_carregamento_h = None
    if (
        boundaries.inicio_carregamento is not None
        and boundaries.fim_carregamento is not None
    ):
        duracao_carregamento_h = (
            boundaries.fim_carregamento - boundaries.inicio_carregamento
        ).total_seconds() / 3600

    duracao_resfriamento_h = None
    if boundaries.inicio_resfriamento is not None:
        duracao_resfriamento_h = (
            valid_df["timestamp"].max() - boundaries.inicio_resfriamento
        ).total_seconds() / 3600

    pct_dentro_faixa = (
        valid_df["erro_glicol_abs"].le(tolerance_band).mean() * 100
        if valid_df["erro_glicol_abs"].notna().any()
        else None
    )

    return pd.DataFrame(
        [
            {
                "Tempo até espeto ≤ 7 °C (h)": tempo_ate_7h,
                "Folga até 16 h (h)": folga_ate_16h,
                "Atraso acima de 16 h (h)": atraso_acima_16h,
                "Duração total (h)": duracao_total_h,
                "Duração carregamento (h)": duracao_carregamento_h,
                "Duração resfriamento (h)": duracao_resfriamento_h,
                "Espeto inicial (°C)": espeto_inicial,
                "Espeto final (°C)": espeto_final,
                "Queda espeto (°C)": queda_espeto,
                "Retorno inicial (°C)": retorno_inicial,
                "Retorno final (°C)": retorno_final,
                "Retorno final ciclo (°C)": retorno_final_ciclo,
                "Queda retorno ar (°C)": queda_retorno,
                "DT médio até espeto ≤ 7 °C (°C)": target_window_df["dt_sistema"].mean() if not target_window_df.empty else None,
                "DT médio total (°C)": valid_df["dt_sistema"].mean(),
                "DT médio resfriamento (°C)": resfriamento_df["dt_sistema"].mean(),
                "Erro médio glicol (°C)": valid_df["erro_glicol"].mean(),
                "Erro abs médio glicol (°C)": valid_df["erro_glicol_abs"].mean(),
                f"% dentro ±{tolerance_band:.1f} °C": pct_dentro_faixa,
                "Ventilação média (%)": valid_df["ventiladores_ec"].mean(),
                "Umidade média (%)": valid_df["umidade_relativa_camara"].mean(),
                f"Taxa média espeto {rate_window_minutes} min (°C/h)": resfriamento_df[rate_esp_col].mean(),
                f"Taxa média retorno ar {rate_window_minutes} min (°C/h)": resfriamento_df[rate_ret_col].mean(),
            }
        ]
    )


def build_phase_summary_table(
    cycle_df: pd.DataFrame,
    tolerance_band: float,
    rate_window_minutes: int,
) -> pd.DataFrame:
    """
    Monta tabela-resumo por fase do ciclo.
    """
    rate_esp_col = f"taxa_espeto_{rate_window_minutes}m"
    rate_ret_col = f"taxa_retorno_ar_{rate_window_minutes}m"

    rows = []

    for phase_name in PHASE_ORDER:
        phase_df = cycle_df[cycle_df["fase"] == phase_name].copy()
        if phase_df.empty:
            continue

        duration_h = (
            phase_df["timestamp"].max() - phase_df["timestamp"].min()
        ).total_seconds() / 3600

        pct_inside_band = (
            phase_df["erro_glicol_abs"].le(tolerance_band).mean() * 100
            if phase_df["erro_glicol_abs"].notna().any()
            else None
        )
        espeto_phase = phase_df["temperatura_espeto"].dropna()
        retorno_phase = phase_df["temp_retorno_ar"].dropna()
        espeto_ini = espeto_phase.iloc[0] if not espeto_phase.empty else None
        espeto_fim = espeto_phase.iloc[-1] if not espeto_phase.empty else None
        retorno_ini = retorno_phase.iloc[0] if not retorno_phase.empty else None
        retorno_fim = retorno_phase.iloc[-1] if not retorno_phase.empty else None
        queda_espeto = (
            espeto_ini - espeto_fim
            if espeto_ini is not None and espeto_fim is not None
            else None
        )
        queda_retorno = (
            retorno_ini - retorno_fim
            if retorno_ini is not None and retorno_fim is not None
            else None
        )

        rows.append(
            {
                "Fase": phase_name,
                "Início": phase_df["timestamp"].min(),
                "Fim": phase_df["timestamp"].max(),
                "Pontos": int(len(phase_df)),
                "Duração (h)": duration_h,
                "Temp espeto inicial (°C)": espeto_ini,
                "Temp espeto final (°C)": espeto_fim,
                "Queda espeto (°C)": queda_espeto,
                "Temp retorno inicial (°C)": retorno_ini,
                "Temp retorno final (°C)": retorno_fim,
                "Queda retorno ar (°C)": queda_retorno,
                "Temp espeto média (°C)": phase_df["temperatura_espeto"].mean(),
                "Temp espeto mín (°C)": phase_df["temperatura_espeto"].min(),
                "Temp espeto máx (°C)": phase_df["temperatura_espeto"].max(),
                "Temp retorno média (°C)": phase_df["temp_retorno_ar"].mean(),
                "Glicol entrada média (°C)": phase_df["temp_entrada_glicol"].mean(),
                "DT médio (°C)": phase_df["dt_sistema"].mean(),
                "DT mín (°C)": phase_df["dt_sistema"].min(),
                "DT máx (°C)": phase_df["dt_sistema"].max(),
                "Erro médio glicol (°C)": phase_df["erro_glicol"].mean(),
                "Erro abs médio glicol (°C)": phase_df["erro_glicol_abs"].mean(),
                f"% dentro faixa ±{tolerance_band:.1f} °C": pct_inside_band,
                "Ventilação média (%)": phase_df["ventiladores_ec"].mean(),
                "Umidade média (%)": phase_df["umidade_relativa_camara"].mean(),
                f"Taxa média espeto {rate_window_minutes} min (°C/h)": phase_df[rate_esp_col].mean(),
                f"Taxa média retorno ar {rate_window_minutes} min (°C/h)": phase_df[rate_ret_col].mean(),
            }
        )

    return pd.DataFrame(rows)


def generate_cycle_description(
    summary: "CycleSummary",
    overall_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    tolerance_band: float,
    rate_window_minutes: int = 60,
    max_cycle_hours: float = 16.0,
) -> list[str]:
    """
    Gera análise técnica do ciclo por fase e geral.
    Retorna lista de strings. Linhas com 'ATENCAO:' indicam pontos críticos.
    """
    if overall_df.empty:
        return ["Dados insuficientes para gerar a análise do ciclo."]

    row = overall_df.iloc[0]
    lines: list[str] = []

    def _get(col: str):
        try:
            v = row[col]
            return None if pd.isna(v) else v
        except KeyError:
            return None

    def _fmt(val, decimals: int = 1) -> str:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "—"
        return f"{val:.{decimals}f}"

    def _phase_row(phase_name: str):
        """Retorna a linha do phase_df para a fase informada, ou None."""
        if phase_df.empty:
            return None
        rows = phase_df[phase_df["Fase"] == phase_name]
        return rows.iloc[0] if not rows.empty else None

    # ─────────────────────────────────────────────────────────────────────────
    # SEÇÃO 1: VISÃO GERAL DO CICLO
    # ─────────────────────────────────────────────────────────────────────────
    lines.append("── VISÃO GERAL DO CICLO ──")

    inicio_str = summary.inicio_ciclo.strftime("%d/%m/%Y %H:%M") if summary.inicio_ciclo else "—"
    fim_str    = summary.fim_ciclo.strftime("%d/%m/%Y %H:%M")    if summary.fim_ciclo    else "—"
    dur_total  = _get("Duração total (h)")
    dur_carg   = _get("Duração carregamento (h)")
    dur_resf   = _get("Duração resfriamento (h)")

    lines.append(
        f"Período: {inicio_str} → {fim_str} | "
        f"Duração total: {_fmt(dur_total)} h | "
        f"Carregamento: {_fmt(dur_carg)} h | "
        f"Resfriamento: {_fmt(dur_resf)} h."
    )

    # Conformidade ao limite de 16 h (contado a partir do início do resfriamento)
    if dur_resf is not None:
        if dur_resf <= max_cycle_hours:
            lines.append(
                f"Tempo de resfriamento ({_fmt(dur_resf)} h) dentro do limite de {max_cycle_hours:.0f} h."
            )
        else:
            over = dur_resf - max_cycle_hours
            lines.append(
                f"ATENCAO: Resfriamento excedeu o limite de {max_cycle_hours:.0f} h "
                f"em {_fmt(over)} h (total: {_fmt(dur_resf)} h)."
            )

    # Meta 7°C: tempo contado a partir do início do resfriamento
    esp_ini = _get("Espeto inicial (°C)")
    esp_fin = _get("Espeto final (°C)")
    t_7 = summary.tempo_ate_7h  # já calculado a partir do início do resfriamento

    lines.append(
        f"T_espeto: inicial = {_fmt(esp_ini)} °C | final = {_fmt(esp_fin)} °C."
    )

    if t_7 is not None:
        if t_7 <= max_cycle_hours:
            lines.append(
                f"Meta T_espeto ≤ 7 °C atingida em {_fmt(t_7)} h após início do resfriamento "
                f"(limite: {max_cycle_hours:.0f} h)."
            )
        else:
            lines.append(
                f"ATENCAO: Meta T_espeto ≤ 7 °C atingida em {_fmt(t_7)} h — "
                f"excedeu o limite de {max_cycle_hours:.0f} h."
            )
    else:
        lines.append(
            f"ATENCAO: Meta T_espeto ≤ 7 °C não atingida. "
            f"Temperatura final do espeto: {_fmt(esp_fin)} °C."
        )

    # ─────────────────────────────────────────────────────────────────────────
    # SEÇÃO 2: ANÁLISE POR FASE
    # ─────────────────────────────────────────────────────────────────────────
    lines.append("")
    lines.append("── ANÁLISE POR FASE ──")

    fase_labels = {
        "1. Carregamento":                        "Fase 1 — Carregamento",
        "2. Resfriamento | retorno > 10 °C":      "Fase 2 — Resfriamento | Retorno > 10 °C",
        "3. Resfriamento | 5 < retorno ≤ 10 °C":  "Fase 3 — Resfriamento | 5 < Retorno ≤ 10 °C",
        "4. Resfriamento | 0 ≤ retorno ≤ 5 °C":   "Fase 4 — Resfriamento | 0 ≤ Retorno ≤ 5 °C",
        "5. Pós-meta | espeto ≤ 7 °C":            "Fase 5 — Pós-meta | Espeto ≤ 7 °C",
    }

    for phase_key, phase_label in fase_labels.items():
        pr = _phase_row(phase_key)
        if pr is None:
            continue

        def _p(col):
            try:
                v = pr[col]
                return None if pd.isna(v) else v
            except KeyError:
                return None

        dur_f    = _p("Duração (h)")
        esp_med  = _p("Temp espeto média (°C)")
        esp_min  = _p("Temp espeto mín (°C)")
        esp_max  = _p("Temp espeto máx (°C)")
        dt_med   = _p("DT médio (°C)")
        dt_min   = _p("DT mín (°C)")
        dt_max   = _p("DT máx (°C)")
        umid     = _p("Umidade média (%)")
        vent     = _p("Ventilação média (%)")
        pct_gli  = _p(f"% dentro faixa ±{tolerance_band:.1f} °C")
        taxa_esp = _p(f"Taxa média espeto {rate_window_minutes} min (°C/h)")

        lines.append(f"{phase_label} [{_fmt(dur_f)} h]:")
        lines.append(
            f"  T_espeto: méd={_fmt(esp_med)} °C | mín={_fmt(esp_min)} °C | máx={_fmt(esp_max)} °C | "
            f"Taxa: {_fmt(taxa_esp)} °C/h"
        )
        lines.append(
            f"  DT (retorno–glicol): méd={_fmt(dt_med)} °C | mín={_fmt(dt_min)} °C | máx={_fmt(dt_max)} °C"
        )
        lines.append(
            f"  Umidade: {_fmt(umid)} % | Ventiladores: {_fmt(vent)} % | "
            f"Glicol dentro da faixa: {_fmt(pct_gli)} %"
        )

        # Alertas por fase
        if umid is not None and umid > 98:
            lines.append(f"  ATENCAO: Umidade excessiva ({_fmt(umid)} %) — risco de condensação.")
        if umid is not None and umid < 80 and phase_key != "1. Carregamento":
            lines.append(f"  ATENCAO: Umidade abaixo de 80 % ({_fmt(umid)} %) — risco de ressecamento.")
        if dt_max is not None and dt_max > 8 and phase_key != "1. Carregamento":
            lines.append(f"  ATENCAO: DT máximo elevado ({_fmt(dt_max)} °C) — verificar formação de gelo no evaporador.")
        if pct_gli is not None and pct_gli < 70:
            lines.append(f"  ATENCAO: Controle do glicol insatisfatório ({_fmt(pct_gli)} % dentro da faixa ±{tolerance_band:.1f} °C).")

    # ─────────────────────────────────────────────────────────────────────────
    # SEÇÃO 3: ANÁLISE GERAL — GLICOL, DT E SÍNTESE
    # ─────────────────────────────────────────────────────────────────────────
    lines.append("")
    lines.append("── ANÁLISE GERAL ──")

    # Controle do glicol (global)
    pct_col = f"% dentro ±{tolerance_band:.1f} °C"
    pct_gli_geral = _get(pct_col)
    err_abs = _get("Erro abs médio glicol (°C)")
    if pct_gli_geral is not None:
        if pct_gli_geral >= 90:
            lines.append(
                f"Controle do glicol: {_fmt(pct_gli_geral)} % do ciclo dentro da faixa ±{tolerance_band:.1f} °C "
                f"(erro médio abs. = {_fmt(err_abs)} °C) — controle satisfatório."
            )
        elif pct_gli_geral >= 70:
            lines.append(
                f"Controle do glicol: {_fmt(pct_gli_geral)} % dentro da faixa ±{tolerance_band:.1f} °C "
                f"(erro médio abs. = {_fmt(err_abs)} °C) — controle moderado; revisar setpoint."
            )
        else:
            lines.append(
                f"ATENCAO: Controle do glicol: apenas {_fmt(pct_gli_geral)} % dentro da faixa ±{tolerance_band:.1f} °C "
                f"(erro médio abs. = {_fmt(err_abs)} °C) — controle insatisfatório; verificar válvula e setpoint."
            )

    # Evolução do DT ao longo das fases de resfriamento
    dt_por_fase = []
    for phase_key in PHASE_ORDER:
        pr = _phase_row(phase_key)
        if pr is not None:
            try:
                v = pr["DT médio (°C)"]
                if pd.notna(v):
                    dt_por_fase.append((PHASE_DISPLAY.get(phase_key, phase_key), v))
            except KeyError:
                pass

    if len(dt_por_fase) >= 2:
        dt_str = " → ".join(f"{label}: {_fmt(val)} °C" for label, val in dt_por_fase)
        lines.append(f"Evolução do DT por fase: {dt_str}.")
        dt_vals = [v for _, v in dt_por_fase]
        if dt_vals[-1] - dt_vals[0] > 2:
            lines.append(
                "ATENCAO: DT crescente ao longo do ciclo — possível acúmulo de gelo no evaporador ou queda de capacidade."
            )
        elif dt_vals[-1] - dt_vals[0] < -2:
            lines.append(
                "DT decrescente ao longo do ciclo — redução natural da carga térmica conforme resfriamento avança."
            )

    # Síntese do carregamento
    if dur_carg is not None:
        if dur_carg < 1.0:
            lines.append(f"Carregamento de curta duração ({_fmt(dur_carg)} h) — lote reduzido.")
        elif dur_carg <= 3.0:
            lines.append(f"Carregamento em {_fmt(dur_carg)} h — dentro do padrão operacional.")
        else:
            lines.append(
                f"ATENCAO: Carregamento prolongado ({_fmt(dur_carg)} h) — carga térmica inicial elevada; "
                f"avaliar impacto no tempo de resfriamento."
            )

    return lines


def build_cycle_comparison_table(
    df: pd.DataFrame,
    summaries: list[CycleSummary],
    tolerance_band: float,
    rate_window_minutes: int,
) -> pd.DataFrame:
    """
    Consolida métricas por ciclo para comparação entre ciclos.
    """
    rows = []

    for summary in summaries:
        cycle_df = select_cycle_df(df, summary)

        cycle_df = add_derived_columns(cycle_df, rate_window_minutes)

        valid_df = cycle_df[cycle_df["fase"] != "0. Indefinido"].copy()
        carregamento_df = valid_df[valid_df["fase"] == "1. Carregamento"].copy()
        resfriamento_df = valid_df[valid_df["fase"].isin(PHASE_RESFRIAMENTO)].copy()
        boundaries = build_phase_boundaries(cycle_df)
        target_window_df = slice_target_window(valid_df, boundaries)

        rate_esp_col = f"taxa_espeto_{rate_window_minutes}m"
        rate_ret_col = f"taxa_retorno_ar_{rate_window_minutes}m"

        dt_medio_total = valid_df["dt_sistema"].mean()
        dt_medio_carregamento = carregamento_df["dt_sistema"].mean()
        dt_medio_resfriamento = resfriamento_df["dt_sistema"].mean()

        erro_abs_medio = valid_df["erro_glicol_abs"].mean()
        pct_dentro_faixa = (
            valid_df["erro_glicol_abs"].le(tolerance_band).mean() * 100
            if valid_df["erro_glicol_abs"].notna().any()
            else None
        )

        ventilacao_media = valid_df["ventiladores_ec"].mean()
        umidade_media = valid_df["umidade_relativa_camara"].mean()
        umidade_min = valid_df["umidade_relativa_camara"].min()

        espeto_valid = valid_df["temperatura_espeto"].dropna()
        retorno_valid = target_window_df["temp_retorno_ar"].dropna()

        espeto_inicial = espeto_valid.iloc[0] if not espeto_valid.empty else None
        espeto_final = espeto_valid.iloc[-1] if not espeto_valid.empty else None
        retorno_ar_inicial = retorno_valid.iloc[0] if not retorno_valid.empty else None
        retorno_ar_final = retorno_valid.iloc[-1] if not retorno_valid.empty else None
        queda_espeto = (
            espeto_inicial - espeto_final
            if espeto_inicial is not None and espeto_final is not None
            else None
        )
        queda_retorno = (
            retorno_ar_inicial - retorno_ar_final
            if retorno_ar_inicial is not None and retorno_ar_final is not None
            else None
        )
        folga_ate_16h = None
        atraso_acima_16h = None
        if summary.tempo_ate_7h is not None:
            folga_ate_16h = max(16 - summary.tempo_ate_7h, 0)
            atraso_acima_16h = max(summary.tempo_ate_7h - 16, 0)

        taxa_media_espeto = resfriamento_df[rate_esp_col].mean()
        taxa_media_retorno = resfriamento_df[rate_ret_col].mean()

        rows.append(
            {
                "cycle_id": summary.cycle_id,
                "inicio_ciclo": summary.inicio_ciclo,
                "fim_ciclo": summary.fim_ciclo,
                "duracao_total_h": summary.duracao_total_h,
                "duracao_carregamento_h": summary.duracao_carregamento_h,
                "duracao_resfriamento_h": summary.duracao_resfriamento_h,
                "espeto_inicial": espeto_inicial,
                "espeto_final": espeto_final,
                "retorno_ar_inicial": retorno_ar_inicial,
                "retorno_ar_final": retorno_ar_final,
                "queda_espeto": queda_espeto,
                "queda_retorno_ar": queda_retorno,
                "tempo_ate_7h": summary.tempo_ate_7h,
                "folga_ate_16h": folga_ate_16h,
                "atraso_acima_16h": atraso_acima_16h,
                "dt_medio_total": dt_medio_total,
                "dt_medio_carregamento": dt_medio_carregamento,
                "dt_medio_resfriamento": dt_medio_resfriamento,
                "erro_abs_medio": erro_abs_medio,
                "pct_dentro_faixa": pct_dentro_faixa,
                "ventilacao_media": ventilacao_media,
                "umidade_media": umidade_media,
                "umidade_min": umidade_min,
                "taxa_media_espeto": taxa_media_espeto,
                "taxa_media_retorno": taxa_media_retorno,
            }
        )

    comparison_df = pd.DataFrame(rows).sort_values("inicio_ciclo").reset_index(drop=True)

    if comparison_df.empty:
        return comparison_df

    def normalize_min_better(series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty or valid.nunique() <= 1:
            return pd.Series([1.0] * len(series), index=series.index)
        return 1 - ((series - valid.min()) / (valid.max() - valid.min()))

    def normalize_max_better(series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty or valid.nunique() <= 1:
            return pd.Series([1.0] * len(series), index=series.index)
        return (series - valid.min()) / (valid.max() - valid.min())

    comparison_df["score_dt"] = normalize_min_better(comparison_df["dt_medio_resfriamento"])
    comparison_df["score_erro"] = normalize_min_better(comparison_df["erro_abs_medio"])
    comparison_df["score_umidade"] = normalize_max_better(comparison_df["umidade_media"])

    tempo_aux = comparison_df["tempo_ate_7h"].copy()
    if tempo_aux.notna().any():
        tempo_aux = tempo_aux.fillna(tempo_aux.max() * 1.2)
        comparison_df["score_tempo_7c"] = normalize_min_better(tempo_aux)
    else:
        comparison_df["score_tempo_7c"] = 0.0

    comparison_df["score_final"] = (
        0.30 * comparison_df["score_dt"]
        + 0.25 * comparison_df["score_erro"]
        + 0.20 * comparison_df["score_umidade"]
        + 0.25 * comparison_df["score_tempo_7c"]
    )

    comparison_df["ranking"] = (
        comparison_df["score_final"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    comparison_df = comparison_df.sort_values(
        ["ranking", "inicio_ciclo"],
        ascending=[True, True],
    ).reset_index(drop=True)

    return comparison_df


def classify_severity_level(series: pd.Series) -> pd.Series:
    """
    Classifica severidade do ciclo em Baixa / Média / Alta
    com base em tercis do indicador de severidade.
    """
    valid = series.dropna()
    if valid.empty:
        return pd.Series(["N/A"] * len(series), index=series.index)

    if valid.nunique() < 3:
        return pd.Series(["Média"] * len(series), index=series.index)

    q1 = valid.quantile(1 / 3)
    q2 = valid.quantile(2 / 3)

    def _classify(value: float) -> str:
        if pd.isna(value):
            return "N/A"
        if value <= q1:
            return "Baixa"
        if value <= q2:
            return "Média"
        return "Alta"

    return series.apply(_classify)


def build_severity_normalized_table(comparison_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria tabela comparativa normalizada por severidade do ciclo.

    Ideia central:
    - ciclos com espeto inicial mais quente exigem mais esforço;
    - carregamentos mais longos também tendem a aumentar severidade;
    - métricas são corrigidas por essa severidade inicial.
    """
    df = comparison_df.copy()

    if df.empty:
        return df

    # Carga térmica inicial acima de 7 °C
    df["carga_termica_inicial"] = (df["espeto_inicial"] - 7).clip(lower=0)

    # Severidade composta simples:
    # 70% da carga térmica inicial
    # 30% da duração de carregamento
    #
    # Escalas diferentes são normalizadas antes da combinação.
    def normalize_0_1(series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty or valid.nunique() <= 1:
            return pd.Series([1.0] * len(series), index=series.index)
        return (series - valid.min()) / (valid.max() - valid.min())

    carga_norm = normalize_0_1(df["carga_termica_inicial"])
    carregamento_norm = normalize_0_1(df["duracao_carregamento_h"])

    df["indice_severidade"] = 0.70 * carga_norm + 0.30 * carregamento_norm
    df["nivel_severidade"] = classify_severity_level(df["indice_severidade"])

    # Métricas corrigidas por severidade térmica inicial
    #
    # Quanto menor melhor:
    # - tempo por grau inicial
    # - DT por grau inicial
    #
    # Quanto maior melhor:
    # - umidade média
    #
    # Observação:
    # +0.1 evita divisão por zero em ciclos muito leves.
    denominator = df["carga_termica_inicial"].replace(0, pd.NA).fillna(0) + 0.1

    df["tempo_ate_7h_por_grau_inicial"] = df["tempo_ate_7h"] / denominator
    df["dt_resfriamento_por_grau_inicial"] = df["dt_medio_resfriamento"] / denominator
    df["taxa_espeto_por_grau_inicial"] = df["taxa_media_espeto"] / denominator

    # Score normalizado por severidade
    def normalize_min_better(series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty or valid.nunique() <= 1:
            return pd.Series([1.0] * len(series), index=series.index)
        return 1 - ((series - valid.min()) / (valid.max() - valid.min()))

    def normalize_max_better(series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty or valid.nunique() <= 1:
            return pd.Series([1.0] * len(series), index=series.index)
        return (series - valid.min()) / (valid.max() - valid.min())

    df["score_norm_tempo"] = normalize_min_better(df["tempo_ate_7h_por_grau_inicial"])
    df["score_norm_dt"] = normalize_min_better(df["dt_resfriamento_por_grau_inicial"])
    df["score_norm_erro"] = normalize_min_better(df["erro_abs_medio"])
    df["score_norm_umidade"] = normalize_max_better(df["umidade_media"])

    df["score_normalizado_final"] = (
        0.30 * df["score_norm_tempo"]
        + 0.30 * df["score_norm_dt"]
        + 0.20 * df["score_norm_erro"]
        + 0.20 * df["score_norm_umidade"]
    )

    df["ranking_normalizado"] = (
        df["score_normalizado_final"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    df = df.sort_values(
        ["ranking_normalizado", "inicio_ciclo"],
        ascending=[True, True],
    ).reset_index(drop=True)

    return df


# ============================================================
# PLOTAGEM
# ============================================================

THOMS_COLORS = {
    "espeto":     "#E63946",
    "retorno_ar": "#457B9D",
    "glicol":     "#2A9D8F",
    "ref":        "#6A4C93",
    "ventilacao": "#F4A261",
    "umidade":    "#264653",
    "dt":         "#E9C46A",
    "taxa":       "#E76F51",
}


def apply_thoms_style(fig: plt.Figure, ax: plt.Axes) -> None:
    """Aplica estilo visual consistente Thoms aos gráficos matplotlib."""
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#F5F5F5")
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#CCCCCC")
    ax.tick_params(colors="#555555", labelsize=9)
    ax.xaxis.label.set_color("#444444")
    ax.yaxis.label.set_color("#444444")
    ax.title.set_color("#1F4E78")
    ax.title.set_fontweight("bold")
    ax.title.set_fontsize(13)
    ax.grid(True, color="#DDDDDD", linewidth=0.6, zorder=0)
    ax.set_axisbelow(True)


def style_datetime_axis(ax: plt.Axes) -> None:
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m\n%H:%M"))
    ax.tick_params(axis="x", rotation=0, labelsize=8)


def build_phase_spans(cycle_df: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp, str]]:
    """
    Retorna os intervalos temporais de cada fase identificada no ciclo.
    """
    boundaries = build_phase_boundaries(cycle_df)
    end_ts = cycle_df["timestamp"].max()
    spans: list[tuple[pd.Timestamp, pd.Timestamp, str]] = []

    if (
        boundaries.inicio_carregamento is not None
        and boundaries.fim_carregamento is not None
    ):
        spans.append((boundaries.inicio_carregamento, boundaries.fim_carregamento, PHASE_ORDER[0]))

    if boundaries.inicio_resfriamento is not None:
        phase2_end = boundaries.primeira_vez_retorno_le_10 or end_ts
        if phase2_end >= boundaries.inicio_resfriamento:
            spans.append((boundaries.inicio_resfriamento, phase2_end, PHASE_ORDER[1]))

    if boundaries.primeira_vez_retorno_le_10 is not None:
        phase3_end = boundaries.primeira_vez_retorno_le_5 or end_ts
        if phase3_end >= boundaries.primeira_vez_retorno_le_10:
            spans.append((boundaries.primeira_vez_retorno_le_10, phase3_end, PHASE_ORDER[2]))

    if boundaries.primeira_vez_retorno_le_5 is not None:
        phase4_end = boundaries.primeira_vez_espeto_le_7 or end_ts
        if phase4_end >= boundaries.primeira_vez_retorno_le_5:
            spans.append((boundaries.primeira_vez_retorno_le_5, phase4_end, PHASE_ORDER[3]))

    if boundaries.primeira_vez_espeto_le_7 is not None:
        spans.append((boundaries.primeira_vez_espeto_le_7, end_ts, PHASE_ORDER[4]))

    return [(start, end, label) for start, end, label in spans if start is not None and end is not None and end >= start]


def add_cycle_phase_markers(
    ax: plt.Axes,
    cycle_df: pd.DataFrame,
    cycle_summary: Optional[CycleSummary] = None,
) -> None:
    """
    Adiciona as fases como faixas coloridas com rótulos no topo do gráfico.
    """
    boundaries = build_phase_boundaries(cycle_df)
    spans = build_phase_spans(cycle_df)

    for start, end, phase_name in spans:
        ax.axvspan(
            start,
            end,
            facecolor=PHASE_COLORS.get(phase_name, "#e0e0e0"),
            alpha=0.28,
            edgecolor="none",
            zorder=0,
        )

        midpoint = start + (end - start) / 2
        ax.text(
            midpoint,
            0.98,
            PHASE_DISPLAY.get(phase_name, phase_name),
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="top",
            fontsize=8.5,
            bbox={"boxstyle": "round,pad=0.22", "facecolor": "white", "alpha": 0.72, "edgecolor": "none"},
            clip_on=True,
        )

    for marker_ts, label in [
        (boundaries.inicio_resfriamento, "Início resfr."),
        (boundaries.primeira_vez_retorno_le_10, "Ret ≤ 10"),
        (boundaries.primeira_vez_retorno_le_5, "Ret ≤ 5"),
        (boundaries.primeira_vez_espeto_le_7, "Espeto ≤ 7"),
    ]:
        if marker_ts is not None:
            ax.axvline(
                marker_ts,
                linestyle="--",
                linewidth=1.1,
                color="#3b82c4",
                alpha=0.85,
                zorder=1,
            )
            ax.text(
                marker_ts,
                1.01,
                label,
                transform=ax.get_xaxis_transform(),
                ha="left",
                va="bottom",
                fontsize=8,
                color="#2b6ea6",
                clip_on=False,
            )


def annotate_transition_values(
    ax: plt.Axes,
    cycle_df: pd.DataFrame,
    columns: list[tuple[str, str, str]],
    y_offsets: Optional[list[int]] = None,
) -> None:
    """
    Rotula valores nos principais marcos de transicao sem poluir toda a curva.
    """
    boundaries = build_phase_boundaries(cycle_df)
    transition_points = [
        boundaries.inicio_resfriamento,
        boundaries.primeira_vez_retorno_le_10,
        boundaries.primeira_vez_retorno_le_5,
        boundaries.primeira_vez_espeto_le_7,
    ]
    transition_points = [ts for ts in transition_points if ts is not None]
    if not transition_points:
        return

    offsets = y_offsets or [14, -18, 28]
    for ts in transition_points:
        nearest_idx = (cycle_df["timestamp"] - ts).abs().idxmin()
        row = cycle_df.loc[nearest_idx]
        for idx, (col, label, color) in enumerate(columns):
            value = row.get(col)
            if pd.isna(value):
                continue
            ax.annotate(
                f"{label}: {value:.1f}",
                xy=(row["timestamp"], value),
                xytext=(6, offsets[idx % len(offsets)]),
                textcoords="offset points",
                fontsize=7.2,
                color=color,
                bbox={
                    "boxstyle": "round,pad=0.18",
                    "facecolor": "white",
                    "alpha": 0.82,
                    "edgecolor": color,
                    "linewidth": 0.4,
                },
                arrowprops={"arrowstyle": "-", "color": color, "lw": 0.5, "alpha": 0.7},
                zorder=5,
            )


def plot_temperature_overview(cycle_df: pd.DataFrame, cycle_summary: CycleSummary) -> plt.Figure:
    """
    Gráfico principal do ciclo.
    """
    fig, ax = plt.subplots(figsize=(15, 6))

    ax.plot(
        cycle_df["timestamp"],
        cycle_df["temperatura_espeto"],
        label="Temperatura espeto (°C)",
        color=THOMS_COLORS["espeto"],
        linewidth=2.5,
        alpha=0.9,
        zorder=3,
    )
    ax.plot(
        cycle_df["timestamp"],
        cycle_df["temp_retorno_ar"],
        label="Temp retorno de ar (°C)",
        color=THOMS_COLORS["retorno_ar"],
        linewidth=1.8,
        alpha=0.9,
        zorder=3,
    )
    ax.plot(
        cycle_df["timestamp"],
        cycle_df["temp_entrada_glicol"],
        label="Temp entrada glicol (°C)",
        color=THOMS_COLORS["glicol"],
        linewidth=1.8,
        alpha=0.9,
        zorder=3,
    )
    ax.plot(
        cycle_df["timestamp"],
        cycle_df["temp_ref"],
        label="Temp ref glicol (°C)",
        color=THOMS_COLORS["ref"],
        linewidth=1.6,
        linestyle="--",
        alpha=0.85,
        zorder=3,
    )

    add_cycle_phase_markers(ax, cycle_df, cycle_summary)
    annotate_transition_values(
        ax,
        cycle_df,
        [
            ("temperatura_espeto", "Espeto", THOMS_COLORS["espeto"]),
            ("temp_retorno_ar", "Retorno", THOMS_COLORS["retorno_ar"]),
            ("temp_entrada_glicol", "Glicol", THOMS_COLORS["glicol"]),
        ],
    )

    ax.set_title("Visão térmica do ciclo")
    ax.set_xlabel("Tempo")
    ax.set_ylabel("Temperatura (°C)")
    style_datetime_axis(ax)
    apply_thoms_style(fig, ax)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=4, framealpha=0.92, fontsize=9)

    fig.tight_layout(rect=[0, 0.08, 1, 1])
    return fig


def plot_operational_overview(cycle_df: pd.DataFrame, cycle_summary: CycleSummary) -> plt.Figure:
    """
    Gráfico operacional auxiliar.
    """
    fig, ax1 = plt.subplots(figsize=(15, 5))

    ax1.plot(
        cycle_df["timestamp"],
        cycle_df["ventiladores_ec"],
        label="Ventiladores EC (%)",
        color=THOMS_COLORS["ventilacao"],
        linewidth=2.0,
        alpha=0.9,
        zorder=3,
    )
    ax1.set_ylabel("Ventiladores (%)")
    ax1.set_xlabel("Tempo")
    style_datetime_axis(ax1)

    ax2 = ax1.twinx()
    ax2.plot(
        cycle_df["timestamp"],
        cycle_df["umidade_relativa_camara"],
        label="Umidade relativa (%)",
        color=THOMS_COLORS["umidade"],
        linewidth=1.8,
        linestyle="--",
        alpha=0.85,
        zorder=3,
    )
    ax2.set_ylabel("Umidade relativa (%)")
    ax2.yaxis.label.set_color("#444444")
    ax2.tick_params(colors="#555555", labelsize=9)
    ax2.axhline(95, linestyle="-", linewidth=1.1, color="#2A9D8F", alpha=0.65, label="Target UR 95%")
    ax2.axhline(90, linestyle="--", linewidth=1.0, color="#E9C46A", alpha=0.7, label="Min. aceitavel UR 90%")

    add_cycle_phase_markers(ax1, cycle_df, cycle_summary)
    annotate_transition_values(
        ax1,
        cycle_df,
        [("ventiladores_ec", "Vent.", THOMS_COLORS["ventilacao"])],
        y_offsets=[14],
    )
    annotate_transition_values(
        ax2,
        cycle_df,
        [("umidade_relativa_camara", "UR", THOMS_COLORS["umidade"])],
        y_offsets=[-20],
    )

    ax1.set_title("Visão operacional do ciclo")
    apply_thoms_style(fig, ax1)

    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(
        lines_1 + lines_2,
        labels_1 + labels_2,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.18),
        ncol=4,
        framealpha=0.92,
        fontsize=9,
    )

    fig.tight_layout(rect=[0, 0.1, 1, 1])
    return fig


def plot_dt_series(cycle_df: pd.DataFrame, cycle_summary: CycleSummary) -> plt.Figure:
    """
    Plota o DT do sistema ao longo do ciclo.
    """
    fig, ax = plt.subplots(figsize=(15, 5))

    ts = cycle_df["timestamp"]
    dt = cycle_df["dt_sistema"]
    ax.plot(ts, dt, color=THOMS_COLORS["dt"], linewidth=2.0, label="DT sistema = retorno ar - entrada glicol", zorder=3)
    ax.fill_between(ts, dt, 0, where=(dt >= 0), alpha=0.18, color=THOMS_COLORS["dt"], zorder=2)
    ax.fill_between(ts, dt, 0, where=(dt < 0), alpha=0.18, color="#E63946", zorder=2)

    add_cycle_phase_markers(ax, cycle_df, cycle_summary)
    annotate_transition_values(
        ax,
        cycle_df,
        [("dt_sistema", "DT", THOMS_COLORS["dt"])],
        y_offsets=[14],
    )

    ax.set_title("DT do sistema ao longo do ciclo")
    ax.set_xlabel("Tempo")
    ax.set_ylabel("DT (°C)")
    style_datetime_axis(ax)
    apply_thoms_style(fig, ax)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=2, framealpha=0.92, fontsize=9)

    fig.tight_layout(rect=[0, 0.1, 1, 1])
    return fig


def plot_glycol_error(
    cycle_df: pd.DataFrame,
    cycle_summary: CycleSummary,
    tolerance_band: float,
) -> plt.Figure:
    """
    Plota o erro do glicol em relação ao setpoint.
    """
    fig, ax = plt.subplots(figsize=(15, 5))

    ax.plot(
        cycle_df["timestamp"],
        cycle_df["erro_glicol"],
        color=THOMS_COLORS["glicol"],
        linewidth=2.0,
        label="Erro do glicol = entrada glicol - setpoint",
        zorder=3,
    )

    ax.axhspan(
        -tolerance_band,
        tolerance_band,
        alpha=0.15,
        color=THOMS_COLORS["glicol"],
        label=f"Faixa ±{tolerance_band:.1f} °C",
        zorder=1,
    )

    ax.axhline(0, linestyle="--", linewidth=1.2, color="#888888", alpha=0.7, zorder=2)

    add_cycle_phase_markers(ax, cycle_df, cycle_summary)
    annotate_transition_values(
        ax,
        cycle_df,
        [("erro_glicol", "Erro", THOMS_COLORS["glicol"])],
        y_offsets=[14],
    )

    ax.set_title("Erro de controle do glicol")
    ax.set_xlabel("Tempo")
    ax.set_ylabel("Erro (°C)")
    style_datetime_axis(ax)
    apply_thoms_style(fig, ax)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=2, framealpha=0.92, fontsize=9)

    fig.tight_layout(rect=[0, 0.1, 1, 1])
    return fig


def plot_cooling_rate(
    cycle_df: pd.DataFrame,
    cycle_summary: CycleSummary,
    rate_window_minutes: int,
    variable: str,
) -> plt.Figure:
    """
    Plota taxa de queda da temperatura em °C/h.
    """
    if variable == "espeto":
        col = f"taxa_espeto_{rate_window_minutes}m"
        title = f"Taxa de queda da temperatura do espeto ({rate_window_minutes} min)"
        ylabel = "Taxa de queda do espeto (°C/h)"
        label = "Taxa de queda espeto"
    elif variable == "retorno_ar":
        col = f"taxa_retorno_ar_{rate_window_minutes}m"
        title = f"Taxa de queda da temperatura de retorno de ar ({rate_window_minutes} min)"
        ylabel = "Taxa de queda do retorno de ar (°C/h)"
        label = "Taxa de queda retorno de ar"
    else:
        raise ValueError("Variável inválida em plot_cooling_rate.")

    fig, ax = plt.subplots(figsize=(15, 5))

    ts = cycle_df["timestamp"]
    vals = cycle_df[col]
    ax.plot(ts, vals, color=THOMS_COLORS["taxa"], linewidth=2.0, label=label, zorder=3)
    ax.fill_between(ts, vals, 0, where=(vals >= 0), alpha=0.18, color="#2A9D8F", zorder=2)
    ax.fill_between(ts, vals, 0, where=(vals < 0), alpha=0.18, color="#E63946", zorder=2)

    ax.axhline(0, linestyle="--", linewidth=1.2, color="#888888", alpha=0.7, zorder=2)

    add_cycle_phase_markers(ax, cycle_df, cycle_summary)
    annotate_transition_values(
        ax,
        cycle_df,
        [(col, "Taxa", THOMS_COLORS["taxa"])],
        y_offsets=[14],
    )

    ax.set_title(title)
    ax.set_xlabel("Tempo")
    ax.set_ylabel(ylabel)
    style_datetime_axis(ax)
    apply_thoms_style(fig, ax)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=2, framealpha=0.92, fontsize=9)

    fig.tight_layout(rect=[0, 0.1, 1, 1])
    return fig


def plot_dt_humidity_correlation(cycle_df: pd.DataFrame, cycle_summary: CycleSummary) -> plt.Figure:
    """
    Relaciona DT do sistema com umidade relativa para avaliar trade-off operacional.
    """
    df = cycle_df.dropna(subset=["dt_sistema", "umidade_relativa_camara", "timestamp"]).copy()
    fig, ax = plt.subplots(figsize=(10, 6))

    if df.empty:
        ax.text(0.5, 0.5, "Dados insuficientes", ha="center", va="center", transform=ax.transAxes)
        return fig

    if cycle_summary.inicio_resfriamento is not None:
        ref_time = cycle_summary.inicio_resfriamento
    else:
        ref_time = df["timestamp"].min()

    df["hora_resfriamento"] = (df["timestamp"] - ref_time).dt.total_seconds() / 3600
    corr = df["dt_sistema"].corr(df["umidade_relativa_camara"])

    scatter = ax.scatter(
        df["dt_sistema"],
        df["umidade_relativa_camara"],
        c=df["hora_resfriamento"],
        cmap="viridis",
        s=32,
        alpha=0.78,
        edgecolor="white",
        linewidth=0.4,
        zorder=3,
    )
    ax.axhline(95, color="#2A9D8F", linewidth=1.2, linestyle="-", alpha=0.7, label="Target UR 95%")
    ax.axhline(90, color="#E9C46A", linewidth=1.1, linestyle="--", alpha=0.8, label="Min. aceitavel UR 90%")

    ax.set_title(f"Relacao entre DT e umidade relativa (r={corr:.2f})" if pd.notna(corr) else "Relacao entre DT e umidade relativa")
    ax.set_xlabel("DT sistema = retorno ar - entrada glicol (°C)")
    ax.set_ylabel("Umidade relativa (%)")
    apply_thoms_style(fig, ax)
    cbar = fig.colorbar(scatter, ax=ax, pad=0.015)
    cbar.set_label("Horas desde inicio do resfriamento")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=2, framealpha=0.92, fontsize=9)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    return fig


def plot_hourly_averages(cycle_df: pd.DataFrame, cycle_summary: CycleSummary) -> plt.Figure:
    """
    Consolida medias por hora para mostrar a evolucao operacional do ciclo.
    """
    df = cycle_df.dropna(subset=["timestamp"]).copy()
    if cycle_summary.inicio_resfriamento is not None:
        ref_time = cycle_summary.inicio_resfriamento
        df = df[df["timestamp"] >= ref_time].copy()
    else:
        ref_time = df["timestamp"].min()

    fig, axes = plt.subplots(4, 1, figsize=(14, 9), sharex=True)
    if df.empty:
        axes[0].text(0.5, 0.5, "Dados insuficientes", ha="center", va="center", transform=axes[0].transAxes)
        return fig

    df["hora_resfriamento"] = ((df["timestamp"] - ref_time).dt.total_seconds() // 3600).astype(int)
    hourly = (
        df.groupby("hora_resfriamento", as_index=False)
        .agg(
            temperatura_espeto=("temperatura_espeto", "mean"),
            dt_sistema=("dt_sistema", "mean"),
            umidade_relativa_camara=("umidade_relativa_camara", "mean"),
            ventiladores_ec=("ventiladores_ec", "mean"),
        )
    )

    x = hourly["hora_resfriamento"]
    width = 0.72
    series = [
        (axes[0], "temperatura_espeto", "Espeto medio (°C)", THOMS_COLORS["espeto"]),
        (axes[1], "dt_sistema", "DT medio (°C)", THOMS_COLORS["dt"]),
        (axes[2], "umidade_relativa_camara", "UR media (%)", THOMS_COLORS["umidade"]),
        (axes[3], "ventiladores_ec", "Ventilacao media (%)", THOMS_COLORS["ventilacao"]),
    ]

    for ax, col, ylabel, color in series:
        ax.bar(x, hourly[col], width=width, color=color, alpha=0.76, edgecolor="white", linewidth=0.6)
        ax.set_ylabel(ylabel)
        ax.grid(True, axis="y", alpha=0.25)
        for xpos, value in zip(x, hourly[col]):
            if pd.notna(value):
                ax.text(xpos, value, f"{value:.1f}", ha="center", va="bottom", fontsize=7)

    axes[0].axhline(7, color="#2A9D8F", linestyle="--", linewidth=1.1, alpha=0.75)
    axes[2].axhline(95, color="#2A9D8F", linestyle="-", linewidth=1.1, alpha=0.75)
    axes[2].axhline(90, color="#E9C46A", linestyle="--", linewidth=1.0, alpha=0.8)

    axes[0].set_title("Medias horarias a partir do inicio do resfriamento")
    axes[-1].set_xlabel("Hora de resfriamento")
    axes[-1].set_xticks(x)
    axes[-1].set_xticklabels([f"H{int(v)}" for v in x], rotation=0)
    fig.tight_layout()
    return fig


def plot_cycle_metric_bar(
    comparison_df: pd.DataFrame,
    metric_col: str,
    title: str,
    ylabel: str,
) -> plt.Figure:
    """
    Gráfico de barras por ciclo para uma métrica consolidada.
    """
    fig, ax = plt.subplots(figsize=(14, 5))

    labels = [f"C{int(c)}" for c in comparison_df["cycle_id"]]
    values = comparison_df[metric_col]

    bars = ax.bar(labels, values, color="#1F4E78", alpha=0.82, edgecolor="white", linewidth=0.5, zorder=3)
    for bar, val in zip(bars, values):
        if pd.notna(val):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{val:.2f}",
                ha="center", va="bottom", fontsize=8, color="#333333",
            )
    ax.set_title(title)
    ax.set_xlabel("Ciclo")
    ax.set_ylabel(ylabel)
    apply_thoms_style(fig, ax)

    fig.tight_layout()
    return fig


def plot_cycle_scatter(
    comparison_df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
) -> plt.Figure:
    """
    Gráfico de dispersão para enxergar trade-off entre métricas de ciclo.
    """
    fig, ax = plt.subplots(figsize=(8, 6))

    ax.scatter(
        comparison_df[x_col],
        comparison_df[y_col],
        color=THOMS_COLORS["espeto"],
        s=80,
        alpha=0.85,
        edgecolors="white",
        linewidths=0.8,
        zorder=3,
    )

    for _, row in comparison_df.iterrows():
        ax.annotate(
            f"C{int(row['cycle_id'])}",
            (row[x_col], row[y_col]),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=9,
            color="#333333",
        )

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    apply_thoms_style(fig, ax)

    fig.tight_layout()
    return fig


def build_overall_metrics_display_table(
    cycle_df: pd.DataFrame,
    tolerance_band: float,
    rate_window_minutes: int,
) -> pd.DataFrame:
    """
    Resume indicadores gerais em formato vertical para leitura rápida.
    """
    overall_df = build_overall_metrics_table(cycle_df, tolerance_band, rate_window_minutes)
    if overall_df.empty:
        return pd.DataFrame(columns=["Indicador", "Valor"])

    row = overall_df.iloc[0]
    items = [
        ("Tempo até espeto ≤ 7 °C", format_hours(row["Tempo até espeto ≤ 7 °C (h)"])),
        ("Folga até 16 h", format_hours(row["Folga até 16 h (h)"])),
        ("Atraso acima de 16 h", format_hours(row["Atraso acima de 16 h (h)"])),
        ("Espeto inicial", format_float(row["Espeto inicial (°C)"], " °C", 1)),
        ("Espeto final", format_float(row["Espeto final (°C)"], " °C", 1)),
        ("Queda do espeto", format_float(row["Queda espeto (°C)"], " °C", 1)),
        ("Retorno no início do resfr.", format_float(row["Retorno inicial (°C)"], " °C", 1)),
        ("Retorno no espeto ≤ 7 °C", format_float(row["Retorno final (°C)"], " °C", 1)),
        ("Queda retorno até 7 °C", format_float(row["Queda retorno ar (°C)"], " °C", 1)),
        ("Duração total", format_hours(row["Duração total (h)"])),
        ("Duração carregamento", format_hours(row["Duração carregamento (h)"])),
        ("Duração resfriamento", format_hours(row["Duração resfriamento (h)"])),
        ("DT médio total", format_float(row["DT médio total (°C)"], " °C", 2)),
        ("DT médio até espeto ≤ 7 °C", format_float(row["DT médio até espeto ≤ 7 °C (°C)"], " °C", 2)),
        ("DT médio resfriamento", format_float(row["DT médio resfriamento (°C)"], " °C", 2)),
        ("Erro abs médio glicol", format_float(row["Erro abs médio glicol (°C)"], " °C", 2)),
        (f"Aderência dentro de ±{tolerance_band:.1f} °C", format_float(row[f"% dentro ±{tolerance_band:.1f} °C"], "%", 1)),
        ("Ventilação média", format_float(row["Ventilação média (%)"], "%", 1)),
        ("Umidade média", format_float(row["Umidade média (%)"], "%", 1)),
        (f"Taxa média espeto {rate_window_minutes} min", format_float(row[f"Taxa média espeto {rate_window_minutes} min (°C/h)"], " °C/h", 2)),
        (f"Taxa média retorno {rate_window_minutes} min", format_float(row[f"Taxa média retorno ar {rate_window_minutes} min (°C/h)"], " °C/h", 2)),
    ]
    return pd.DataFrame(items, columns=["Indicador", "Valor"])


def build_phase_summary_compact_table(
    cycle_df: pd.DataFrame,
    tolerance_band: float,
    rate_window_minutes: int,
    view: str = "general",
) -> pd.DataFrame:
    """
    Reduz a tabela por fase para um conjunto enxuto de colunas.
    """
    phase_df = build_phase_summary_table(cycle_df, tolerance_band, rate_window_minutes).copy()
    if phase_df.empty:
        return phase_df

    phase_df["Fase"] = phase_df["Fase"].map(PHASE_DISPLAY).fillna(phase_df["Fase"])

    if view == "general":
        cols = [
            "Fase",
            "Início",
            "Fim",
            "Duração (h)",
            "Temp espeto inicial (°C)",
            "Temp espeto final (°C)",
            "Queda espeto (°C)",
            "Temp retorno inicial (°C)",
            "Temp retorno final (°C)",
            "Queda retorno ar (°C)",
            "DT médio (°C)",
            f"Taxa média espeto {rate_window_minutes} min (°C/h)",
            f"Taxa média retorno ar {rate_window_minutes} min (°C/h)",
        ]
        renamed = {
            "Temp espeto inicial (°C)": "Espeto inicial (°C)",
            "Temp espeto final (°C)": "Espeto final (°C)",
            "Queda espeto (°C)": "Queda espeto (°C)",
            "Temp retorno inicial (°C)": "Retorno inicial (°C)",
            "Temp retorno final (°C)": "Retorno final (°C)",
            "Queda retorno ar (°C)": "Queda retorno (°C)",
            "DT médio (°C)": "DT méd. (°C)",
            f"Taxa média espeto {rate_window_minutes} min (°C/h)": f"Taxa espeto {rate_window_minutes}m (°C/h)",
            f"Taxa média retorno ar {rate_window_minutes} min (°C/h)": f"Taxa retorno {rate_window_minutes}m (°C/h)",
        }
    else:
        cols = [
            "Fase",
            "Duração (h)",
            "Queda espeto (°C)",
            "Queda retorno ar (°C)",
            "DT médio (°C)",
            "Erro abs médio glicol (°C)",
            f"% dentro faixa ±{tolerance_band:.1f} °C",
            "Ventilação média (%)",
            "Umidade média (%)",
        ]
        renamed = {
            "Queda retorno ar (°C)": "Queda retorno (°C)",
            "DT médio (°C)": "DT méd. (°C)",
            "Erro abs médio glicol (°C)": "Erro abs. méd. glicol (°C)",
            f"% dentro faixa ±{tolerance_band:.1f} °C": "% dentro faixa",
            "Ventilação média (%)": "Vent. média (%)",
            "Umidade média (%)": "Umidade média (%)",
        }

    return phase_df[cols].rename(columns=renamed)


def render_table_with_optional_details(
    title: str,
    compact_df: pd.DataFrame,
    detailed_df: pd.DataFrame,
    details_label: str,
) -> None:
    """
    Exibe tabela compacta e empurra a tabela detalhada para um expander.
    """
    st.subheader(title)
    st.dataframe(compact_df, use_container_width=True, hide_index=True)
    with st.expander(details_label, expanded=False):
        st.dataframe(detailed_df, use_container_width=True, hide_index=True)


# ============================================================
# FUNÇÕES DE INTERFACE
# ============================================================

def render_diagnostics(df: pd.DataFrame, file_names: list[str]) -> None:
    """
    Exibe diagnóstico de leitura.
    """
    with st.expander("Diagnóstico de leitura", expanded=False):
        st.write(f"Arquivos encontrados: {len(file_names)}")
        st.write(file_names)
        st.write("Colunas lidas:")
        st.write(list(df.columns))
        st.write(f"Total de linhas consolidadas: {len(df)}")
        st.write(f"Período consolidado: {df['timestamp'].min()} até {df['timestamp'].max()}")


def render_cycle_selector(summaries: list[CycleSummary]) -> tuple[int, CycleSummary]:
    """
    Exibe seletor de ciclo e retorna o selecionado.
    """
    cycle_options = {build_cycle_label(summary): summary.cycle_id for summary in summaries}

    selected_label = st.selectbox(
        "Selecione o ciclo para análise",
        options=list(cycle_options.keys()),
        index=len(cycle_options) - 1,
    )

    selected_cycle_id = cycle_options[selected_label]
    selected_summary = next(s for s in summaries if s.cycle_id == selected_cycle_id)

    return selected_cycle_id, selected_summary


# ============================================================
# HELPERS DO PAINEL 0 — status do banco e disponibilidade de datas
# ============================================================

@dataclass
class MasterStatus:
    exists: bool
    n_rows: int
    n_cycles: int
    first_date: Optional[datetime.date]
    last_date: Optional[datetime.date]
    size_kb: float


def _master_csv_path() -> Path:
    return Path(__file__).parent / "data" / "historico.csv"


def get_master_status() -> MasterStatus:
    """Resumo do banco único `data/historico.csv` (linhas, ciclos, primeira/última data)."""
    path = _master_csv_path()
    if not path.exists():
        return MasterStatus(False, 0, 0, None, None, 0.0)

    size_kb = path.stat().st_size / 1024
    try:
        from gerar_relatorio import load_folder_no_cache
        df, _ = load_folder_no_cache(str(path.parent))
    except Exception:
        return MasterStatus(True, 0, 0, None, None, size_kb)

    if df.empty:
        return MasterStatus(True, 0, 0, None, None, size_kb)

    df_with_ids = assign_cycle_ids(df)
    summaries = build_cycle_summaries(df_with_ids)
    ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
    first_date = ts.min().date() if not ts.empty else None
    last_date = ts.max().date() if not ts.empty else None
    return MasterStatus(True, len(df), len(summaries), first_date, last_date, size_kb)


def get_available_dates_in_master() -> set[datetime.date]:
    """Set de datas distintas presentes no banco (qualquer linha conta)."""
    path = _master_csv_path()
    if not path.exists():
        return set()
    try:
        from gerar_relatorio import load_folder_no_cache
        df, _ = load_folder_no_cache(str(path.parent))
    except Exception:
        return set()
    if df.empty:
        return set()
    ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
    return set(ts.dt.date.unique())


def check_data_availability(start: datetime.date, end: datetime.date) -> dict:
    """Retorna {'present': [...], 'missing': [...]} para o intervalo solicitado."""
    available = get_available_dates_in_master()
    days = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += datetime.timedelta(days=1)
    present = [d for d in days if d in available]
    missing = [d for d in days if d not in available]
    return {"present": present, "missing": missing}


def _resolve_output_dir(choice: str, custom: str) -> Path:
    """Converte a escolha do radio em Path real."""
    if choice == "Personalizada":
        return Path(custom).expanduser()
    if choice == "data/relatorios":
        return Path(__file__).parent / "data" / "relatorios"
    return Path(__file__).parent / "reports"


def _run_generation(
    start: datetime.date,
    end: datetime.date,
    output_path: Path,
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """Executa generate_reports filtrando por intervalo e mostra feedback."""
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        run_started = datetime.datetime.now().timestamp()
        with st.spinner(
            f"Gerando relatórios de {start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')}..."
        ):
            from gerar_relatorio import generate_reports
            data_dir = str(Path(__file__).parent / "data")
            n_ciclos = generate_reports(
                data_folder=data_dir,
                output_dir=str(output_path),
                rate_window=rate_window_minutes,
                tolerance=tolerance_band,
                date_start=pd.Timestamp(start),
                date_end=pd.Timestamp(end),
            )
        st.success(f"✅ {n_ciclos} ciclo(s) processado(s). Pasta: `{output_path}`")
        generated_files = sorted(
            [
                path for path in output_path.glob("*")
                if path.is_file()
                and path.suffix.lower() in {".pdf", ".xlsx"}
                and path.stat().st_mtime >= run_started - 1
            ],
            key=lambda path: path.name,
        )
        if generated_files:
            st.write("Arquivos gerados:")
            for file_path in generated_files:
                mime = (
                    "application/pdf"
                    if file_path.suffix.lower() == ".pdf"
                    else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.download_button(
                    label=f"Baixar {file_path.name}",
                    data=file_path.read_bytes(),
                    file_name=file_path.name,
                    mime=mime,
                    key=f"download_{file_path.name}_{int(file_path.stat().st_mtime)}",
                    use_container_width=True,
                )
    except DataValidationError as exc:
        st.error(
            "Nao foi possivel gerar os relatorios porque o banco de dados esta incompleto "
            "ou fora do padrao esperado."
        )
        st.write(str(exc))
        st.markdown(
            "O que fazer:\n"
            "1. Va em **Coletar dados** e atualize o banco.\n"
            "2. Se voce usa um CSV manual, exporte novamente com todas as variaveis obrigatorias.\n"
            "3. Tente gerar os relatorios outra vez."
        )
    except KeyError as exc:
        missing_column = str(exc).strip("'\"")
        st.error(
            f"Nao foi possivel gerar os relatorios porque a coluna obrigatoria "
            f"'{missing_column}' nao foi encontrada no banco de dados."
        )
        st.write(
            "Sem essa coluna o sistema nao consegue separar corretamente as fases do ciclo "
            "nem calcular os indicadores termicos."
        )
        st.markdown(
            "O que fazer:\n"
            "1. Va em **Coletar dados** e atualize o banco.\n"
            "2. Se o erro continuar, reexporte o CSV do supervisorio incluindo essa variavel.\n"
            "3. Gere os relatorios novamente."
        )
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao gerar relatórios: {e}")


def _run_fetch(start: datetime.date, end: datetime.date) -> dict | None:
    """Executa fetch_date_range; devolve stats ou None em caso de erro."""
    n_dias = (end - start).days + 1
    try:
        with st.spinner(
            f"Baixando {n_dias} dia(s) ({start.strftime('%d/%m/%Y')} → {end.strftime('%d/%m/%Y')})..."
        ):
            from gerar_relatorio_camcarcacas import fetch_date_range
            return fetch_date_range(start, end)
    except Exception as e:
        st.error(f"❌ Erro ao buscar dados: {e}")
        return None


@st.dialog("Faltam dados no banco")
def _confirm_fetch_dialog(
    missing_days: list,
    start: datetime.date,
    end: datetime.date,
    output_choice: str,
    custom_dir: str,
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    n = len(missing_days)
    st.write(f"Faltam **{n} dia(s)** no banco para o intervalo solicitado:")
    preview = ", ".join(d.strftime("%d/%m/%Y") for d in missing_days[:8])
    if n > 8:
        preview += f" … (+{n - 8})"
    st.caption(preview)
    st.write("O que deseja fazer?")
    col1, col2 = st.columns(2)
    if col1.button("🔄 Buscar e gerar", type="primary", use_container_width=True):
        st.session_state["_pending_fetch_then_generate"] = {
            "missing_min": min(missing_days),
            "missing_max": max(missing_days),
            "start": start,
            "end": end,
            "output_choice": output_choice,
            "custom_dir": custom_dir,
            "tolerance_band": tolerance_band,
            "rate_window_minutes": rate_window_minutes,
        }
        st.rerun()
    if col2.button("📊 Gerar só com o que tem", use_container_width=True):
        st.session_state["_pending_generate_only"] = {
            "start": start,
            "end": end,
            "output_choice": output_choice,
            "custom_dir": custom_dir,
            "tolerance_band": tolerance_band,
            "rate_window_minutes": rate_window_minutes,
        }
        st.rerun()


def render_panel_0(tolerance_band: float, rate_window_minutes: int) -> None:
    """
    Painel 0 — Controle e geração.
    Estrutura: status do banco + 3 abas (Gerar relatório / Coletar dados / Manutenção).
    """
    st.header("Painel 0 — Controle e geração")

    # ─── Status do banco (Etapa 1) ──────────────────────────────────────
    status = get_master_status()
    if not status.exists:
        st.warning(
            "⚠️ Banco `data/historico.csv` ainda não existe. "
            "Use a aba **Coletar dados** para criá-lo."
        )
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Linhas no banco", f"{status.n_rows:,}".replace(",", "."))
        c2.metric("Ciclos detectados", status.n_cycles)
        c3.metric(
            "Primeira data",
            status.first_date.strftime("%d/%m/%Y") if status.first_date else "—",
        )
        c4.metric(
            "Última data",
            status.last_date.strftime("%d/%m/%Y") if status.last_date else "—",
        )
        st.caption(f"Arquivo: `data/historico.csv` ({status.size_kb:.1f} KB)")

    st.divider()

    # ─── Processar pendências de modal ──────────────────────────────────
    if "_pending_fetch_then_generate" in st.session_state:
        req = st.session_state.pop("_pending_fetch_then_generate")
        stats = _run_fetch(req["missing_min"], req["missing_max"])
        if stats is not None:
            has_failures = bool(stats.get("falhas")) or (
                stats.get("datas_baixadas") != stats.get("datas_solicitadas")
            )
            if has_failures:
                st.warning(
                    "A coleta terminou com falhas parciais. "
                    "O relatório não foi gerado automaticamente para evitar dados incompletos."
                )
                if stats.get("falhas"):
                    st.warning(
                        "Dias que falharam:\n"
                        + "\n".join(f"- {f}" for f in stats["falhas"])
                    )
            else:
                output_path = _resolve_output_dir(req["output_choice"], req["custom_dir"])
                _run_generation(
                    req["start"],
                    req["end"],
                    output_path,
                    req.get("tolerance_band", tolerance_band),
                    req.get("rate_window_minutes", rate_window_minutes),
                )

    if "_pending_generate_only" in st.session_state:
        req = st.session_state.pop("_pending_generate_only")
        output_path = _resolve_output_dir(req["output_choice"], req["custom_dir"])
        _run_generation(
            req["start"],
            req["end"],
            output_path,
            req.get("tolerance_band", tolerance_band),
            req.get("rate_window_minutes", rate_window_minutes),
        )

    # ─── Abas (Etapa 2) ─────────────────────────────────────────────────
    tab_gerar, tab_coletar, tab_manut = st.tabs(
        ["📊 Gerar relatório", "🔄 Coletar dados", "🛠️ Manutenção"]
    )

    today = datetime.date.today()

    # =================== ABA: GERAR RELATÓRIO ===================
    with tab_gerar:
        st.subheader("Gerar relatórios para um intervalo")
        st.caption(
            "Escolha as datas dos ciclos. Se faltarem dados no banco, "
            "o sistema vai pedir confirmação para baixá-los antes."
        )

        col_a, col_b = st.columns(2)
        with col_a:
            rel_inicio = st.date_input(
                "Data inicial",
                value=today - datetime.timedelta(days=14),
                max_value=today,
                key="painel0_rel_inicio",
                format="DD/MM/YYYY",
            )
        with col_b:
            rel_fim = st.date_input(
                "Data final",
                value=today,
                max_value=today,
                key="painel0_rel_fim",
                format="DD/MM/YYYY",
            )

        # Pasta simplificada (Etapa 4)
        output_choice = st.radio(
            "Pasta de saída",
            ["Padrão (reports/)", "data/relatorios", "Personalizada"],
            horizontal=True,
            key="painel0_output_choice",
        )
        custom_dir = ""
        if output_choice == "Personalizada":
            custom_dir = st.text_input(
                "Caminho completo da pasta",
                value=st.session_state.get("painel0_custom_dir", ""),
                key="painel0_custom_dir",
                placeholder="reports/minha_saida",
            )

        if st.button(
            "📊 Gerar relatório",
            type="primary",
            use_container_width=True,
            key="painel0_btn_gerar",
        ):
            if rel_fim < rel_inicio:
                st.error("❌ Data final é anterior à data inicial.")
            elif output_choice == "Personalizada" and not custom_dir.strip():
                st.error("❌ Informe o caminho da pasta personalizada.")
            else:
                avail = check_data_availability(rel_inicio, rel_fim)
                if not avail["missing"]:
                    # Fluxo silencioso: tudo OK no banco
                    output_path = _resolve_output_dir(output_choice, custom_dir)
                    _run_generation(
                        rel_inicio,
                        rel_fim,
                        output_path,
                        tolerance_band,
                        rate_window_minutes,
                    )
                else:
                    # Faltam dados → modal
                    _confirm_fetch_dialog(
                        avail["missing"],
                        rel_inicio,
                        rel_fim,
                        output_choice,
                        custom_dir,
                        tolerance_band,
                        rate_window_minutes,
                    )

    # =================== ABA: COLETAR DADOS ===================
    with tab_coletar:
        st.subheader("Coletar dados do supervisório")
        st.caption(
            "Baixa um intervalo de datas e mescla no `historico.csv`. "
            "Use só quando quiser atualizar o banco sem gerar relatórios."
        )

        col_c, col_d = st.columns(2)
        with col_c:
            col_inicio = st.date_input(
                "Data inicial",
                value=today - datetime.timedelta(days=1),
                max_value=today,
                key="painel0_col_inicio",
                format="DD/MM/YYYY",
            )
        with col_d:
            col_fim = st.date_input(
                "Data final",
                value=today,
                max_value=today,
                key="painel0_col_fim",
                format="DD/MM/YYYY",
            )

        if st.button(
            "🔄 Buscar dados",
            type="primary",
            use_container_width=True,
            key="painel0_btn_coletar",
        ):
            if col_fim < col_inicio:
                st.error("❌ Data final é anterior à data inicial.")
            else:
                stats = _run_fetch(col_inicio, col_fim)
                if stats is not None:
                    st.success(
                        f"✅ {stats['datas_baixadas']}/{stats['datas_solicitadas']} dia(s) baixado(s) | "
                        f"+{stats['linhas_adicionadas']} novas, "
                        f"{stats['linhas_atualizadas']} atualizadas | "
                        f"total: {stats['total_no_master']}"
                    )
                    if stats.get("falhas"):
                        st.warning(
                            "Dias que falharam:\n"
                            + "\n".join(f"- {f}" for f in stats["falhas"])
                        )

    # =================== ABA: MANUTENÇÃO ===================
    with tab_manut:
        st.subheader("Operações de manutenção")
        st.caption("Operações raras que normalmente só rodam uma vez.")

        with st.expander("🗂️ Migrar pastas legadas para o banco único", expanded=False):
            st.caption(
                "Mescla pastas antigas `Ciclo N/` em `historico.csv` e move "
                "para `data/_legado_pastas_ciclos/`. Já deve ter sido executada."
            )
            if st.button(
                "Executar migração",
                key="painel0_btn_migrar",
                use_container_width=True,
            ):
                try:
                    with st.spinner("Migrando pastas legadas..."):
                        from gerar_relatorio_camcarcacas import migrate_legacy_folders
                        stats = migrate_legacy_folders()
                    if stats["pastas_processadas"] == 0:
                        st.info("Nenhuma pasta legada encontrada (migração já realizada).")
                    else:
                        st.success(
                            f"✅ {stats['pastas_processadas']} pasta(s) migrada(s) | "
                            f"{stats['csvs_mesclados']} CSV(s) | "
                            f"+{stats['linhas_adicionadas']} linhas novas, "
                            f"{stats['linhas_atualizadas']} atualizadas | "
                            f"total no master: {stats['total_no_master']}"
                        )
                    if stats.get("erros"):
                        st.warning("Avisos:\n" + "\n".join(f"- {e}" for e in stats["erros"][:10]))
                except Exception as e:
                    st.error(f"❌ Erro na migração: {e}")



def render_panel_1(
    cycle_df: pd.DataFrame,
    selected_summary: CycleSummary,
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """
    Painel 1 - Visão geral do ciclo.
    """
    st.header("Painel 1 - Visão geral do ciclo")

    cycle_df = add_derived_columns(cycle_df, rate_window_minutes)
    boundaries = build_phase_boundaries(cycle_df)
    overall_df = build_overall_metrics_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )
    overall_display_df = build_overall_metrics_display_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )
    phase_summary_df = build_phase_summary_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )
    phase_summary_compact_df = build_phase_summary_compact_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
        view="general",
    )

    if overall_df.empty:
        st.warning("Não foi possível calcular os indicadores do ciclo.")
        return

    row = overall_df.iloc[0]

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Duração total", format_hours(row["Duração total (h)"]))
    col2.metric("Carregamento", format_hours(row["Duração carregamento (h)"]))
    col3.metric("Resfriamento", format_hours(row["Duração resfriamento (h)"]))
    col4.metric("Espeto inicial", format_temp(row["Espeto inicial (°C)"]))
    col5.metric("Espeto final", format_temp(row["Espeto final (°C)"]))
    col6.metric("Tempo até 7 °C", format_hours(row["Tempo até espeto ≤ 7 °C (h)"]))

    st.divider()

    info1, info2, info3, info4 = st.columns(4)
    info1.write(f"Início do ciclo: {selected_summary.inicio_ciclo:%d/%m/%Y %H:%M}")
    info1.write(f"Fim do ciclo: {selected_summary.fim_ciclo:%d/%m/%Y %H:%M}")

    info2.write(f"Início do carregamento: {selected_summary.inicio_carregamento:%d/%m/%Y %H:%M}")
    info2.write(f"Fim do carregamento: {selected_summary.fim_carregamento:%d/%m/%Y %H:%M}")

    info3.write(
        "Início do resfriamento: "
        + (
            f"{boundaries.inicio_resfriamento:%d/%m/%Y %H:%M}"
            if boundaries.inicio_resfriamento is not None
            else "N/A"
        )
    )
    info3.write(
        "Retorno ≤ 10 °C: "
        + (
            f"{boundaries.primeira_vez_retorno_le_10:%d/%m/%Y %H:%M}"
            if boundaries.primeira_vez_retorno_le_10 is not None
            else "N/A"
        )
    )

    info4.write(
        "Retorno ≤ 5 °C: "
        + (
            f"{boundaries.primeira_vez_retorno_le_5:%d/%m/%Y %H:%M}"
            if boundaries.primeira_vez_retorno_le_5 is not None
            else "N/A"
        )
    )
    info4.write(
        "Espeto ≤ 7 °C: "
        + (
            f"{boundaries.primeira_vez_espeto_le_7:%d/%m/%Y %H:%M}"
            if boundaries.primeira_vez_espeto_le_7 is not None
            else "N/A"
        )
    )

    st.divider()

    render_single_cycle_pdf_export(
        cycle_df=cycle_df,
        selected_summary=selected_summary,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )

    st.divider()

    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.subheader("Indicadores gerais do ciclo")
        st.dataframe(overall_display_df, use_container_width=True, hide_index=True)

    with col_b:
        render_table_with_optional_details(
            title="Indicadores por fase",
            compact_df=phase_summary_compact_df,
            detailed_df=phase_summary_df,
            details_label="Abrir tabela detalhada por fase",
        )

    st.divider()

    st.subheader("Comportamento térmico do ciclo")
    fig_temp = plot_temperature_overview(cycle_df, selected_summary)
    st.pyplot(fig_temp, use_container_width=True)

    st.caption("As fases estão destacadas por faixas coloridas no fundo. As linhas verticais pontilhadas marcam as transições operacionais.")

    st.divider()

    st.subheader("Ventiladores e umidade")
    fig_oper = plot_operational_overview(cycle_df, selected_summary)
    st.pyplot(fig_oper, use_container_width=True)

    st.divider()

    with st.expander("Abrir dados brutos do ciclo", expanded=False):
        st.dataframe(
            cycle_df[
                [
                    "timestamp",
                    "fase",
                    "carregamento",
                    "resfriamento",
                    "ventiladores_ec",
                    "temp_entrada_glicol",
                    "temp_ref",
                    "temp_retorno_ar",
                    "temperatura_espeto",
                    "umidade_relativa_camara",
                    "arquivo_origem",
                ]
            ],
            use_container_width=True,
            height=350,
        )


def render_panel_2(
    cycle_df: pd.DataFrame,
    selected_summary: CycleSummary,
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """
    Painel 2 - Desempenho térmico.
    """
    st.header("Painel 2 - Desempenho térmico")

    cycle_df = add_derived_columns(cycle_df, rate_window_minutes)
    overall_display_df = build_overall_metrics_display_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )
    phase_summary_df = build_phase_summary_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )
    phase_summary_compact_df = build_phase_summary_compact_table(
        cycle_df=cycle_df,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
        view="technical",
    )

    resfriamento_df = cycle_df[cycle_df["fase"].isin(PHASE_RESFRIAMENTO)].copy()
    rate_esp_col = f"taxa_espeto_{rate_window_minutes}m"
    rate_ret_col = f"taxa_retorno_ar_{rate_window_minutes}m"

    dt_medio_carregamento = cycle_df.loc[
        cycle_df["fase"] == "1. Carregamento",
        "dt_sistema",
    ].mean()
    dt_medio_resfriamento = resfriamento_df["dt_sistema"].mean()
    erro_abs_medio = cycle_df["erro_glicol_abs"].mean()
    pct_dentro_faixa = cycle_df["erro_glicol_abs"].le(tolerance_band).mean() * 100
    taxa_media_espeto = resfriamento_df[rate_esp_col].mean()
    taxa_media_retorno = resfriamento_df[rate_ret_col].mean()

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("DT méd. carregamento", format_float(dt_medio_carregamento, " °C", 2))
    col2.metric("DT méd. resfriamento", format_float(dt_medio_resfriamento, " °C", 2))
    col3.metric("Erro abs méd. glicol", format_float(erro_abs_medio, " °C", 2))
    col4.metric(f"% dentro ±{tolerance_band:.1f} °C", format_float(pct_dentro_faixa, "%", 1))
    col5.metric(
        f"Taxa espeto {rate_window_minutes} min",
        format_float(taxa_media_espeto, " °C/h", 2),
    )
    col6.metric(
        f"Taxa retorno ar {rate_window_minutes} min",
        format_float(taxa_media_retorno, " °C/h", 2),
    )

    st.divider()

    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.subheader("Indicadores gerais do ciclo")
        st.dataframe(overall_display_df, use_container_width=True, hide_index=True)

    with col_b:
        render_table_with_optional_details(
            title="Resumo técnico por fase",
            compact_df=phase_summary_compact_df,
            detailed_df=phase_summary_df,
            details_label="Abrir tabela técnica detalhada por fase",
        )

    st.divider()

    st.subheader("DT do sistema")
    fig_dt = plot_dt_series(cycle_df, selected_summary)
    st.pyplot(fig_dt, use_container_width=True)

    st.divider()

    st.subheader("Aderência do glicol ao setpoint")
    fig_error = plot_glycol_error(
        cycle_df=cycle_df,
        cycle_summary=selected_summary,
        tolerance_band=tolerance_band,
    )
    st.pyplot(fig_error, use_container_width=True)

    st.divider()

    st.subheader("Taxa de queda da temperatura do espeto")
    fig_rate_esp = plot_cooling_rate(
        cycle_df=cycle_df,
        cycle_summary=selected_summary,
        rate_window_minutes=rate_window_minutes,
        variable="espeto",
    )
    st.pyplot(fig_rate_esp, use_container_width=True)

    st.divider()

    st.subheader("Taxa de queda da temperatura de retorno de ar")
    fig_rate_ret = plot_cooling_rate(
        cycle_df=cycle_df,
        cycle_summary=selected_summary,
        rate_window_minutes=rate_window_minutes,
        variable="retorno_ar",
    )
    st.pyplot(fig_rate_ret, use_container_width=True)

    st.caption("As faixas coloridas distinguem as fases. Os rótulos no topo mostram a fase ativa em cada trecho do ciclo.")

    st.divider()

    with st.expander("Abrir dados calculados do ciclo selecionado", expanded=False):
        display_cols = [
            "timestamp",
            "fase",
            "temp_entrada_glicol",
            "temp_ref",
            "temp_retorno_ar",
            "temperatura_espeto",
            "dt_sistema",
            "erro_glicol",
            "erro_glicol_abs",
            rate_esp_col,
            rate_ret_col,
            "arquivo_origem",
        ]
        st.dataframe(
            cycle_df[display_cols],
            use_container_width=True,
            height=350,
        )


def render_panel_3(
    df: pd.DataFrame,
    summaries: list[CycleSummary],
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """
    Painel 3 - Comparação entre ciclos.
    """
    st.header("Painel 3 - Comparação entre ciclos")

    comparison_df = build_cycle_comparison_table(
        df=df,
        summaries=summaries,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )

    if comparison_df.empty:
        st.warning("Não foi possível montar a tabela comparativa entre ciclos.")
        return

    col1, col2, col3, col4 = st.columns(4)

    melhor_ciclo = comparison_df.iloc[0]
    col1.metric("Melhor ciclo (ranking)", f"C{int(melhor_ciclo['cycle_id'])}")
    col2.metric("Melhor score", format_float(melhor_ciclo["score_final"], "", 3))
    col3.metric(
        "Melhor DT méd. resfriamento",
        format_float(comparison_df["dt_medio_resfriamento"].min(), " °C", 2),
    )
    col4.metric(
        "Melhor tempo até 7 °C",
        format_float(comparison_df["tempo_ate_7h"].min(), " h", 2),
    )

    st.divider()

    st.subheader("Tabela comparativa consolidada por ciclo")

    display_cols = [
        "ranking",
        "cycle_id",
        "inicio_ciclo",
        "fim_ciclo",
        "tempo_ate_7h",
        "folga_ate_16h",
        "atraso_acima_16h",
        "espeto_inicial",
        "espeto_final",
        "queda_espeto",
        "retorno_ar_inicial",
        "retorno_ar_final",
        "queda_retorno_ar",
        "duracao_total_h",
        "duracao_carregamento_h",
        "duracao_resfriamento_h",
        "dt_medio_carregamento",
        "dt_medio_resfriamento",
        "erro_abs_medio",
        "pct_dentro_faixa",
        "ventilacao_media",
        "umidade_media",
        "umidade_min",
        "taxa_media_espeto",
        "taxa_media_retorno",
        "score_final",
    ]

    st.dataframe(
        comparison_df[display_cols],
        use_container_width=True,
        hide_index=True,
        height=350,
    )

    st.divider()

    st.subheader("Comparação direta entre ciclos")

    fig_bar_dt = plot_cycle_metric_bar(
        comparison_df=comparison_df.sort_values("inicio_ciclo"),
        metric_col="dt_medio_resfriamento",
        title="DT médio no resfriamento por ciclo",
        ylabel="DT médio no resfriamento (°C)",
    )
    st.pyplot(fig_bar_dt, use_container_width=True)

    st.divider()

    fig_bar_tempo = plot_cycle_metric_bar(
        comparison_df=comparison_df.sort_values("inicio_ciclo"),
        metric_col="tempo_ate_7h",
        title="Tempo até o espeto atingir 7 °C por ciclo",
        ylabel="Tempo até 7 °C (h)",
    )
    st.pyplot(fig_bar_tempo, use_container_width=True)

    st.divider()

    fig_bar_umidade = plot_cycle_metric_bar(
        comparison_df=comparison_df.sort_values("inicio_ciclo"),
        metric_col="umidade_media",
        title="Umidade média por ciclo",
        ylabel="Umidade média (%)",
    )
    st.pyplot(fig_bar_umidade, use_container_width=True)

    st.divider()

    st.subheader("Trade-offs do processo")

    col_sc1, col_sc2 = st.columns(2)

    with col_sc1:
        fig_sc1 = plot_cycle_scatter(
            comparison_df=comparison_df,
            x_col="dt_medio_resfriamento",
            y_col="tempo_ate_7h",
            title="DT médio no resfriamento vs tempo até 7 °C",
            xlabel="DT médio no resfriamento (°C)",
            ylabel="Tempo até 7 °C (h)",
        )
        st.pyplot(fig_sc1, use_container_width=True)

    with col_sc2:
        fig_sc2 = plot_cycle_scatter(
            comparison_df=comparison_df,
            x_col="ventilacao_media",
            y_col="umidade_media",
            title="Ventilação média vs umidade média",
            xlabel="Ventilação média (%)",
            ylabel="Umidade média (%)",
        )
        st.pyplot(fig_sc2, use_container_width=True)

    st.divider()

    st.subheader("Ranking dos ciclos")

    ranking_df = comparison_df[
        [
            "ranking",
            "cycle_id",
            "score_final",
            "dt_medio_resfriamento",
            "erro_abs_medio",
            "umidade_media",
            "tempo_ate_7h",
        ]
    ].copy()

    st.dataframe(
        ranking_df,
        use_container_width=True,
        hide_index=True,
    )


def render_panel_4(
    df: pd.DataFrame,
    summaries: list[CycleSummary],
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """
    Painel 4 - Comparação normalizada por severidade.
    """
    st.header("Painel 4 - Comparação normalizada por severidade")

    comparison_df = build_cycle_comparison_table(
        df=df,
        summaries=summaries,
        tolerance_band=tolerance_band,
        rate_window_minutes=rate_window_minutes,
    )

    if comparison_df.empty:
        st.warning("Não foi possível montar a base comparativa.")
        return

    normalized_df = build_severity_normalized_table(comparison_df)

    if normalized_df.empty:
        st.warning("Não foi possível montar a comparação normalizada.")
        return

    col1, col2, col3, col4 = st.columns(4)

    melhor_ciclo = normalized_df.iloc[0]
    col1.metric("Melhor ciclo normalizado", f"C{int(melhor_ciclo['cycle_id'])}")
    col2.metric("Melhor score normalizado", format_float(melhor_ciclo["score_normalizado_final"], "", 3))
    col3.metric(
        "Melhor tempo/grau inicial",
        format_float(normalized_df["tempo_ate_7h_por_grau_inicial"].min(), " h/°C", 3),
    )
    col4.metric(
        "Melhor DT/grau inicial",
        format_float(normalized_df["dt_resfriamento_por_grau_inicial"].min(), " °C/°C", 3),
    )

    st.divider()

    st.subheader("Tabela normalizada por severidade")

    display_cols = [
        "ranking_normalizado",
        "cycle_id",
        "inicio_ciclo",
        "nivel_severidade",
        "indice_severidade",
        "espeto_inicial",
        "carga_termica_inicial",
        "duracao_carregamento_h",
        "tempo_ate_7h",
        "tempo_ate_7h_por_grau_inicial",
        "dt_medio_resfriamento",
        "dt_resfriamento_por_grau_inicial",
        "erro_abs_medio",
        "umidade_media",
        "score_normalizado_final",
    ]

    st.dataframe(
        normalized_df[display_cols],
        use_container_width=True,
        hide_index=True,
        height=350,
    )

    st.divider()

    st.subheader("Comparações corrigidas por severidade")

    fig_bar_tempo_norm = plot_cycle_metric_bar(
        comparison_df=normalized_df.sort_values("inicio_ciclo"),
        metric_col="tempo_ate_7h_por_grau_inicial",
        title="Tempo até 7 °C por grau inicial de espeto",
        ylabel="Tempo até 7 °C / grau inicial (h/°C)",
    )
    st.pyplot(fig_bar_tempo_norm, use_container_width=True)

    st.divider()

    fig_bar_dt_norm = plot_cycle_metric_bar(
        comparison_df=normalized_df.sort_values("inicio_ciclo"),
        metric_col="dt_resfriamento_por_grau_inicial",
        title="DT médio no resfriamento por grau inicial de espeto",
        ylabel="DT resfriamento / grau inicial",
    )
    st.pyplot(fig_bar_dt_norm, use_container_width=True)

    st.divider()

    st.subheader("Severidade vs desempenho")

    col_sc1, col_sc2 = st.columns(2)

    with col_sc1:
        fig_sc1 = plot_cycle_scatter(
            comparison_df=normalized_df,
            x_col="indice_severidade",
            y_col="tempo_ate_7h_por_grau_inicial",
            title="Severidade vs tempo corrigido por carga",
            xlabel="Índice de severidade",
            ylabel="Tempo até 7 °C / grau inicial (h/°C)",
        )
        st.pyplot(fig_sc1, use_container_width=True)

    with col_sc2:
        fig_sc2 = plot_cycle_scatter(
            comparison_df=normalized_df,
            x_col="indice_severidade",
            y_col="score_normalizado_final",
            title="Severidade vs score normalizado",
            xlabel="Índice de severidade",
            ylabel="Score normalizado final",
        )
        st.pyplot(fig_sc2, use_container_width=True)

    st.divider()

    st.subheader("Ranking normalizado")

    ranking_df = normalized_df[
        [
            "ranking_normalizado",
            "cycle_id",
            "nivel_severidade",
            "score_normalizado_final",
            "tempo_ate_7h_por_grau_inicial",
            "dt_resfriamento_por_grau_inicial",
            "erro_abs_medio",
            "umidade_media",
        ]
    ].copy()

    st.dataframe(
        ranking_df,
        use_container_width=True,
        hide_index=True,
    )


def render_panel_5(
    df: pd.DataFrame,
    summaries: list[CycleSummary],
    tolerance_band: float,
    rate_window_minutes: int,
) -> None:
    """
    Painel 5 — Comparativo entre ciclos selecionados (MVP).
    Tabela lado a lado + gráfico de barras do tempo até 7 °C.
    """
    st.header("Painel 5 — Comparativo entre ciclos")
    st.caption(
        "Selecione 2 ou mais ciclos para ver os indicadores lado a lado "
        "e comparar o tempo até 7 °C."
    )

    if not summaries:
        st.warning("Nenhum ciclo disponível para comparação.")
        return

    # Ordena do mais recente para o mais antigo (pela data de FIM)
    summaries_sorted = sorted(summaries, key=lambda s: s.fim_ciclo, reverse=True)

    # Rótulo: "C12 — inicio 15/04 22:10 → fim 16/04 09:45"
    def _label_for(s: CycleSummary) -> str:
        return (
            f"C{s.cycle_id} — inicio {s.inicio_ciclo.strftime('%d/%m %H:%M')} "
            f"→ fim {s.fim_ciclo.strftime('%d/%m %H:%M')}"
        )

    label_to_summary = {_label_for(s): s for s in summaries_sorted}

    # ─── Filtro opcional por data de fim do ciclo ───────────────────────
    min_fim = min(s.fim_ciclo for s in summaries_sorted).date()
    max_fim = max(s.fim_ciclo for s in summaries_sorted).date()

    with st.expander("🔎 Filtrar ciclos por data de término", expanded=False):
        st.caption(
            "Deixe em branco (ou use o intervalo completo) para ver todos. "
            "Útil para focar em ciclos que terminaram em dias específicos."
        )
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filtro_inicio = st.date_input(
                "Término a partir de",
                value=min_fim,
                min_value=min_fim,
                max_value=max_fim,
                key="panel5_filter_start",
                format="DD/MM/YYYY",
            )
        with col_f2:
            filtro_fim = st.date_input(
                "Término até",
                value=max_fim,
                min_value=min_fim,
                max_value=max_fim,
                key="panel5_filter_end",
                format="DD/MM/YYYY",
            )

    # Aplica o filtro à lista de ciclos
    filtered_pairs = [
        (lab, s) for lab, s in label_to_summary.items()
        if filtro_inicio <= s.fim_ciclo.date() <= filtro_fim
    ]
    labels = [lab for lab, _ in filtered_pairs]

    if not labels:
        st.info("Nenhum ciclo termina no intervalo escolhido. Ajuste o filtro acima.")
        return

    st.caption(f"📋 {len(labels)} ciclo(s) no intervalo selecionado.")

    MAX_CYCLES = 4
    default_n = min(3, len(labels))
    selected_labels = st.multiselect(
        f"Ciclos para comparar (máximo {MAX_CYCLES})",
        options=labels,
        default=labels[:default_n],
        key="panel5_selected_cycles",
        help=f"Selecione de 2 a {MAX_CYCLES} ciclos para comparação gerencial.",
    )

    if len(selected_labels) > MAX_CYCLES:
        st.warning(
            f"⚠️ Você selecionou {len(selected_labels)} ciclos. "
            f"Apenas os primeiros {MAX_CYCLES} serão comparados."
        )
        selected_labels = selected_labels[:MAX_CYCLES]

    if len(selected_labels) < 2:
        st.info("Selecione pelo menos 2 ciclos para ativar a comparação.")
        return

    # ─── Calcula indicadores de cada ciclo selecionado ──────────────────
    from gerar_relatorio import calculate_indicators

    per_cycle_rows: list[dict] = []
    per_cycle_indicators: list[tuple[str, object]] = []
    per_cycle_phase_dfs: dict[str, pd.DataFrame] = {}  # "C<id>" -> phase_df

    for label in selected_labels:
        summary = label_to_summary[label]
        cycle_col = f"C{summary.cycle_id}"
        cycle_df = select_cycle_df(df, summary)
        cycle_df = add_derived_columns(cycle_df, rate_window_minutes)
        overall_df = build_overall_metrics_table(
            cycle_df=cycle_df,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )
        phase_df_cycle = build_phase_summary_table(
            cycle_df=cycle_df,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )
        per_cycle_phase_dfs[cycle_col] = phase_df_cycle
        indicators = calculate_indicators(summary, overall_df, tolerance_band)
        per_cycle_indicators.append((label, indicators))

        if indicators is None:
            continue

        per_cycle_rows.append({
            "Ciclo": f"C{summary.cycle_id}",
            "Início": summary.inicio_ciclo.strftime("%d/%m/%Y %H:%M"),
            "Tempo até 7 °C (h)": indicators.tempo_7,
            "Espeto inicial (°C)": indicators.esp_inicial,
            "Espeto final (°C)": indicators.esp_final,
            "Queda do espeto (°C)": indicators.queda_espeto,
            "DT médio resfriamento (°C)": indicators.dt_medio_resf,
            "Umidade média (%)": indicators.umidade_media,
            "Glicol em tolerância (%)": indicators.pct_glicol,
            "Duração resfr. (h)": indicators.dur_resf,
            "Meta 7 °C": "✅" if indicators.meta_ok else "❌",
            "Umidade ideal": "✅" if indicators.umidade_ideal else ("~" if indicators.umidade_aceitavel else "❌"),
            "Glicol ok": "✅" if indicators.glicol_ok else "❌",
        })

    if not per_cycle_rows:
        st.warning("Não foi possível calcular indicadores para os ciclos selecionados.")
        return

    comp_df = pd.DataFrame(per_cycle_rows)

    st.subheader("Indicadores lado a lado")
    # Transpõe para ver ciclos em colunas e indicadores em linhas
    display_df = comp_df.set_index("Ciclo").T
    st.dataframe(display_df, use_container_width=True)

    st.divider()

    # ─── Comparação por fase do ciclo (visão gerencial) ─────────────────
    st.subheader("Comparação por fase do ciclo")
    st.caption(
        "Para cada métrica, as linhas mostram as fases do ciclo e as colunas "
        "os ciclos selecionados. Útil para entender a evolução entre ciclos."
    )

    # Métricas de interesse: (rótulo exibido, coluna na phase_df, casas decimais, sufixo)
    phase_metrics: list[tuple[str, str, int, str]] = [
        ("Duração", "Duração (h)", 2, " h"),
        ("Umidade média", "Umidade média (%)", 1, " %"),
        ("Ventilação média", "Ventilação média (%)", 1, " %"),
        ("DT médio", "DT médio (°C)", 2, " °C"),
        ("Queda do espeto", "Queda espeto (°C)", 2, " °C"),
        ("Temp espeto final", "Temp espeto final (°C)", 1, " °C"),
    ]

    def _build_phase_metric_table(column: str, decimals: int, suffix: str) -> pd.DataFrame:
        rows = []
        for phase_name in PHASE_ORDER:
            row = {"Fase": PHASE_DISPLAY.get(phase_name, phase_name)}
            for cycle_col, phase_df_cycle in per_cycle_phase_dfs.items():
                match = phase_df_cycle[phase_df_cycle["Fase"] == phase_name]
                if match.empty or column not in match.columns:
                    row[cycle_col] = "—"
                else:
                    val = match.iloc[0][column]
                    if pd.isna(val):
                        row[cycle_col] = "—"
                    else:
                        row[cycle_col] = f"{val:.{decimals}f}{suffix}"
            rows.append(row)
        return pd.DataFrame(rows)

    for metric_label, column, decimals, suffix in phase_metrics:
        st.markdown(f"**{metric_label}**")
        table = _build_phase_metric_table(column, decimals, suffix)
        st.dataframe(table, use_container_width=True, hide_index=True)
        st.write("")

    st.divider()

    # ─── Gráfico de barras: tempo até 7 °C ──────────────────────────────
    st.subheader("Tempo até espeto ≤ 7 °C")

    tempos = []
    nomes = []
    for label, ind in per_cycle_indicators:
        if ind is None or ind.tempo_7 is None:
            continue
        summary = label_to_summary[label]
        nomes.append(f"C{summary.cycle_id}")
        tempos.append(ind.tempo_7)

    if not tempos:
        st.info("Nenhum ciclo selecionado tem tempo até 7 °C calculado.")
        return

    fig, ax = plt.subplots(figsize=(10, 4.5))
    bars = ax.bar(nomes, tempos, color=THOMS_COLORS.get("espeto", "#E63946"), edgecolor="white")
    ax.axhline(16, color="red", linestyle="--", linewidth=1.2, label="Limite 16 h")

    for bar, valor in zip(bars, tempos):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.15,
            f"{valor:.2f} h",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    ax.set_ylabel("Tempo até 7 °C (h)")
    ax.set_title("Comparativo — tempo até espeto atingir 7 °C")
    ax.set_ylim(0, max(max(tempos) * 1.15, 17))
    ax.legend(loc="upper right")
    apply_thoms_style(fig, ax)

    st.pyplot(fig, use_container_width=True)

    st.caption(
        "Barras abaixo da linha vermelha (16 h) cumpriram a meta térmica. "
        "Veja mais detalhes no Painel 1 de cada ciclo individual."
    )

    st.divider()

    # ─── Exportar relatório gerencial comparativo ───────────────────────
    st.subheader("Exportar relatório gerencial")
    st.caption(
        "Gera um Excel consolidado com os mesmos números dos relatórios "
        "individuais (coerência garantida por construção). "
        "Uma aba por métrica: indicadores principais, duração, umidade, "
        "ventilação, DT, queda do espeto e espeto final por fase."
    )

    col_path, col_btn = st.columns([3, 1])
    with col_path:
        output_choice = st.radio(
            "Pasta de saída",
            ["Padrão (reports/)", "data/relatorios", "Personalizada"],
            horizontal=True,
            key="panel5_output_choice",
        )
        custom_dir = ""
        if output_choice == "Personalizada":
            custom_dir = st.text_input(
                "Caminho completo da pasta",
                value=st.session_state.get("panel5_custom_dir", ""),
                key="panel5_custom_dir",
                placeholder="reports/minha_saida",
            )
    with col_btn:
        st.write("")
        st.write("")
        exportar = st.button(
            "📄 Gerar Excel comparativo",
            type="primary",
            use_container_width=True,
            key="panel5_btn_export",
        )

    if exportar:
        selected_summaries = [label_to_summary[l] for l in selected_labels]
        output_dir = _resolve_output_dir(output_choice, custom_dir)
        ids = "_".join(f"C{s.cycle_id}" for s in selected_summaries)
        filename = f"Comparativo_{ids}.xlsx"
        output_path = output_dir / filename
        try:
            with st.spinner(f"Gerando {filename}..."):
                from gerar_relatorio import generate_comparative_excel
                final_path = generate_comparative_excel(
                    output_path=output_path,
                    summaries_selected=selected_summaries,
                    df=df,
                    tolerance_band=tolerance_band,
                    rate_window_minutes=rate_window_minutes,
                )
            st.success(f"✅ Relatório salvo em: `{final_path}`")
            final_path = Path(final_path)
            st.download_button(
                label=f"Baixar {final_path.name}",
                data=final_path.read_bytes(),
                file_name=final_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_{final_path.name}_{int(final_path.stat().st_mtime)}",
                use_container_width=True,
            )
        except Exception as e:
            st.error(f"❌ Erro ao gerar comparativo: {e}")


# ============================================================
# APLICAÇÃO PRINCIPAL
# ============================================================

def main() -> None:
    st.title("Câmara de resfriados - análise de ciclos")
    st.write(
        "Aplicação única para análise de ciclos operacionais de carregamento e resfriamento."
    )

    with st.sidebar:
        st.header("Configuração geral")

        folder_path = st.text_input(
            "Pasta com os arquivos de dados",
            value="data",
            placeholder="data",
        )

        # Navegação em 2 níveis: seção → painel
        secao = st.radio(
            "Seção",
            options=["🔧 Operação", "🔍 Ciclo individual", "📊 Comparar ciclos"],
            key="sidebar_secao",
        )

        if secao == "🔧 Operação":
            selected_panel = "Painel 0 - Controle de scripts"
            st.caption("Coleta de dados e geração de relatórios.")
        elif secao == "🔍 Ciclo individual":
            sub = st.radio(
                "Painel",
                options=["Visão geral", "Desempenho térmico"],
                key="sidebar_sub_individual",
            )
            selected_panel = {
                "Visão geral": "Painel 1 - Visão geral do ciclo",
                "Desempenho térmico": "Painel 2 - Desempenho térmico",
            }[sub]
        else:  # Comparar ciclos
            sub = st.radio(
                "Painel",
                options=[
                    "Gerencial (até 4 ciclos)",
                    "Comparação geral",
                    "Normalizado por severidade",
                ],
                key="sidebar_sub_comparar",
            )
            selected_panel = {
                "Gerencial (até 4 ciclos)": "Painel 5 - Comparativo entre ciclos selecionados",
                "Comparação geral": "Painel 3 - Comparação entre ciclos",
                "Normalizado por severidade": "Painel 4 - Normalização por severidade",
            }[sub]

        st.divider()

        st.header("Parâmetros dos painéis")
        tolerance_band = st.number_input(
            "Faixa aceitável do erro do glicol (± °C)",
            min_value=0.1,
            max_value=5.0,
            value=0.5,
            step=0.1,
        )

        rate_window_minutes = st.selectbox(
            "Janela da taxa de resfriamento",
            options=[30, 60],
            index=1,
        )

    if selected_panel == "Painel 0 - Controle de scripts":
        render_panel_0(tolerance_band, rate_window_minutes)
        return

    if not folder_path:
        st.warning("Informe o caminho da pasta na barra lateral para continuar.")
        return

    try:
        df, file_names = load_folder_data(folder_path)
    except Exception as exc:
        st.error(f"Erro ao carregar a pasta: {exc}")
        return

    render_diagnostics(df, file_names)

    df = assign_cycle_ids(df)
    summaries = build_cycle_summaries(df)

    st.write(f"Ciclos válidos detectados: {len(summaries)}")

    if not summaries:
        st.error(
            "Nenhum ciclo válido foi detectado. "
            "Revise os sinais de carregamento e resfriamento."
        )
        with st.expander("Prévia dos dados consolidados"):
            st.dataframe(df.head(50), use_container_width=True)
        return

    selected_cycle_id, selected_summary = render_cycle_selector(summaries)

    cycle_df = select_cycle_df(df, selected_summary)

    if selected_panel == "Painel 1 - Visão geral do ciclo":
        render_panel_1(
            cycle_df=cycle_df,
            selected_summary=selected_summary,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )

    elif selected_panel == "Painel 2 - Desempenho térmico":
        render_panel_2(
            cycle_df=cycle_df,
            selected_summary=selected_summary,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )

    elif selected_panel == "Painel 3 - Comparação entre ciclos":
        render_panel_3(
            df=df,
            summaries=summaries,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )

    elif selected_panel == "Painel 4 - Normalização por severidade":
        render_panel_4(
            df=df,
            summaries=summaries,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )

    elif selected_panel == "Painel 5 - Comparativo entre ciclos selecionados":
        render_panel_5(
            df=df,
            summaries=summaries,
            tolerance_band=tolerance_band,
            rate_window_minutes=rate_window_minutes,
        )


if __name__ == "__main__":
    main()
