"""Funções para leitura e normalização das planilhas Excel/Dropbox."""
from __future__ import annotations

from io import BytesIO
from typing import Dict, Iterable, List

import pandas as pd

COLUMN_MAP = {
    'ID': 'id',
    'DATA': 'data',
    'PERÍODO': 'periodo',
    'PERIODO': 'periodo',
    'TIPO': 'tipo',
    'EQUIPE': 'equipe',
    'ENCARREGADO': 'encarregado',
    'SUPERVISOR': 'supervisor',
    'COM LV': 'com_lv',
    'SI/NR': 'si_inc',
    'SI/INC': 'si_inc',
    'PEP': 'pep',
    'NOTA': 'nota',
    'LOCAL': 'local',
    'STATUS': 'status',
    'CONDIÇÃO': 'condicao',
    'CONDICAO': 'condicao',
    'OBSERVAÇÃO': 'obs',
    'OBSERVACAO': 'obs'
}

COLUNAS_VALIDAS = tuple(COLUMN_MAP.values())

SENTINEL_HEADER_MARKERS = ('BASE', 'PLANILHA', 'AUX', 'R$ PROGRAMACAO')


def _normalize_header(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def ajustar_cabecalho_excel(df: pd.DataFrame) -> pd.DataFrame:
    df_tmp = _normalize_header(df)
    if 'DATA' in df_tmp.columns:
        return df_tmp

    limite = min(30, len(df_tmp))
    for idx in range(limite):
        linha = df_tmp.iloc[idx]
        linha_norm = [str(v).strip().upper() if pd.notna(v) else '' for v in linha]
        if 'DATA' in linha_norm and 'EQUIPE' in linha_norm:
            novo = df_tmp.iloc[idx + 1:].reset_index(drop=True)
            novo.columns = linha_norm
            return novo
        if any(marker in linha_norm for marker in SENTINEL_HEADER_MARKERS):
            aux_idx = idx + 1
            if aux_idx < len(df_tmp):
                aux_linha = df_tmp.iloc[aux_idx]
                aux_norm = [str(v).strip().upper() if pd.notna(v) else '' for v in aux_linha]
                if 'DATA' in aux_norm and 'EQUIPE' in aux_norm:
                    novo = df_tmp.iloc[aux_idx + 1:].reset_index(drop=True)
                    novo.columns = aux_norm
                    return novo

    df_col = df_tmp.dropna(axis=1, how='all').reset_index(drop=True)
    for col in list(df_col.columns):
        serie = df_col[col].astype(str).str.upper()
        if 'DATA' in serie.values:
            df_col = df_col.rename(columns={col: 'DATA'})
        if 'EQUIPE' in serie.values:
            df_col = df_col.rename(columns={col: 'EQUIPE'})
    return df_col

def carregar_registros_do_dataframe(df: pd.DataFrame) -> List[Dict]:
    df = ajustar_cabecalho_excel(df)
    df = df.dropna(axis=1, how='all')
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()]

    if 'data' not in df.columns:
        raise ValueError("Coluna 'DATA' não encontrada no arquivo Excel.")

    df = df.reset_index(drop=True)
    if 'id' not in df.columns:
        df.insert(0, 'id', df.index + 1)

    df['data'] = pd.to_datetime(df['data'], errors='coerce')
    df = df.dropna(subset=['data'])
    df['data'] = df['data'].dt.strftime('%d/%m/%Y')
    df = df.fillna('-')

    colunas_disponiveis = [c for c in COLUNAS_VALIDAS if c in df.columns]
    registros = df[colunas_disponiveis].to_dict(orient='records')
    if not registros:
        raise ValueError('Nenhum registro válido encontrado após o processamento do Excel.')
    return registros

def carregar_registros_do_arquivo(excel_buffer: BytesIO | str) -> List[Dict]:
    registros: List[Dict] = []
    planilhas = pd.read_excel(excel_buffer, sheet_name=None, header=None)
    for nome, df in planilhas.items():
        if df.empty:
            continue
        try:
            registros.extend(carregar_registros_do_dataframe(df))
        except ValueError as ve:
            print(f"[AVISO] Aba '{nome}' ignorada: {ve}")
        except Exception as exc:
            print(f"[ERRO] Aba '{nome}' ignorada: {exc}")
    if not registros:
        raise ValueError('Colunas obrigatórias não foram encontradas em nenhuma aba do arquivo Excel.')
    return registros
