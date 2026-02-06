"""Serviços de cache e histórico em JSON."""
from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from typing import Iterable, List, Sequence

Record = dict

DEFAULT_KEY_FIELDS: Sequence[str] = (
    'data', 'equipe', 'pep', 'nota', 'local', 'periodo'
)

def _read_list(path: str) -> List[Record]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, 'r', encoding='utf-8') as handler:
            data = json.load(handler)
            return data if isinstance(data, list) else []
    except Exception as exc:
        print(f'[AVISO] Falha ao ler {path}: {exc}')
        return []

def _write_list(path: str, registros: Iterable[Record]) -> None:
    try:
        with open(path, 'w', encoding='utf-8') as handler:
            json.dump(list(registros), handler, ensure_ascii=False)
    except Exception as exc:
        print(f'[AVISO] Falha ao salvar {path}: {exc}')

def load_cache(path: str) -> List[Record]:
    return _read_list(path)

def save_cache(path: str, registros: Iterable[Record]) -> None:
    _write_list(path, registros)

def load_history(path: str) -> List[Record]:
    return _read_list(path)

def save_history(path: str, registros: Iterable[Record]) -> None:
    _write_list(path, registros)

def partition_records_by_date(registros: Iterable[Record], dias_historico: int = 7) -> tuple[List[Record], List[Record]]:
    limite = datetime.now().date() - timedelta(days=dias_historico)
    historico, recentes = [], []
    for registro in registros:
        data_str = str(registro.get('data', '')).strip()
        try:
            data_registro = datetime.strptime(data_str, '%d/%m/%Y').date()
        except Exception:
            recentes.append(registro)
            continue
        if data_registro < limite:
            historico.append(registro)
        else:
            recentes.append(registro)
    return historico, recentes

def deduplicate_records(registros: Iterable[Record], key_fields: Sequence[str] = DEFAULT_KEY_FIELDS) -> List[Record]:
    vistos = set()
    saida: List[Record] = []
    for registro in registros:
        chave = tuple(registro.get(c) for c in key_fields)
        if chave in vistos:
            continue
        vistos.add(chave)
        saida.append(registro)
    return saida

def update_memory_and_persist(
    registros_filtrados: Iterable[Record],
    cache_path: str,
    history_path: str,
    dias_historico: int = 7
) -> List[Record]:
    historico_existente = deduplicate_records(load_history(history_path))
    novos_historicos, recentes = partition_records_by_date(registros_filtrados, dias_historico)
    historico_atualizado = deduplicate_records(historico_existente + novos_historicos)
    recentes_deduplicados = deduplicate_records(recentes)
    save_history(history_path, historico_atualizado)
    save_cache(cache_path, recentes_deduplicados)
    return historico_atualizado + recentes_deduplicados
