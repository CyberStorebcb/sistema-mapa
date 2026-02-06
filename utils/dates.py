"""Funções utilitárias relacionadas a datas e semanas personalizadas."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, List

Projeto = dict


def semana_customizada(dt: datetime) -> int:
    y, m, d = dt.year, dt.month, dt.day
    if y == 2026:
        if m == 1 and d >= 26:
            return 1
        if m == 2:
            if d == 1:
                return 1
            if 2 <= d <= 7:
                return 2
            if 8 <= d <= 14:
                return 3
            if 15 <= d <= 21:
                return 4
            if 22 <= d <= 28:
                return 5
            return 6
        if m == 3:
            if 1 <= d <= 7:
                return 1
            if 8 <= d <= 14:
                return 2
            if 15 <= d <= 21:
                return 3
            if 22 <= d <= 28:
                return 4
            return 5
        if m == 4:
            if 1 <= d <= 4:
                return 1
            if 5 <= d <= 11:
                return 2
            if 12 <= d <= 18:
                return 3
            if 19 <= d <= 25:
                return 4
            return 5
    return ((d - 1) // 7) + 1

def filtrar_por_mes_e_semana(
    projetos: Iterable[Projeto],
    mes_sel: str,
    semana_sel: str
) -> List[Projeto]:
    resultado: List[Projeto] = []
    for projeto in projetos:
        data_str = str(projeto.get('data', '')).strip()
        try:
            data_dt = datetime.strptime(data_str, '%d/%m/%Y')
        except Exception:
            continue
        if mes_sel == '02' and semana_sel == '1':
            if (data_dt.month == 2 and semana_customizada(data_dt) == 1) or (data_dt.month == 1 and data_dt.day >= 26):
                resultado.append(projeto)
            continue
        if mes_sel and data_dt.strftime('%m') != mes_sel:
            continue
        if semana_sel:
            if str(semana_customizada(data_dt)) != semana_sel:
                continue
        resultado.append(projeto)
    return resultado

def gerar_intervalo_datas(projetos: Iterable[Projeto], base_norm: str = '') -> List[str]:
    datas = [p.get('data') for p in projetos if p.get('data') not in (None, '-', '')]
    if not datas:
        return []
    try:
        dt_objs = [datetime.strptime(d, '%d/%m/%Y') for d in datas]
    except Exception:
        return []
    data_inicio = min(dt_objs)
    data_fim = data_inicio + timedelta(days=2) if base_norm else max(dt_objs)
    intervalo = []
    atual = data_inicio
    while atual <= data_fim:
        intervalo.append(atual.strftime('%d/%m/%Y'))
        atual += timedelta(days=1)
    return intervalo
