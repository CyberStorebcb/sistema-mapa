"""Funções utilitárias relacionadas a equipes."""
from __future__ import annotations

from typing import Iterable, List, Sequence


def normalizar_codigo_equipe(equipe: str | None) -> str:
    if not equipe:
        return ''
    codigo = str(equipe).strip().upper().replace(' ', '')
    if 'MA-STI-000' in codigo:
        codigo = codigo.replace('MA-STI-000', 'MA-STI-O00')
    if codigo.startswith('MA-STI-0'):
        codigo = codigo.replace('0M', 'OM', 1)
    partes = codigo.split('-')
    if len(partes) >= 3:
        prefixo = '-'.join(partes[:2])
        sufixo = '-'.join(partes[2:])
        if sufixo and sufixo[0] == '0':
            sufixo = 'O' + sufixo[1:]
        codigo = f"{prefixo}-{sufixo}"
    return codigo

def filtrar_registros_por_equipes(registros: Iterable[dict], equipes_permitidas: Sequence[str]) -> List[dict]:
    equipes_normalizadas = {normalizar_codigo_equipe(eq): normalizar_codigo_equipe(eq) for eq in equipes_permitidas}
    saida: List[dict] = []
    for registro in registros:
        equipe = normalizar_codigo_equipe(registro.get('equipe'))
        if equipe in equipes_normalizadas:
            registro['equipe'] = equipe
            saida.append(registro)
    return saida
