from app import _metricas_concluidas


def test_metricas_concluidas_soma_valores_e_faltantes():
    obras = [
        {
            'base': 'BCB',
            'status': 'LIB/ATEC',
            'valor': '1.000,00',
            'andamento': '200,00',
            'inic': '01/02/2026',
            'conc': '05/02/2026',
            'obra': 'MA-001',
        },
        {
            'base': 'ITM',
            'status': 'SEM PEP',
            'valor': '-',
            'andamento': 0,
            'inic': '02/02/2026',
            'conc': '06/02/2026',
            'obra': 'MA-002',
        },
    ]

    metricas = _metricas_concluidas(obras)

    assert metricas['total'] == 2
    assert metricas['total_valor'] == 1200.0
    assert metricas['total_andamento'] == 200.0
    assert metricas['bases_valor'][0]['nome'] == 'BCB'
    assert metricas['faltantes']
    assert metricas['faltantes'][0]['obra'] == 'MA-002'
