from io import BytesIO

import pandas as pd

from services.excel_loader import carregar_concluidas_do_arquivo


def test_carregar_concluidas_do_arquivo_minimo():
    df = pd.DataFrame(
        {
            'BASE': ['BCB'],
            'OBRA': ['MA-123'],
            'STATUS': ['LIB/ATEC'],
            'VALOR': [1000],
            'AND': [500],
            'INIC': ['01/02/2026'],
            'CONC': ['05/02/2026'],
        }
    )
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='CONCLUÍDAS', startrow=0)
    buffer.seek(0)

    registros = carregar_concluidas_do_arquivo(buffer)

    assert len(registros) == 1
    registro = registros[0]
    assert registro['base'] == 'BCB'
    assert registro['obra'] == 'MA-123'
    assert registro['status'] == 'LIB/ATEC'
    assert registro['valor'] == 1000
    assert registro['andamento'] == 500
