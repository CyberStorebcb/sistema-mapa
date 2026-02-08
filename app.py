import os
import unicodedata
import warnings
from collections import defaultdict
from datetime import datetime
from typing import List

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

from services.cache import (
    deduplicate_records,
    load_cache,
    load_history,
    save_cache,
    save_history,
    update_memory_and_persist,
)
from services.dropbox_client import DropboxSettings, TokenCache, iter_excel_files
from services.equipes import filtrar_registros_por_equipes, normalizar_codigo_equipe
from services.excel_loader import carregar_registros_do_arquivo
from utils.dates import filtrar_por_mes_e_semana, gerar_intervalo_datas, obter_mes_semana_atual

warnings.filterwarnings(
    'ignore',
    message='Data Validation extension is not supported and will be removed',
    category=UserWarning,
    module='openpyxl'
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'supersecretkey-mapa-2024')
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

CACHE_FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'programacao_cache.json')
HISTORY_FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'programacao_historico.json')

ALLOWED_EQUIPES: List[str] = [
    'MA-BCB-O001M', 'MA-BCB-O002M', 'MA-BCB-O003M', 'MA-BCB-O004M',
    'MA-BCB-O005M', 'MA-BCB-O006M', 'MA-BCB-T001M', 'MA-ITM-O001M',
    'MA-ITM-O002M', 'MA-ITM-O003M', 'MA-ITM-O004M', 'MA-STI-T001M',
    'MA-STI-O001M', 'MA-STI-O002M', 'MA-STI-O003M', 'MA-STI-O004M'
]

BASE_PREFIXES = {
    'BCB': 'MA-BCB',
    'ITM': 'MA-ITM',
    'STI': 'MA-STI'
}
BASE_OPTIONS = list(BASE_PREFIXES.keys())

MESES_PT = [
    'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
]

DROPBOX_SETTINGS = DropboxSettings(
    folder_path=os.environ.get('DROPBOX_FOLDER_PATH', '/Programação Semanal Equipes/Programação Semanal 2026'),
    files={
        'BCB': 'PROGRAMAÇÃO SEMANAL BCB.xlsm',
        'ITM': 'PROGRAMAÇÃO SEMANAL ITM.xlsm',
        'STI': 'PROGRAMAÇÃO SEMANAL STI.xlsm'
    },
    access_token=os.environ.get('DROPBOX_ACCESS_TOKEN'),
    refresh_token=os.environ.get('DROPBOX_REFRESH_TOKEN'),
    app_key=os.environ.get('DROPBOX_APP_KEY'),
    app_secret=os.environ.get('DROPBOX_APP_SECRET')
)
DROPBOX_TOKEN_CACHE = TokenCache()

db_projetos: List[dict] = []

cache_inicial = load_cache(CACHE_FILE_PATH)
historico_inicial = load_history(HISTORY_FILE_PATH)
if cache_inicial or historico_inicial:
    registros_iniciais = filtrar_registros_por_equipes(historico_inicial + cache_inicial, ALLOWED_EQUIPES)
    db_projetos = deduplicate_records(registros_iniciais)


def normalizar_texto(texto: str | None) -> str:
    if not texto:
        return ''
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    ).upper().strip()


def identificar_base_por_equipe(equipe: str | None) -> str:
    codigo = normalizar_codigo_equipe(equipe)
    for base, prefixo in BASE_PREFIXES.items():
        if prefixo in codigo:
            return base
    return ''


def status_programado(status: str | None) -> bool:
    return normalizar_texto(status).startswith('PROGRAMAD')


def sincronizar_programacao_dropbox():
    global db_projetos
    registros_total = []
    erros = []

    for chave, conteudo in iter_excel_files(DROPBOX_SETTINGS, DROPBOX_TOKEN_CACHE):
        try:
            conteudo.seek(0)
            registros_total.extend(carregar_registros_do_arquivo(conteudo))
        except Exception as exc:
            erros.append(f"{chave}: {exc}")

    registros_filtrados = filtrar_registros_por_equipes(registros_total, ALLOWED_EQUIPES)
    if registros_filtrados:
        db_projetos = update_memory_and_persist(registros_filtrados, CACHE_FILE_PATH, HISTORY_FILE_PATH)
        mensagem = f"Atualização concluída! {len(db_projetos)} registros sincronizados."
        sucesso = True
    else:
        mensagem = 'Nenhum registro das equipes selecionadas foi sincronizado.'
        sucesso = False

    if erros:
        print('[AVISO] Ocorreram erros ao sincronizar com o Dropbox:', erros)
        mensagem += ' ' + '; '.join(erros)

    return {
        'sucesso': sucesso,
        'mensagem': mensagem,
        'erros': erros,
        'registros': db_projetos
    }


@app.route('/')
def inicio():
    return render_template('inicio.html')


@app.route('/programacao_geral')
def programacao_geral():
    exibicao = db_projetos if db_projetos else []
    return render_template('programacao_geral.html', projetos=exibicao)


@app.route('/importar_excel', methods=['POST'])
def importar_excel():
    global db_projetos
    if 'file' not in request.files:
        flash('Nenhum arquivo enviado')
        return redirect(url_for('programacao_geral'))

    file = request.files['file']
    if file.filename == '' or not file.filename.endswith(('.xlsx', '.xls')):
        flash('Selecione um arquivo Excel válido (.xlsx)')
        return redirect(url_for('programacao_geral'))

    try:
        file.stream.seek(0)
        registros = carregar_registros_do_arquivo(file)
        registros_filtrados = filtrar_registros_por_equipes(registros, ALLOWED_EQUIPES)
        if not registros_filtrados:
            raise ValueError('Nenhuma das equipes permitidas foi encontrada no arquivo Excel enviado.')
        db_projetos = update_memory_and_persist(registros_filtrados, CACHE_FILE_PATH, HISTORY_FILE_PATH)
        flash(f'Sucesso! {len(db_projetos)} registros importados das equipes selecionadas.')
    except ValueError as ve:
        flash(str(ve))
        db_projetos = []
    except Exception as exc:
        import traceback
        print('[ERRO] Falha ao importar Excel:', exc)
        traceback.print_exc()
        flash(f'Erro ao processar: {exc}')
    return redirect(url_for('programacao_geral'))


@app.route('/atualizar_programacao', methods=['POST'])
def atualizar_programacao():
    resultado = sincronizar_programacao_dropbox()
    flash(resultado['mensagem'])
    return redirect(url_for('programacao_geral'))


def _equipes_ordenadas(projetos: List[dict]) -> List[str]:
    presentes = {p.get('equipe') for p in projetos if p.get('equipe') not in ('-', None)}
    ordenadas = [eq for eq in ALLOWED_EQUIPES if eq in presentes]
    extras = sorted(presentes - set(ALLOWED_EQUIPES))
    ordenadas.extend(extras)
    return ordenadas


def _datas_colunas(datas_exibicao: List[str]):
    dias_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    resposta = []
    for data_str in datas_exibicao:
        try:
            dt = datetime.strptime(data_str, '%d/%m/%Y')
            resposta.append({
                'original': data_str,
                'exibicao': f"{dt.strftime('%d/%m')} - {dias_semana[dt.weekday()]}",
                'dia_num': dt.weekday()
            })
        except Exception:
            continue
    return resposta


@app.route('/mapa')
def mapa():
    base_selecionada = request.args.get('base', '')
    mes_sel = request.args.get('mes', '')
    semana_sel = request.args.get('semana', '')
    base_norm = normalizar_texto(base_selecionada)

    prefixos = {'BACABAL': 'BCB', 'ITAPECURU': 'ITM', 'SANTA INES': 'STI'}
    prefixo_alvo = ''
    for nome, pref in prefixos.items():
        if nome in base_norm:
            prefixo_alvo = pref

    projetos_base = []
    for projeto in db_projetos:
        equipe = normalizar_codigo_equipe(projeto.get('equipe'))
        if not base_norm or (prefixo_alvo and prefixo_alvo in equipe):
            projeto['equipe'] = equipe
            projeto['data'] = str(projeto.get('data', '')).strip()
            projetos_base.append(projeto)

    projetos_filtrados = filtrar_por_mes_e_semana(projetos_base, mes_sel, semana_sel)
    datas_exibicao = gerar_intervalo_datas(projetos_filtrados, base_norm)
    equipes_finais = _equipes_ordenadas([p for p in projetos_filtrados if p.get('data') in datas_exibicao])

    return render_template(
        'mapa.html',
        projetos=projetos_filtrados,
        equipes=equipes_finais,
        datas_colunas=_datas_colunas(datas_exibicao),
        base_ativa=base_selecionada,
        mes_sel=mes_sel,
        semana_sel=semana_sel
    )


@app.route('/semanal')
def semanal():
    mes_sel = request.args.get('mes', '')
    semana_sel = request.args.get('semana', '')
    projetos_filtrados = filtrar_por_mes_e_semana(db_projetos, mes_sel, semana_sel)
    datas_exibicao = gerar_intervalo_datas(projetos_filtrados)
    equipes_finais = _equipes_ordenadas(projetos_filtrados)

    return render_template(
        'mapa.html',
        base_ativa='Semanal',
        projetos=projetos_filtrados,
        equipes=equipes_finais,
        datas_colunas=_datas_colunas(datas_exibicao),
        mes_sel=mes_sel,
        semana_sel=semana_sel
    )


def _projetos_semana_atual():
    mes_sel, semana_sel = obter_mes_semana_atual()
    projetos_semana = filtrar_por_mes_e_semana(db_projetos, mes_sel, semana_sel)
    return projetos_semana, mes_sel, semana_sel


@app.route('/localizacao_atual')
def localizacao_atual():
    projetos_semana, mes_sel, semana_sel = _projetos_semana_atual()
    agrupados = defaultdict(list)
    for projeto in projetos_semana:
        equipe = normalizar_codigo_equipe(projeto.get('equipe'))
        projeto['equipe'] = equipe
        agrupados[equipe].append(projeto)

    cards = []
    for equipe in ALLOWED_EQUIPES:
        if equipe not in agrupados:
            continue
        def _ordena(proj):
            try:
                return datetime.strptime(str(proj.get('data')), '%d/%m/%Y')
            except Exception:
                return datetime.max
        registros = sorted(agrupados[equipe], key=_ordena)
        cards.append({
            'equipe': equipe,
            'projetos': registros,
            'local_principal': registros[-1].get('local', '-') if registros else '-'
        })

    return render_template(
        'localizacao.html',
        cards=cards,
        semana_label=f"Semana {semana_sel}",
        mes_label=MESES_PT[int(mes_sel) - 1] if mes_sel.isdigit() else mes_sel,
        total=len(projetos_semana)
    )


@app.route('/localizacao_mapa')
def localizacao_mapa():
    _, mes_sel, semana_sel = _projetos_semana_atual()
    return render_template(
        'localizacao_mapa.html',
        semana_label=f"Semana {semana_sel}",
        mes_label=MESES_PT[int(mes_sel) - 1] if mes_sel.isdigit() else mes_sel,
        bases=BASE_OPTIONS,
        equipes=ALLOWED_EQUIPES
    )


@app.route('/api/localizacoes_atual')
def api_localizacoes_atual():
    projetos_semana, _, _ = _projetos_semana_atual()
    base_filter = request.args.get('base', '').strip().upper()
    if base_filter and base_filter not in BASE_PREFIXES:
        base_filter = ''
    equipe_param = request.args.get('equipe', '').strip()
    equipe_filter = normalizar_codigo_equipe(equipe_param) if equipe_param else ''
    agrupados = defaultdict(list)
    for projeto in projetos_semana:
        if not status_programado(projeto.get('status')):
            continue
        equipe = normalizar_codigo_equipe(projeto.get('equipe'))
        if not equipe:
            continue
        if equipe_filter and equipe != equipe_filter:
            continue
        base_atual = identificar_base_por_equipe(equipe)
        if base_filter and base_atual != base_filter:
            continue
        local = (projeto.get('local') or '-').strip()
        if not local or local == '-':
            continue
        agrupados[local].append({
            'equipe': equipe,
            'data': projeto.get('data'),
            'status': projeto.get('status'),
            'periodo': projeto.get('periodo'),
            'base': base_atual
        })
    payload = [
        {
            'local': local,
            'projetos': registros
        }
        for local, registros in agrupados.items()
    ]
    return jsonify(payload)


@app.route('/limpar_dados')
def limpar_dados():
    global db_projetos
    db_projetos = []
    save_cache(CACHE_FILE_PATH, [])
    save_history(HISTORY_FILE_PATH, [])
    flash('A tabela foi limpa com sucesso!')
    return redirect(url_for('programacao_geral'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
