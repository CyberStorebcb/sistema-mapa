import csv
import os
from io import StringIO, BytesIO
from urllib.parse import urlencode

from dotenv import load_dotenv
import unicodedata
import warnings
from collections import Counter, defaultdict
from datetime import datetime
from typing import List

import requests
from flask import Flask, Response, flash, jsonify, redirect, render_template, request, url_for
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from services.cache import (
    deduplicate_records,
    load_cache,
    load_history,
    save_cache,
    save_history,
    update_memory_and_persist,
)
from services.dropbox_client import (
    DropboxSettings,
    TokenCache,
    download_file,
    get_access_token,
)
from services.equipes import filtrar_registros_por_equipes, normalizar_codigo_equipe
from services.excel_loader import carregar_concluidas_do_arquivo, carregar_registros_do_arquivo
from utils.dates import filtrar_por_mes_e_semana, gerar_intervalo_datas, obter_mes_semana_atual

warnings.filterwarnings(
    'ignore',
    message='Data Validation extension is not supported and will be removed',
    category=UserWarning,
    module='openpyxl'
)

load_dotenv()
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'supersecretkey-mapa-2024')
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


@app.template_filter('brl')
def format_currency_brl(valor: float | int | str | None) -> str:
    numero = _parse_decimal(valor)
    return f"R$ {numero:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


@app.template_filter('data_curta')
def format_date_short(valor: str | datetime | None) -> str:
    return formatar_data_curta(valor)

CACHE_FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'programacao_cache.json')
HISTORY_FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'programacao_historico.json')
CONCLUIDAS_FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'concluidas_cache.json')
PENDENTES_WEBHOOK_URL = os.environ.get('PENDENTES_WEBHOOK_URL', '').strip()

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

CRITICAL_STATUSES = {'SEM PEP', 'ABER/LOG', 'SEM STATUS'}


def _parse_decimal(valor: float | int | str | None) -> float:
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip()
    if not texto or texto == '-':
        return 0.0
    texto = texto.replace('R$', '').replace(' ', '').replace('\xa0', '')
    if ',' in texto:
        texto = texto.replace('.', '').replace(',', '.')
    else:
        texto = texto.replace(',', '.')
    try:
        return float(texto)
    except ValueError:
        numeros = ''.join(ch for ch in texto if ch.isdigit() or ch == '.')
        try:
            return float(numeros)
        except ValueError:
            return 0.0


def _pendencia_do_registro(obra: dict, valor_atual: float, andamento_atual: float) -> dict | None:
    motivos: List[str] = []
    if valor_atual <= 0:
        motivos.append('Valor')
    if andamento_atual <= 0:
        motivos.append('AND')
    if not motivos:
        return None
    return {
        'base': str(obra.get('base') or '-').strip() or '-',
        'obra': str(obra.get('obra') or '-').strip() or '-',
        'motivo': ', '.join(motivos)
    }


def _extrair_data_texto(valor: str | datetime | None) -> str:
    if isinstance(valor, datetime):
        return valor.strftime('%d/%m/%Y')
    if valor is None:
        return ''
    texto = str(valor).strip()
    if not texto or texto == '-':
        return ''
    texto = texto.replace('T', ' ')
    base = texto.split(' ')[0]
    return base


def _parse_data_generica(valor: str | datetime | None) -> datetime | None:
    base = _extrair_data_texto(valor)
    if not base:
        return None
    formatos = ('%d/%m/%Y', '%Y-%m-%d')
    for formato in formatos:
        try:
            return datetime.strptime(base, formato)
        except ValueError:
            continue
    return None


def formatar_data_curta(valor: str | datetime | None) -> str:
    data = _parse_data_generica(valor)
    if data:
        return data.strftime('%d/%m/%Y')
    texto = _extrair_data_texto(valor)
    return texto if texto else '-'


def _obter_cache_timestamp(path: str) -> datetime | None:
    if os.path.exists(path):
        return datetime.fromtimestamp(os.path.getmtime(path))
    return None


def _formata_timestamp_legivel(data: datetime | None) -> str:
    if not data:
        return ''
    return data.strftime('%d/%m/%Y %H:%M')


def _listar_pendencias(registros: List[dict]) -> List[dict]:
    pendentes: List[dict] = []
    for obra in registros:
        pendencia = _pendencia_do_registro(
            obra,
            _parse_decimal(obra.get('valor')),
            _parse_decimal(obra.get('andamento'))
        )
        if pendencia:
            pendentes.append(pendencia)
    return pendentes


def _contar_pendencias_globais() -> int:
    return len(_listar_pendencias(db_concluidas or []))


def _normalize_dropbox_path(path: str | None) -> str | None:
    if not path:
        return None
    valor = path.strip()
    if not valor:
        return None
    return valor if valor.startswith('/') else f'/{valor}'


DEFAULT_CONTROLE_PATH = '/Controle - Obras.xlsx'

controle_path_env = _normalize_dropbox_path(os.environ.get('DROPBOX_CONTROLE_PATH'))
controle_path = controle_path_env or DEFAULT_CONTROLE_PATH

DROPBOX_SETTINGS = DropboxSettings(
    controle_path=controle_path,
    access_token=os.environ.get('DROPBOX_ACCESS_TOKEN'),
    refresh_token=os.environ.get('DROPBOX_REFRESH_TOKEN'),
    app_key=os.environ.get('DROPBOX_APP_KEY'),
    app_secret=os.environ.get('DROPBOX_APP_SECRET')
)
DROPBOX_TOKEN_CACHE = TokenCache()

db_projetos: List[dict] = []
db_concluidas: List[dict] = []

cache_inicial = load_cache(CACHE_FILE_PATH)
historico_inicial = load_history(HISTORY_FILE_PATH)
if cache_inicial or historico_inicial:
    registros_iniciais = filtrar_registros_por_equipes(historico_inicial + cache_inicial, ALLOWED_EQUIPES)
    db_projetos = deduplicate_records(registros_iniciais)

concluidas_inicial = load_cache(CONCLUIDAS_FILE_PATH)
if concluidas_inicial:
    db_concluidas = concluidas_inicial


def normalizar_texto(texto: str | None) -> str:
    if not texto:
        return ''
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    ).upper().strip()

def _definir_condicoes_basicas(registros: List[dict]) -> None:
    for registro in registros:
        condicao = str(registro.get('condicao') or registro.get('status') or '').strip()
        registro['condicao'] = condicao if condicao else '-'


def _parse_data_segura(data_str: str) -> datetime | None:
    return _parse_data_generica(data_str)


def _agrupar_status_criticos(registros: List[dict]) -> List[dict]:
    agregados: dict[str, dict] = {}
    for registro in registros:
        condicao = str(registro.get('condicao') or '-').strip().upper()
        if condicao not in CRITICAL_STATUSES:
            continue
        pep = str(registro.get('pep') or '').strip()
        nota = str(registro.get('nota') or '').strip()
        chave = pep if pep and pep != '-' else nota
        if not chave:
            chave = f"REGISTRO-{registro.get('id', len(agregados) + 1)}"
        data_str = str(registro.get('data') or '').strip()
        data_obj = _parse_data_segura(data_str) or datetime.max
        existente = agregados.get(chave)
        if not existente or data_obj < existente['data_ord']:
            agregados[chave] = {
                'pep': pep or '-',
                'nota': nota or '-',
                'data': data_str or '-',
                'data_ord': data_obj,
                'condicao': condicao,
                'local': registro.get('local') or '-',
                'equipe': registro.get('equipe') or '-'
            }
    ordenados = sorted(agregados.values(), key=lambda item: item['data_ord'])
    for item in ordenados:
        item.pop('data_ord', None)
    return ordenados


def _semana_str_to_int(valor: str | None) -> int | None:
    if not valor:
        return None
    digitos = ''.join(ch for ch in str(valor) if ch.isdigit())
    return int(digitos) if digitos else None


def _obras_concluidas_por_mes(mes_sel: str) -> List[dict]:
    registros = db_concluidas or []
    concluidas: List[dict] = []
    for linha in registros:
        data_ref = linha.get('conc') or linha.get('inic')
        if mes_sel:
            data_dt = _parse_data_generica(data_ref)
            if not data_dt:
                continue
            if data_dt.strftime('%m') != mes_sel:
                continue
        concluidas.append(linha)

    return concluidas


def _coletar_filtros(args) -> dict:
    return {
        'base': (args.get('base') or '').strip(),
        'status': (args.get('status') or '').strip(),
        'inicio': (args.get('inicio') or '').strip(),
        'fim': (args.get('fim') or '').strip(),
        'semana_inicio': (args.get('semana_inicio') or '').strip(),
        'semana_fim': (args.get('semana_fim') or '').strip()
    }


def _filtrar_obras_por_filtros(obras: List[dict], filtros: dict) -> List[dict]:
    base_sel = filtros.get('base', '').upper()
    status_sel = filtros.get('status', '').upper()
    semana_inicio_sel = filtros.get('semana_inicio', '').strip()
    semana_fim_sel = filtros.get('semana_fim', '').strip()
    data_inicio = _parse_data_generica(filtros.get('inicio'))
    data_fim = _parse_data_generica(filtros.get('fim'))
    filtradas: List[dict] = []

    for obra in obras:
        base_atual = (obra.get('base') or '').strip()
        if base_sel and base_atual.upper() != base_sel:
            continue

        status_atual = (obra.get('status') or '').strip().upper()
        if status_sel and status_atual != status_sel:
            continue

        if semana_inicio_sel or semana_fim_sel:
            semana_inicio_num = _semana_str_to_int(obra.get('inic_sem'))
            semana_fim_num = _semana_str_to_int(obra.get('conc_sem'))
            filtro_inicio_num = _semana_str_to_int(semana_inicio_sel)
            filtro_fim_num = _semana_str_to_int(semana_fim_sel)

            if filtro_inicio_num and (not semana_inicio_num or semana_inicio_num != filtro_inicio_num):
                continue
            if filtro_fim_num and (not semana_fim_num or semana_fim_num != filtro_fim_num):
                continue

        data_comparacao = _parse_data_generica(obra.get('conc')) or _parse_data_generica(obra.get('inic'))
        if data_inicio and (not data_comparacao or data_comparacao < data_inicio):
            continue
        if data_fim and (not data_comparacao or data_comparacao > data_fim):
            continue

        filtradas.append(obra)

    return filtradas


def _gerar_pdf_concluidas(obras: List[dict], metricas: dict) -> bytes:
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=48, bottomMargin=36)
    styles = getSampleStyleSheet()
    elementos = [Paragraph('Relatório - Obras Concluídas', styles['Title']), Spacer(1, 12)]

    resumo_dados = [
        ['Total', metricas.get('total', 0)],
        ['Valor Total', format_currency_brl(metricas.get('total_valor', 0))],
        ['Valor em Andamento', format_currency_brl(metricas.get('total_andamento', 0))],
        ['Base Destaque', f"{metricas.get('base_top', ('-', 0))[0]} ({metricas.get('base_top', ('-', 0))[1]})"],
    ]
    tabela_resumo = Table(resumo_dados, hAlign='LEFT')
    tabela_resumo.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3c72')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold')
    ]))
    elementos.extend([tabela_resumo, Spacer(1, 18)])

    cabecalho = ['Base', 'Obra', 'Status', 'Qtd', 'Início', 'Conclusão', 'Início Sem', 'Conclusão Sem', 'Prog', 'AND', 'Valor', 'Vizita']
    dados_tabela = [cabecalho]
    for obra in obras:
        dados_tabela.append([
            obra.get('base', ''),
            obra.get('obra', ''),
            obra.get('status', ''),
            obra.get('qtd_prog', ''),
            obra.get('inic', ''),
            obra.get('conc', ''),
            obra.get('inic_sem', ''),
            obra.get('conc_sem', ''),
            obra.get('prog', ''),
            format_currency_brl(obra.get('andamento')),
            format_currency_brl(obra.get('valor')),
            obra.get('vizita', '')
        ])

    tabela = Table(dados_tabela, repeatRows=1)
    tabela.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2a5298')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey)
    ]))
    elementos.append(tabela)
    doc.build(elementos)
    buffer.seek(0)
    return buffer.read()


def _metricas_concluidas(obras: List[dict]) -> dict:
    total = len(obras)
    base_counter: Counter[str] = Counter()
    status_counter: Counter[str] = Counter()
    duracoes: List[int] = []
    base_valor_counter: defaultdict[str, float] = defaultdict(float)
    total_valor = 0.0
    total_andamento = 0.0
    faltantes: List[dict] = []

    for obra in obras:
        base = str(obra.get('base') or 'Sem base').strip() or 'Sem base'
        base_counter[base] += 1
        status = str(obra.get('status') or '-').strip() or '-'
        status_counter[status] += 1
        valor_atual = _parse_decimal(obra.get('valor'))
        andamento_atual = _parse_decimal(obra.get('andamento'))
        valor_total_obra = valor_atual + andamento_atual
        total_valor += valor_total_obra
        total_andamento += andamento_atual
        base_valor_counter[base] += valor_total_obra
        pendencia = _pendencia_do_registro(obra, valor_atual, andamento_atual)
        if pendencia:
            faltantes.append(pendencia)
        inicio = _parse_data_generica(obra.get('inic'))
        fim = _parse_data_generica(obra.get('conc'))
        if inicio and fim:
            duracoes.append((fim - inicio).days + 1)

    media_dias = round(sum(duracoes) / len(duracoes), 1) if duracoes else 0
    maior_duracao = max(duracoes) if duracoes else 0
    base_top = base_counter.most_common(1)[0] if base_counter else ('-', 0)

    bases = [
        {
            'nome': base,
            'quantidade': quantidade,
            'percentual': round((quantidade / total) * 100, 1) if total else 0
        }
        for base, quantidade in base_counter.most_common()
    ]

    status = [
        {
            'nome': nome,
            'quantidade': quantidade,
            'percentual': round((quantidade / total) * 100, 1) if total else 0
        }
        for nome, quantidade in status_counter.most_common()
    ]

    bases_valor = [
        {
            'nome': base,
            'valor': valor,
            'percentual': round((valor / total_valor) * 100, 1) if total_valor else 0
        }
        for base, valor in sorted(base_valor_counter.items(), key=lambda item: item[1], reverse=True)
    ]

    return {
        'total': total,
        'media_dias': media_dias,
        'maior_duracao': maior_duracao,
        'base_top': base_top,
        'bases': bases,
        'status': status,
        'bases_valor': bases_valor,
        'total_valor': round(total_valor, 2),
        'total_andamento': round(total_andamento, 2),
        'faltantes': faltantes
    }


def identificar_base_por_equipe(equipe: str | None) -> str:
    codigo = normalizar_codigo_equipe(equipe)
    for base, prefixo in BASE_PREFIXES.items():
        if prefixo in codigo:
            return base
    return ''


def status_programado(status: str | None) -> bool:
    return normalizar_texto(status).startswith('PROGRAMAD')


def _carregar_controle_obras() -> tuple[List[dict], List[dict]]:
    caminho = DROPBOX_SETTINGS.controle_path
    if not caminho:
        raise RuntimeError('Defina DROPBOX_CONTROLE_PATH com o caminho do Controle - Obras no Dropbox.')
    token = get_access_token(DROPBOX_SETTINGS, DROPBOX_TOKEN_CACHE)
    conteudo = download_file(caminho, token)
    conteudo.seek(0)
    registros = carregar_registros_do_arquivo(conteudo)
    conteudo.seek(0)
    concluidas = carregar_concluidas_do_arquivo(conteudo)
    return registros, concluidas


def sincronizar_programacao_dropbox():
    global db_projetos, db_concluidas
    erros = []
    try:
        registros_total, concluidas_total = _carregar_controle_obras()
    except Exception as exc:  # noqa: BLE001
        erros.append(f'Controle - Obras: {exc}')
        registros_total = []
        concluidas_total = []

    registros_filtrados = filtrar_registros_por_equipes(registros_total, ALLOWED_EQUIPES)
    _definir_condicoes_basicas(registros_filtrados)
    if registros_filtrados:
        db_projetos = update_memory_and_persist(registros_filtrados, CACHE_FILE_PATH, HISTORY_FILE_PATH)
        mensagem = f"Atualização concluída! {len(db_projetos)} registros sincronizados."
        sucesso = True
    else:
        mensagem = 'Nenhum registro das equipes selecionadas foi sincronizado.'
        sucesso = False

    db_concluidas = concluidas_total or []
    save_cache(db_concluidas, CONCLUIDAS_FILE_PATH)

    if erros:
        print('[AVISO] Ocorreram erros ao sincronizar com o Dropbox:', erros)
        mensagem += ' ' + '; '.join(erros)

    return {
        'sucesso': sucesso,
        'mensagem': mensagem,
        'erros': erros,
        'registros': db_projetos
    }


def _aplicar_condicoes_cache_iniciais():
    if db_projetos:
        _definir_condicoes_basicas(db_projetos)


_aplicar_condicoes_cache_iniciais()


@app.context_processor
def inject_global_counts():
    return {
        'pendencias_alerta': _contar_pendencias_globais()
    }


@app.route('/')
def inicio():
    return render_template('inicio.html')


@app.route('/programacao_geral')
def programacao_geral():
    exibicao = db_projetos if db_projetos else []
    return render_template('programacao_geral.html', projetos=exibicao)


@app.route('/concluidas')
def concluidas():
    filtros = _coletar_filtros(request.args)
    todas_obras = _obras_concluidas_por_mes('')
    obras = _filtrar_obras_por_filtros(todas_obras, filtros)
    metricas = _metricas_concluidas(obras)
    bases_opcoes = sorted({(obra.get('base') or '').strip() for obra in todas_obras if (obra.get('base') or '').strip()})
    status_opcoes = sorted({(obra.get('status') or '').strip() for obra in todas_obras if (obra.get('status') or '').strip()})
    semanas_conjunto = set()
    for obra in todas_obras:
        inic_sem = _semana_str_to_int(obra.get('inic_sem'))
        conc_sem = _semana_str_to_int(obra.get('conc_sem'))
        if inic_sem:
            semanas_conjunto.add(inic_sem)
        if conc_sem:
            semanas_conjunto.add(conc_sem)
    semanas_opcoes = sorted(semanas_conjunto)
    sync_dt = _obter_cache_timestamp(CONCLUIDAS_FILE_PATH)
    export_params = {k: v for k, v in filtros.items() if v}
    export_url = url_for('exportar_concluidas')
    if export_params:
        export_url = f"{export_url}?{urlencode(export_params)}"
    export_pdf_url = url_for('exportar_concluidas_pdf')
    if export_params:
        export_pdf_url = f"{export_pdf_url}?{urlencode(export_params)}"
    pendentes_registros = metricas.get('faltantes', [])

    return render_template(
        'concluidas.html',
        obras=obras,
        metricas=metricas,
        filtros=filtros,
        bases_opcoes=bases_opcoes,
        status_opcoes=status_opcoes,
        semanas_opcoes=semanas_opcoes,
        sync_timestamp=_formata_timestamp_legivel(sync_dt),
        registros_sem_valor=pendentes_registros,
        pendentes_total=len(pendentes_registros),
        export_url=export_url,
        export_pdf_url=export_pdf_url
    )


@app.route('/concluidas/export')
def exportar_concluidas():
    filtros = _coletar_filtros(request.args)
    obras = _filtrar_obras_por_filtros(_obras_concluidas_por_mes(''), filtros)
    campos = ['base', 'obra', 'status', 'qtd_prog', 'inic', 'conc', 'inic_sem', 'conc_sem', 'prog', 'andamento', 'valor', 'vizita']
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=campos)
    writer.writeheader()
    for obra in obras:
        linha = {campo: obra.get(campo, '') for campo in campos}
        writer.writerow(linha)
    buffer.seek(0)
    nome_arquivo = f"concluidas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        buffer.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={nome_arquivo}'}
    )


@app.route('/concluidas/export/pdf')
def exportar_concluidas_pdf():
    filtros = _coletar_filtros(request.args)
    obras = _filtrar_obras_por_filtros(_obras_concluidas_por_mes(''), filtros)
    metricas = _metricas_concluidas(obras)
    pdf_bytes = _gerar_pdf_concluidas(obras, metricas)
    nome_arquivo = f"concluidas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return Response(
        pdf_bytes,
        mimetype='application/pdf',
        headers={'Content-Disposition': f'attachment; filename={nome_arquivo}'}
    )


@app.route('/concluidas/notificar', methods=['POST'])
def notificar_pendencias():
    pendentes = _listar_pendencias(db_concluidas or [])
    if not pendentes:
        return jsonify({'success': False, 'message': 'Nenhum registro pendente encontrado.'}), 200
    if not PENDENTES_WEBHOOK_URL:
        return jsonify({'success': False, 'message': 'Configuração PENDENTES_WEBHOOK_URL ausente.'}), 400
    texto_linhas = [f"{item['base']} - {item['obra']} ({item['motivo']})" for item in pendentes[:15]]
    if len(pendentes) > 15:
        texto_linhas.append(f"... e {len(pendentes) - 15} registros extras")
    payload = {
        'content': '**Pendências detectadas no painel Concluídas**\n' + '\n'.join(texto_linhas)
    }
    try:
        resposta = requests.post(PENDENTES_WEBHOOK_URL, json=payload, timeout=10)
        resposta.raise_for_status()
    except requests.RequestException as exc:
        return jsonify({'success': False, 'message': f'Falha ao enviar notificação: {exc}'}), 500
    return jsonify({'success': True, 'message': 'Notificação enviada com sucesso.'}), 200


@app.route('/importar_excel', methods=['POST'])
def importar_excel():
    global db_projetos, db_concluidas
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
        file.stream.seek(0)
        concluidas_total = carregar_concluidas_do_arquivo(file)
        registros_filtrados = filtrar_registros_por_equipes(registros, ALLOWED_EQUIPES)
        if not registros_filtrados:
            raise ValueError('Nenhuma das equipes permitidas foi encontrada no arquivo Excel enviado.')
        _definir_condicoes_basicas(registros_filtrados)
        db_projetos = update_memory_and_persist(registros_filtrados, CACHE_FILE_PATH, HISTORY_FILE_PATH)
        db_concluidas = concluidas_total or []
        save_cache(db_concluidas, CONCLUIDAS_FILE_PATH)
        flash(f'Sucesso! {len(db_projetos)} registros importados das equipes selecionadas.')
    except ValueError as ve:
        flash(str(ve))
        db_projetos = []
        db_concluidas = []
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
    criticos_por_pep = _agrupar_status_criticos(projetos_filtrados)

    return render_template(
        'mapa.html',
        projetos=projetos_filtrados,
        equipes=equipes_finais,
        datas_colunas=_datas_colunas(datas_exibicao),
        base_ativa=base_selecionada,
        mes_sel=mes_sel,
        semana_sel=semana_sel,
        criticos_por_pep=criticos_por_pep
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
