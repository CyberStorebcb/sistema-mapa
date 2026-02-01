import os
import unicodedata
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash
from datetime import datetime, timedelta

app = Flask(__name__)
# Em produção, o Render pode usar uma variável de ambiente, ou usa a string padrão
app.secret_key = os.environ.get('SECRET_KEY', 'supersecretkey-mapa-123')
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Banco de dados em memória (Nota: No plano grátis do Render, os dados resetam se o app dormir)
db_projetos = []

def normalizar_texto(texto):
    if not texto: return ""
    return "".join(c for c in unicodedata.normalize('NFD', str(texto))
                  if unicodedata.category(c) != 'Mn').upper().strip()

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
        # header=3 pula as primeiras linhas conforme a estrutura do seu Excel
        df = pd.read_excel(file, header=3)
        df = df.dropna(axis=1, how='all')
        df.columns = [str(c).strip().upper() for c in df.columns]

        mapeamento = {
            'ID': 'id', 'DATA': 'data', 'PERÍODO': 'periodo', 'TIPO': 'tipo',
            'EQUIPE': 'equipe', 'ENCARREGADO': 'encarregado', 'SUPERVISOR': 'supervisor',
            'COM LV': 'com_lv', 'SI/NR': 'si_nr', 'PEP': 'pep', 'NOTA': 'nota',
            'LOCAL': 'local', 'STATUS': 'status', 'CONDIÇÃO': 'condicao', 'OBSERVAÇÃO': 'obs'
        }
        
        df = df.rename(columns=mapeamento)
        
        # Filtra linhas sem ID ou Equipe para evitar lixo
        if 'id' in df.columns:
            df = df.dropna(subset=['id'])
        
        if 'data' in df.columns:
            # Garante que a data seja tratada corretamente
            df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.strftime('%d/%m/%Y')

        df = df.fillna("-")
        
        colunas_validas = [c for c in mapeamento.values() if c in df.columns]
        db_projetos = df[colunas_validas].to_dict(orient='records')
        
        flash(f'Sucesso! {len(db_projetos)} registros importados.')
    except Exception as e:
        flash(f'Erro ao processar: {e}')
        
    return redirect(url_for('programacao_geral'))

@app.route('/mapa')
def mapa():
    global db_projetos
    base_selecionada = request.args.get('base', '')
    mes_sel = request.args.get('mes', '')
    semana_sel = request.args.get('semana', '')
    base_norm = normalizar_texto(base_selecionada)

    prefixos = {"BACABAL": "BCB", "ITAPECURU": "ITM", "SANTA INES": "STI"}
    prefixo_alvo = ""
    for nome, pref in prefixos.items():
        if nome in base_norm:
            prefixo_alvo = pref

    projetos_validos = []
    for p in db_projetos:
        equipe = str(p.get('equipe', '')).upper()
        if not base_norm or (prefixo_alvo and prefixo_alvo in equipe):
            p['data'] = str(p.get('data', '')).strip()
            projetos_validos.append(p)
    
    projetos_filtrados = []
    for p in projetos_validos:
        if mes_sel:
            try:
                data_projeto = datetime.strptime(p.get('data'), '%d/%m/%Y')
                if str(data_projeto.month).zfill(2) != mes_sel:
                    continue
            except: continue

        if semana_sel:
            try:
                data_projeto = datetime.strptime(p.get('data'), '%d/%m/%Y')
                if str(data_projeto.isocalendar().week) != semana_sel:
                    continue
            except: continue
        projetos_filtrados.append(p)

    projetos_validos = projetos_filtrados

    datas_em_dados = [p.get('data') for p in projetos_validos if p.get('data') != "-"]
    
    if datas_em_dados:
        try:
            dt_objetos = [datetime.strptime(d, '%d/%m/%Y') for d in datas_em_dados]
            data_inicio = min(dt_objetos)
            data_fim = data_inicio + timedelta(days=2) if base_norm else max(dt_objetos)
            
            intervalo = pd.date_range(start=data_inicio, end=data_fim)
            datas_exibicao = [d.strftime('%d/%m/%Y') for d in intervalo]
        except: datas_exibicao = []
    else:
        datas_exibicao = []

    dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    datas_colunas = []
    for d_str in datas_exibicao:
        try:
            dt_obj = datetime.strptime(d_str, '%d/%m/%Y')
            datas_colunas.append({
                'original': d_str,
                'exibicao': f"{dt_obj.strftime('%d/%m')} - {dias_semana[dt_obj.weekday()]}",
                'dia_num': dt_obj.weekday() 
            })
        except: continue

    ordem_prioritaria = [
        "MA-BCB-O001M", "MA-BCB-O002M", "MA-BCB-O003M", "MA-BCB-O004M",
        "MA-BCB-O005M", "MA-BCB-O006M", "MA-BCB-O007M", "MA-BCB-T001M",
        "MA-ITM-O001M", "MA-ITM-O002M", "MA-ITM-O003M", "MA-ITM-O004M",
        "MA-STI-O001M", "MA-STI-O002M", "MA-STI-O003M", "MA-STI-O004M", "MA-STI-T001M"
    ]
    
    equipes_nos_dados = set(p.get('equipe') for p in projetos_validos if p.get('data') in datas_exibicao)
    equipes_finais = [e for e in ordem_prioritaria if e in equipes_nos_dados]
    outras = sorted(list(equipes_nos_dados - set(ordem_prioritaria)))
    equipes_finais.extend(outras)

    return render_template('mapa.html', projetos=projetos_validos, equipes=equipes_finais, 
                           datas_colunas=datas_colunas, base_ativa=base_selecionada, 
                           mes_sel=mes_sel, semana_sel=semana_sel)

@app.route('/semanal')
def semanal(): 
    mes_sel = request.args.get('mes', '')
    semana_sel = request.args.get('semana', '')
    
    projetos_filtrados = []
    for p in db_projetos:
        if mes_sel:
            try:
                data_projeto = datetime.strptime(p.get('data'), '%d/%m/%Y')
                if str(data_projeto.month).zfill(2) != mes_sel: continue
            except: continue
        if semana_sel:
            try:
                data_projeto = datetime.strptime(p.get('data'), '%d/%m/%Y')
                if str(data_projeto.isocalendar().week) != semana_sel: continue
            except: continue
        projetos_filtrados.append(p)

    datas_em_dados = [p.get('data') for p in projetos_filtrados if p.get('data') != "-"]
    if datas_em_dados:
        try:
            dt_objetos = [datetime.strptime(d, '%d/%m/%Y') for d in datas_em_dados]
            intervalo = pd.date_range(start=min(dt_objetos), end=max(dt_objetos))
            datas_lista = [d.strftime('%d/%m/%Y') for d in intervalo]
        except: datas_lista = []
    else: datas_lista = []

    dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    datas_colunas = []
    for d_str in datas_lista:
        try:
            dt_obj = datetime.strptime(d_str, '%d/%m/%Y')
            datas_colunas.append({
                'original': d_str,
                'exibicao': f"{dt_obj.strftime('%d/%m')} - {dias_semana[dt_obj.weekday()]}",
                'dia_num': dt_obj.weekday()
            })
        except: continue

    equipes_finais = sorted(list(set(p.get('equipe') for p in projetos_filtrados if p.get('equipe') != "-")))

    return render_template('mapa.html', base_ativa="Semanal", projetos=projetos_filtrados, 
                           equipes=equipes_finais, datas_colunas=datas_colunas,
                           mes_sel=mes_sel, semana_sel=semana_sel)

@app.route('/limpar_dados')
def limpar_dados():
    global db_projetos
    db_projetos = []  
    flash('A tabela foi limpa com sucesso!')
    return redirect(url_for('programacao_geral'))

if __name__ == '__main__':
    # Configuração crucial para o Render: lê a porta da variável de ambiente
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)