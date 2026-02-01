import os
import unicodedata
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash
from datetime import datetime, timedelta

app = Flask(__name__)
# O Render usa a variável de ambiente SECRET_KEY se configurada, senão usa a padrão
app.secret_key = os.environ.get('SECRET_KEY', 'supersecretkey-mapa-2024')

# Banco de dados em memória (limpa ao reiniciar no plano free)
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
    return render_template('programacao_geral.html', projetos=db_projetos)

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
        if 'id' in df.columns:
            df = df.dropna(subset=['id'])
        
        if 'data' in df.columns:
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

    # Filtragem
    projetos_filtrados = []
    for p in db_projetos:
        equipe = str(p.get('equipe', '')).upper()
        if not base_norm or (prefixo_alvo and prefixo_alvo in equipe):
            dt_str = str(p.get('data', '')).strip()
            if mes_sel:
                try:
                    dt = datetime.strptime(dt_str, '%d/%m/%Y')
                    if str(dt.month).zfill(2) != mes_sel: continue
                except: continue
            if semana_sel:
                try:
                    dt = datetime.strptime(dt_str, '%d/%m/%Y')
                    if str(dt.isocalendar().week) != semana_sel: continue
                except: continue
            projetos_filtrados.append(p)

    # Timeline de Datas
    datas_em_dados = [p.get('data') for p in projetos_filtrados if p.get('data') != "-"]
    datas_colunas = []
    if datas_em_dados:
        try:
            dt_objs = [datetime.strptime(d, '%d/%m/%Y') for d in datas_em_dados]
            inicio_t = min(dt_objs)
            fim_t = inicio_t + timedelta(days=2) if base_norm else max(dt_objs)
            intervalo = pd.date_range(start=inicio_t, end=fim_t)
            dias_pt = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
            for d in intervalo:
                datas_colunas.append({
                    'original': d.strftime('%d/%m/%Y'),
                    'exibicao': f"{d.strftime('%d/%m')} - {dias_pt[d.weekday()]}",
                    'dia_num': d.weekday()
                })
        except: pass

    # Ordem Prioritária (BCB, ITM, STI)
    ordem_ref = ["BCB", "ITM", "STI"]
    equipes_presentes = sorted(list(set(p.get('equipe') for p in projetos_filtrados)))
    
    # Ordenação customizada: equipes da ordem_ref primeiro, depois as outras
    equipes_finais = sorted(equipes_presentes, key=lambda x: next((i for i, ref in enumerate(ordem_ref) if ref in x), 999))

    return render_template('mapa.html', projetos=projetos_filtrados, equipes=equipes_finais, 
                           datas_colunas=datas_colunas, base_ativa=base_selecionada, 
                           mes_sel=mes_sel, semana_sel=semana_sel)

@app.route('/limpar_dados')
def limpar_dados():
    global db_projetos
    db_projetos = []
    return redirect(url_for('programacao_geral'))

@app.route('/semanal')
def semanal():
    return render_template('semanal.html')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)