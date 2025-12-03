8import json
import os
from typing import Counter
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
from datetime import datetime
import numpy as np
import time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import re
import unicodedata

# CONFIGURAÇÃO DO LOGGER
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ETL")


# CARREGA CREDENCIAIS DO GOOGLE SHEETS DAS VARIÁVEIS DE AMBIENTE
def load_google_credentials():
    json_path = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not json_path:
        raise ValueError("Variável GOOGLE_CREDENTIALS_JSON não definida.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    return Credentials.from_service_account_file(json_path, scopes=scopes)


# EXTRACT – LE O GOOGLE SHEETS
def extract(sheet_id: str, tab_name: str):
    logger.info(f"Lendo planilha: {sheet_id} | Aba: {tab_name}")

    creds = load_google_credentials()
    client = gspread.authorize(creds)

    ws = client.open_by_key(sheet_id).worksheet(tab_name)

    # Mais rápido que get_all_records
    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])

    logger.info(f"Linhas carregadas: {len(df)}")
    return df, client

def limpar_texto(texto: str) -> str:
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-zA-Z\s]', ' ', texto.lower())
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def gerar_nome_grupo(textos: list[str]) -> str:
    """
    Gera um nome legível para o grupo com base nas palavras mais comuns
    """
    # Palavras-chave conhecidas (você pode expandir)
    palavras_chave = {
        'admin': 'Administrativo',
        'vigilancia': 'Vigilância Epidemiológica',
        'viep': 'Vigilância Epidemiológica',
        'visa': 'Vigilância Sanitária',
        'enferm': 'Enfermagem',
        'chef': 'Chefia',
        'acao': 'Ações e Serviços',
        'dent': 'Saúde Bucal',
        'odont': 'Saúde Bucal',
        'farm': 'Farmácia',
        'ti': 'TI/Suporte',
        'inform': 'TI/Suporte',
        'nutri': 'Nutrição',
        'imun': 'Imunização',
        'saude mulh': 'Saúde da Mulher',
        'saude crianc': 'Saúde da Criança',
        'adolescent': 'Saúde do Adolescente',
        'epidem': 'Epidemiologia',
        'nuget': 'Epidemiologia',
        'sanitar': 'Sanitarista',
        'serv geral': 'Serviços Gerais',
        'motorist': 'Serviços Gerais',
        'rh': 'RH',
        'pessoal': 'RH',
        'tecnico': 'Técnico',
        'referencia': 'Referência Técnica',
        'curativ': 'Curativos',
        'tubercul': 'Tuberculose',
        'sala imun': 'Imunização',
        'vacin': 'Imunização',
        'ouvid': 'Ouvidoria',
        'diret': 'Diretoria',
        'subcoord': 'Subcoordenadoria',
        'coord': 'Coordenação'
    }

    # Contar palavras limpas
    texto_unido = ' '.join([limpar_texto(t) for t in textos])
    palavras = texto_unido.split()
    contador = Counter(palavras)

    # Detectar palavra-chave mais frequente
    for palavra, nome in palavras_chave.items():
        if contador[palavra] > 0:
            return nome

    # Se não encontrou, usar as 2 palavras mais frequentes
    top = contador.most_common(2)
    if len(top) >= 2:
        return f"{top[0][0].capitalize()} / {top[1][0].capitalize()}"
    elif len(top) == 1:
        return f"{top[0][0].capitalize()}"
    else:
        return "Outros"
    
def normalizar_area_atuacao(df: pd.DataFrame, n_clusters: int = 15) -> pd.DataFrame:
    df = df.copy()
    df['area_limpa'] = df['area_atuacao'].fillna('').apply(limpar_texto)

    vetorizador = TfidfVectorizer(max_features=500, stop_words='english')
    X = vetorizador.fit_transform(df['area_limpa'])

    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df['grupo'] = kmeans.fit_predict(X)

    grupo_nome = {}
    for grupo in df['grupo'].unique():
        textos_originais = df[df['grupo'] == grupo]['area_atuacao'].tolist()
        nome = gerar_nome_grupo(textos_originais)
        grupo_nome[grupo] = nome

    df['area_atuacao_normalizada'] = df['grupo'].map(grupo_nome)

    print("📊 Grupos detectados:")
    for grupo, nome in grupo_nome.items():
        amostra = df[df['grupo'] == grupo]['area_atuacao'].mode()[0] if not df[df['grupo'] == grupo]['area_atuacao'].mode().empty else "-"
        print(f"  Grupo {grupo}: {nome} (ex: '{amostra}')")

    df['area_atuacao'] = df['area_atuacao_normalizada']
    df = df.drop(columns=['area_limpa', 'grupo', 'area_atuacao_normalizada'])

    return df

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renomeia as colunas do DataFrame conforme o mapeamento fornecido.
    """
    rename_map = {
        "Carimbo de data/hora": "timestamp",
        "1 - Distrito Sanitário (DS) ao qual você está vinculado": "ds_vinculado",
        "2 - Você é coordenador do DS?": "coord_ds",
        "3  - Se não, qual a sua área de atuação no DS?": "area_atuacao",
        "4 - Na sua área de atuação, você trabalha diretamente com a coleta, análise ou gestão da informação?": "atuacao_info",
        "6 - Você participa de qualificações sobre análise de dados, sistemas de informação ou planejamento em saúde?": "participa_qualificacoes",
        "7 - Na sua opinião existe,  por parte dos profissionais, uma cultura de valorização e uso de dados para a tomada de decisão no dia a dia do Distrito Sanitário?": "cultura_uso_dados",
        "8 - Quais ferramentas são mais utilizadas por você para analisar dados?": "ferramentas_analise",
        "9 - O distrito sanitário de saúde possui estações de trabalho (computador, teclado, mouse e monitor) em condições adequadas de uso? Quantas? ": "estacoes_trabalho_boas",
        "10 - Quantos computadores de mesa estão instalados, mas apresentam problemas recorrentes (lentidão, defeitos de hardware, etc.)?": "computadores_problema",
        "11 - O Distrito Sanitário possui notebooks em condições de uso? Se sim, quantos?": "notebooks_boas",
        "11.1 - Se respondeu sim na pergunta anterior, quantos desses notebooks possuem câmera, microfone e alto-falantes integrados e funcionais?  [Câmeras (webcams)]": "notebooks_com_camera",
        "11.1 - Se respondeu sim na pergunta anterior, quantos desses notebooks possuem câmera, microfone e alto-falantes integrados e funcionais?  [Caixas de som]": "notebooks_com_caixa_som",
        "11.1 - Se respondeu sim na pergunta anterior, quantos desses notebooks possuem câmera, microfone e alto-falantes integrados e funcionais?  [Microfones]": "notebooks_com_microfone",
        "12 - Para realizar reuniões remotas (videoconferências), marque quantos dos seguintes itens estão disponíveis e em condição de uso no Distrito Sanitário: [Câmeras (webcams)]": "webcams_disponiveis",
        "12 - Para realizar reuniões remotas (videoconferências), marque quantos dos seguintes itens estão disponíveis e em condição de uso no Distrito Sanitário: [Microfones (de mesa ou headsets):]": "microfones_disponiveis",
        "12 - Para realizar reuniões remotas (videoconferências), marque quantos dos seguintes itens estão disponíveis e em condição de uso no Distrito Sanitário: [Fones de ouvido (headsets ou simples):]": "fones_disponiveis",
        "12 - Para realizar reuniões remotas (videoconferências), marque quantos dos seguintes itens estão disponíveis e em condição de uso no Distrito Sanitário: [Caixas de som (para uso coletivo em sala):]": "caixas_som_disponiveis",
        "13 - O Distrito Sanitário possui televisores ou projetores que podem ser conectados a computadores/notebooks para apresentações? Se sim, quantos?  [Televisor]": "televisores",
        "14 - Existem cabos (ex: HDMI) ou adaptadores disponíveis e funcionais para conectar os computadores a esses televisores/projetores?": "cabos_adaptadores",
        "15 - Nos últimos 6 meses, o Distrito Sanitário possuiu conexão estável com a internet, permitindo o uso de videoconferências e acesso a sistemas de informação em saúde online e painéis de BI?": "internet_estavel",
        "15.1 - Se sim, em uma escala de 0 (péssima) a 10 (excelente), como você avalia a qualidade geral (velocidade e estabilidade) da internet?": "qualidade_internet",
        "16 - A rede de internet no Distrito Sanitário é:": "tipo_rede_internet",
        "17 - O acesso à rede Wi-Fi, se existente, é:": "acesso_wifi",
        "18 - A estrutura elétrica do Distrito Sanitário suporta a inserção de novos equipamentos tecnológicos (Ex: mais computadores, televisores, etc.)?": "estrutura_eletrica_suporta",
        "19 - O Distrito Sanitário possui uma sala adequada para a realização de reuniões em grupo e que possa abrigar a estrutura da Sala de Situação (projeção de painéis, computadores, etc.)?": "sala_situacao",
        "19.1 - Se possui uma sala, ela é climatizada (com ar-condicionado em funcionamento)?": "sala_climatizada",
        "20 - Há indicadores definidos para monitorar o desempenho das ações de saúde acompanhadas pela equipe técnica distrital?": "indicadores_definidos",
        "21 - As informações e análises de dados geradas no Distrito Sanitário subsidiam a elaboração das metas para a Programação Operativa Anual (POA)?": "dados_subsidiam_metas",
        "21.1 - Se sim na pergunta anterior, descreva brevemente quais são os principais indicadores acompanhados.": "principais_indicadores",
        "22 - As metas distritais são definidas a partir da análise dos dados?": "metas_base_dados",
        "23 - Existem meios de comunicação entre o Distrito Sanitário e as áreas técnicas do Nível Central para dialogar sobre os indicadores por território?": "comunicacao_nivel_central",
        "23.1 - Se sim, descreva quais os meios de comunicação utilizados (e-mail, reuniões, ofícios, grupos de mensagens, etc.) e com quais áreas técnicas.": "meios_comunicacao",
        "24 - Há periodicidade definida para atualização e revisão das metas estratégicas que compõem a POA do Distrito Sanitário?": "periodicidade_revisao_metas",
        "24.1 - Se sim, qual a periodicidade?": "periodicidade_metas",
        "25 - Quais bases de dados dos Sistemas de Informação em Saúde (SIS), elencados abaixo, você utiliza para tabulação e análise dos dados no Distrito Sanitário?": "sistemas_informacao_utilizados",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [SINASC]": "qualidade_sinasc",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [Vida+]": "qualidade_vida_plus",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [E-SUS AB/SISAB]": "qualidade_esus_sisab",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [SINAN]": "qualidade_sinan",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [GAL]": "qualidade_gal",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [SIA-SUS]": "qualidade_sia_sus",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [SIH-SUS]": "qualidade_sih_sus",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [SIM]": "qualidade_sim",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [Sivep-Gripe]": "qualidade_sivep_gripe",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [E-SUS Notifica]": "qualidade_esus_notifica",
        "26 - Apenas para os sistemas escolhidos na questão anterior, avalie a qualidade dos dados desses sistemas refletindo em suas dimensões de qualidade. [Sisvan]": "qualidade_sisvan",
        "27 - Em relação aos dados digitados no Distrito Sanitário, os fluxos de coleta e digitação estão formalizados com as unidades de saúde do território? (Ex: Fichas do SINAN que são oriundas de unidades hospitalares)": "fluxos_formalizados",
        "28 - Existe rotina de conferência e validação da consistência dos dados que são digitados no Distrito Sanitário?": "rotina_validacao",
        "29 - Na sua opinião, a equipe responsável pelo registro dos dados é devidamente treinada?": "equipe_treinada",
        "30 - Quais foram as ações (planejamento, intervenções, etc.) realizadas a partir dos dados tabulados no Distrito Sanitário?": "acoes_base_dados",
        "31 - Os resultados dos indicadores são comparados com séries históricas ou padrões de referência para análise de tendências?": "comparacao_series_historicas",
        "32 - Há momentos institucionais de devolutiva e discussões dos resultados com as equipes das unidades de saúde? ": "devolutiva_resultados",
        "33 - Os boletins, informes ou comunicados com resultados dos indicadores de saúde analisados são discutidos com as unidades de saúde do território? ": "discussao_boletins",
        "34 - Os painéis da Sala de Situação estão sendo utilizados para a tomada de decisão? ": "paineis_tomada_decisao",
        "34.1 - Se sim, especificar quais painéis são mais utilizados. ": "paineis_utilizados",
        "35 - Na sua opinião, existe estímulo à inovação e ao uso de novas ferramentas digitais para análise de dados no  Distrito Sanitário?": "estimulo_inovacao",
        "36 - Você compreende o papel estratégico da Sala de Situação como uma ferramenta de apoio à gestão?": "compreensao_sala_situacao",
        "37 - O Distrito Sanitário tem alguma unidade de saúde que utiliza a telessaúde para a realização de consultas ou atendimentos remotos? ": "telessaude",
        "38 - Você sabe o que é e qual o objetivo da Lei Geral de Proteção de Dados Pessoais (LGPD)?": "conhecimento_lgpd",
        "39 - Você já recebeu treinamentos ou orientações formais sobre a confidencialidade das informações de saúde e a conformidade com a LGPD?": "treinamento_lgpd",
        "40 - O acesso aos sistemas de informação é controlado por níveis de permissão individualizados (cada profissional com seu próprio login e senha)?": "acesso_individualizado",
        "41 - Existem protocolos de backup e recuperação de dados para os sistemas que são alimentados localmente no Distrito Sanitário?": "protocolos_backup",
        "5 - Em uma escala de 1 (nenhuma) a 5 (muita), como você avalia a competência técnica da equipe do distrito para analisar e interpretar indicadores de saúde? [.]": "competencia_tecnica_equipe",
        "13 - O Distrito Sanitário possui televisores ou projetores que podem ser conectados a computadores/notebooks para apresentações? Se sim, quantos?  [Projetor]": "projetores"
    }

    df = df.rename(columns=rename_map)
    return df

def transformar_atuacao_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a coluna de múltipla escolha em colunas binárias individuais
    """
    
    
    # Criar colunas binárias para cada opção
    df['atuacao_coleta'] = df['atuacao_info'].str.lower().str.strip().str.contains('coleta', na=False).astype(int)
    df['atuacao_analise'] = df['atuacao_info'].str.lower().str.strip().str.contains('análise', na=False).astype(int)
    df['atuacao_gestao'] = df['atuacao_info'].str.lower().str.strip().str.contains('gestão', na=False).astype(int)
    df['atuacao_nao'] = (df['atuacao_info'].str.lower().str.strip() == 'não').astype(int)
    
    # Debug: verificar se as colunas foram criadas
    '''
    print("Colunas criadas:")
    print(f"Coleta: {df['atuacao_coleta'].sum()}")
    print(f"Análise: {df['atuacao_analise'].sum()}")
    print(f"Gestão: {df['atuacao_gestao'].sum()}")
    print(f"Não: {df['atuacao_nao'].sum()}")
    '''
    
    # Agora criar as colunas combinadas
    df['atuacao_multipla'] = (df['atuacao_coleta'] + df['atuacao_analise'] + df['atuacao_gestao'] > 1).astype(int)
    df['atuacao_apenas_uma'] = (df['atuacao_coleta'] + df['atuacao_analise'] + df['atuacao_gestao'] == 1).astype(int)
    
    # Criar categorias combinadas
    conditions = [
        (df['atuacao_coleta'] == 1) & (df['atuacao_analise'] == 1) & (df['atuacao_gestao'] == 1),
        (df['atuacao_coleta'] == 1) & (df['atuacao_analise'] == 1),
        (df['atuacao_analise'] == 1) & (df['atuacao_gestao'] == 1),
        (df['atuacao_coleta'] == 1) & (df['atuacao_gestao'] == 1),
        (df['atuacao_coleta'] == 1),
        (df['atuacao_analise'] == 1),
        (df['atuacao_gestao'] == 1),
        (df['atuacao_nao'] == 1)
    ]
    
    choices = [
        'Coleta+Análise+Gestão',
        'Coleta+Análise',
        'Análise+Gestão',
        'Coleta+Gestão',
        'Apenas Coleta',
        'Apenas Análise',
        'Apenas Gestão',
        'Nenhuma'
    ]
    
    df['atuacao_categoria'] = np.select(conditions, choices, default='Outro')

    '''
    print("Distribuição das categorias:")
    print(df['atuacao_categoria'].value_counts())
    '''
    return df

def transformar_ferramentas_analise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a coluna de ferramentas de análise em colunas binárias
    """
    
    # Mapeamento básico das ferramentas padrão
    mapeamento_ferramentas = {
        'Planilhas (Excel, Google Sheets, etc.)': 'Planilhas',
        'Sistemas de tabulação do SUS (Tabwin, TabNet, etc.)': 'Sistemas SUS', 
        'Painéis de BI (Qlik Sense, Power BI, Looker Studio, Oracle, Sistema de Monitoramento da APS, etc.)': 'Painéis BI',
        'Apresentações (PowerPoint, Google Slides, etc.)': 'Apresentações',
    }
    
    def categorizar_ferramentas(texto):
        if pd.isna(texto) or texto == '':
            return set(['Outras Ferramentas'])
        
        texto = str(texto).strip().lower()
        categorias = set()
        
        # Verificar se é "nenhuma ferramenta" ou similar
        if any(phrase in texto for phrase in ['nenhuma ferramenta', 'não faz análise', 'não faz analise', 'incipiente', 'não faz análise']):
            return set(['Outras Ferramentas'])
        
        # 1. Primeiro busca pelos padrões exatos do mapeamento
        for padrao, categoria in mapeamento_ferramentas.items():
            if padrao.lower() in texto:
                categorias.add(categoria)
        
        # 2. Busca por palavras-chave para categorizar automaticamente
        palavras_chave = {
            'Planilhas': ['planilha', 'excel', 'google sheets', 'calc', 'sheet'],
            'Sistemas SUS': ['sistema', 'tabwin', 'tabnet', 'sus', 'sinan', 'siscan', 'sivep', 
                           'sisvan', 'sim', 'sinasc', 'gal', 'sia', 'sih', 'datasus', 'e-sus'],
            'Painéis BI': ['painel', 'bi', 'business intelligence', 'qlik', 'power bi', 'looker', 
                          'oracle', 'tableau', 'dashboard', 'painéis'],
            'Apresentações': ['apresentação', 'powerpoint', 'google slides', 'slide', 'ppt']
        }
        
        for categoria, palavras in palavras_chave.items():
            for palavra in palavras:
                if palavra in texto:
                    categorias.add(categoria)
                    break
        
        # Se não encontrou nenhuma categoria padrão, classifica como "Outras Ferramentas"
        if len(categorias) == 0:
            categorias.add('Outras Ferramentas')
        
        return categorias
    
    # Criar colunas dummy para cada categoria
    categorias = ['Planilhas', 'Sistemas SUS', 'Painéis BI', 'Apresentações', 'Outras Ferramentas']
    
    for categoria in categorias:
        coluna_nome = f"ferramenta_{categoria.lower().replace(' ', '_').replace('ã', 'a').replace('ç', 'c').replace('é', 'e')}"
        df[coluna_nome] = df['ferramentas_analise'].apply(
            lambda x: 1 if categoria in categorizar_ferramentas(x) else 0
        )
    
    # Debug: mostrar distribuição
    print("Distribuição das ferramentas:")
    for categoria in categorias:
        coluna = f"ferramenta_{categoria.lower().replace(' ', '_').replace('ã', 'a').replace('ç', 'c').replace('é', 'e')}"
        print(f"{categoria}: {df[coluna].sum()}")
    
    # Calcular quantidade de ferramentas usadas (excluindo "Outras Ferramentas")
    colunas_ferramentas_principais = [f"ferramenta_{cat.lower().replace(' ', '_').replace('ã', 'a').replace('ç', 'c').replace('é', 'e')}" 
                                     for cat in ['Planilhas', 'Sistemas SUS', 'Painéis BI', 'Apresentações']]
    
    df['qtd_ferramentas'] = df[colunas_ferramentas_principais].sum(axis=1)
    
    # Categorizar por quantidade
    conditions = [
        df['qtd_ferramentas'] == 0,
        df['qtd_ferramentas'] == 1,
        df['qtd_ferramentas'] == 2,
        df['qtd_ferramentas'] >= 3
    ]
    
    choices = ['Nenhuma', '1 ferramenta', '2 ferramentas', '3+ ferramentas']
    
    df['categoria_ferramentas'] = np.select(conditions, choices, default='Nenhuma')
    
    '''
    print(f"\nPessoas sem ferramentas principais: {(df['qtd_ferramentas'] == 0).sum()}")
    print(f"Pessoas com 1 ferramenta principal: {(df['qtd_ferramentas'] == 1).sum()}")
    print(f"Pessoas com 2 ferramentas principais: {(df['qtd_ferramentas'] == 2).sum()}")
    print(f"Pessoas com 3+ ferramentas principais: {(df['qtd_ferramentas'] >= 3).sum()}")
    print(f"Pessoas com outras ferramentas: {df['ferramenta_outras_ferramentas'].sum()}")
    '''

    return df

def transformar_categoricos_grandes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma as colunas em valores numéricos e categorias ordenados
    """
    
    # Mapeamento para valores numéricos (ponto médio dos intervalos)
    mapeamento_numerico = {
        '1 a 10': 5,
        '11 a 15': 13, 
        '16 a 20': 18,
        '21 ou mais': 25,
        'Nenhum': 0,
        'Não sei informar': None,
        '': None
    }
    
    
    # Aplicar mapeamento às colunas específicas
    colunas_perifericos = [
        'estacoes_trabalho_boas'
    
    ]

    
    for coluna in colunas_perifericos:
        if coluna not in df.columns:
            continue
            
        # 1. Criar versão numérica
        df[f'{coluna}_num'] = df[coluna].map(mapeamento_numerico)
        
        # 2. Substituir a coluna original pela versão ordenada
        ordem_categorias = ['Nenhum', '1 a 10', '11 a 15', '16 a 20', '21 ou mais']
        df[coluna] = pd.Categorical(
            df[coluna], 
            categories=ordem_categorias, 
            ordered=True
        )
        
        # 3. Criar categorias simplificadas
        conditions = [
            df[coluna].isin(['1 a 10', 'Nenhum']),
            df[coluna] == '11 a 15',
            df[coluna] == '16 a 20', 
            df[coluna] == '21 ou mais',
            df[coluna].isin(['Não sei informar', 'Não se aplica'])

        ]
        
        choices = ['Baixa', 'Média', 'Alta', 'Muito Alta', 'Não informado']
        df[f'{coluna}_cat_simples'] = np.select(conditions, choices, default='Não informado')

    return df

def transformar_categoricos_pequenos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma as colunas em valores numéricos e categorias ordenados
    """
    
    # Mapeamento para valores numéricos (ponto médio dos intervalos)
    mapeamento_numerico = {
        'Nenhum': 0,
        '1': 1,
        '2': 2,
        '3 a 5': 4,  # ponto médio
        '6 ou mais': 6,
        'Não sei informar': None,
        'Não se aplica': None,
        '': None
    }
    
    colunas_perifericos = [
        'webcams_disponiveis',
        'microfones_disponiveis', 
        'fones_disponiveis',
        'caixas_som_disponiveis',
        'notebooks_com_camera',
        'notebooks_com_caixa_som', 
        'notebooks_com_microfone',

        'notebooks_boas',
        'computadores_problema',

        'televisores',
        'projetores'
    
    ]
    
    for coluna in colunas_perifericos:
        if coluna not in df.columns:
            print(f"⚠️ Coluna {coluna} não encontrada")
            continue
            
        print(f"\n🔍 Analisando {coluna}:")
        print(f"Valores únicos antes: {df[coluna].unique()}")
        print(f"Contagem de valores:\n{df[coluna].value_counts()}")
        
        # 1. Criar versão numérica
        df[f'{coluna}_num'] = df[coluna].map(mapeamento_numerico)
        
        print(f"Valores numéricos criados:")
        print(f"Mínimo: {df[f'{coluna}_num'].min()}")
        print(f"Máximo: {df[f'{coluna}_num'].max()}")
        print(f"Média: {df[f'{coluna}_num'].mean()}")
        print(f"Contagem de NaNs: {df[f'{coluna}_num'].isna().sum()}")
        
        ordem_categorias = ['Nenhum', '1', '2', '3 a 5', '6 ou mais', 'Não sei informar', 'Não se aplica']
        df[coluna] = pd.Categorical(df[coluna], categories=ordem_categorias, ordered=True)
        
        conditions = [
            df[coluna].isin(['Nenhum', '1']),
            df[coluna] == '2',
            df[coluna] == '3 a 5', 
            df[coluna] == '6 ou mais',
            df[coluna].isin(['Não sei informar', 'Não se aplica'])
        ]
        
        choices = ['Baixa', 'Média', 'Alta', 'Muito Alta', 'Não informado']
        df[f'{coluna}_cat_simples'] = np.select(conditions, choices, default='Não informado')

    return df


def transformar_escala_ordenada(df: pd.DataFrame, coluna: str, ordem_categorias: list) -> pd.DataFrame:
    """
    Função auxiliar para transformar qualquer coluna em categórica ordenada
    """
    if coluna not in df.columns:
        return df
        
    # Converter para categórica ordenada
    df[coluna] = pd.Categorical(
        df[coluna], 
        categories=ordem_categorias, 
        ordered=True
    )
    
    return df

def transformar_escalas_zero_dez(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformação ordenada a todas as colunas de escala 0-10
    """
    mapeamento_numerico = {
        '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5,
        '6': 6, '7': 7, '8': 8, '9': 9, '10': 10,
        'Não sei informar': None, 'Não se aplica': None,
    }
    
    # Definir a ordem natural para escala 0-10
    ordem_escala = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    
    # Colunas que são escalas 0-10
    colunas_escala = ['qualidade_internet']
    
    for coluna in colunas_escala:
        if coluna not in df.columns:
            continue
            
        # 1. Criar versão numérica
        df[f'{coluna}_num'] = df[coluna].map(mapeamento_numerico)
        
        # 2. Aplicar transformação ordenada usando a função auxiliar
        df = transformar_escala_ordenada(df, coluna, ordem_escala)
        
        # 3. Criar categorias simplificadas
        conditions = [
            df[coluna].isin(['0', '1', '2', '3']),
            df[coluna].isin(['4', '5', '6']),
            df[coluna].isin(['7', '8']),
            df[coluna].isin(['9', '10']),
            df[coluna].isin(['Não sei informar', 'Não se aplica'])
        ]
        
        choices = ['Baixa (0-3)', 'Média (4-6)', 'Alta (7-8)', 'Muito Alta (9-10)', 'Não informado']
        df[f'{coluna}_cat_simples'] = np.select(conditions, choices, default='Não informado')
    
    return df

def transformar_escala_ordenada(df: pd.DataFrame, coluna: str, ordem_categorias: list) -> pd.DataFrame:
    """
    Função auxiliar para transformar qualquer coluna em categórica ordenada
    """
    if coluna not in df.columns:
        return df
        
    # Converter para categórica ordenada
    df[coluna] = pd.Categorical(
        df[coluna], 
        categories=ordem_categorias, 
        ordered=True
    )
    
    return df

def transformar_escalas_zero_dez(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformação ordenada a todas as colunas de escala 0-10
    """
    mapeamento_numerico = {
        '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5,
        '6': 6, '7': 7, '8': 8, '9': 9, '10': 10,
        'Não sei informar': None, 'Não se aplica': None,
    }
    
    # Definir a ordem natural para escala 0-10
    ordem_escala = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    
    # Colunas que são escalas 0-10
    colunas_escala = ['qualidade_internet']
    
    for coluna in colunas_escala:
        if coluna not in df.columns:
            continue
            
        # 1. Criar versão numérica
        df[f'{coluna}_num'] = df[coluna].map(mapeamento_numerico)
        
        # 2. Aplicar transformação ordenada usando a função auxiliar
        df = transformar_escala_ordenada(df, coluna, ordem_escala)
        
        # 3. Criar categorias simplificadas
        conditions = [
            df[coluna].isin(['0', '1', '2', '3']),
            df[coluna].isin(['4', '5', '6']),
            df[coluna].isin(['7', '8']),
            df[coluna].isin(['9', '10']),
            df[coluna].isin(['Não sei informar', 'Não se aplica'])
        ]
        
        choices = ['Baixa (0-3)', 'Média (4-6)', 'Alta (7-8)', 'Muito Alta (9-10)', 'Não informado']
        df[f'{coluna}_cat_simples'] = np.select(conditions, choices, default='Não informado')
    
    return df

def transformar_escalas_zero_cinco(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformação ordenada a todas as colunas de escala 1-5
    """
    mapeamento_numerico = {
        '1': 1, '2': 2, '3': 3, '4': 4, '5': 5,
        'Não sei informar': None
    }
    
    # Definir a ordem natural para escala 1-5
    ordem_escala = ['1', '2', '3', '4', '5']
    
    # Colunas que são escalas 1-5
    colunas_escala = ['competencia_tecnica_equipe']
    
    for coluna in colunas_escala:
        if coluna not in df.columns:
            continue
            
        # 1. Criar versão numérica
        df[f'{coluna}_num'] = df[coluna].map(mapeamento_numerico)
        
        # 2. Aplicar transformação ordenada usando a função auxiliar
        df = transformar_escala_ordenada(df, coluna, ordem_escala)
        
        # 3. Criar categorias simplificadas
        conditions = [
            df[coluna].isin(['1', '2']),
            df[coluna] == '3',
            df[coluna].isin(['4', '5']),
            df[coluna] == 'Não sei informar'
        ]
        
        choices = ['Baixa (1-2)', 'Média (3)', 'Alta (4-5)', 'Não informado']
        df[f'{coluna}_cat_simples'] = np.select(conditions, choices, default='Não informado')
    
    return df

def tratar_sistemas_e_qualidade(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata as colunas Q25 (sistemas usados) e Q26 (qualidade dos dados).
    Cria colunas binárias de uso e avaliação por sistema.
    Detecta inconsistências entre uso e avaliação.
    """

    # Lista de sistemas possíveis (baseado nas colunas de qualidade)
    sistemas = [
        'SINASC', 'Vida+', 'E-SUS AB/SISAB', 'SINAN', 'GAL',
        'SIA-SUS', 'SIH-SUS', 'SIM', 'Sivep-Gripe', 'E-SUS Notifica', 'Sisvan'
    ]

    # Normalizar a coluna de sistemas usados
    df['sistemas_informacao_utilizados'] = df['sistemas_informacao_utilizados'].fillna('').str.strip()

    # Criar colunas binárias de uso
    for sistema in sistemas:
        col_uso = f'usou_{sistema.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}'
        df[col_uso] = df['sistemas_informacao_utilizados'].str.contains(sistema, case=False, na=False).astype(int)

    # Criar colunas binárias de avaliação (se avaliou, o valor não é vazio ou "Não se aplica")
    for sistema in sistemas:
        col_qualidade = f'qualidade_{sistema.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}'
        col_avaliou = f'avaliou_{sistema.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}'

        if col_qualidade in df.columns:
            df[col_avaliou] = df[col_qualidade].notna() & (~df[col_qualidade].isin(['', 'Não se aplica'])).astype(int)
        else:
            df[col_avaliou] = 0  # Se não existe coluna de qualidade, não avaliou

    # Detectar inconsistências
    df['sistemas_inconsistentes'] = 0
    for sistema in sistemas:
        col_uso = f'usou_{sistema.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}'
        col_avaliou = f'avaliou_{sistema.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}'

        # Inconsistência: avaliou mas não usou
        inconsistencia = (df[col_avaliou] == 1) & (df[col_uso] == 0)
        df.loc[inconsistencia, 'sistemas_inconsistentes'] += 1

    # Criar contadores
    df['total_sistemas_usados'] = df[[f'usou_{s.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}' for s in sistemas]].sum(axis=1)
    df['total_sistemas_avaliados'] = df[[f'avaliou_{s.lower().replace("-", "_").replace("+", "plus").replace(" ", "_")}' for s in sistemas]].sum(axis=1)

    return df

def criar_resumo_sistemas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria uma tabela resumo com uso e qualidade média por sistema.
    """

    sistemas = [
        'SINASC', 'Vida+', 'E-SUS AB/SISAB', 'SINAN', 'GAL', 'SIA-SUS', 'SIH-SUS', 'SIM', 'Sivep-Gripe', 'E-SUS Notifica', 'Sisvan'
    ]

    # Mapeamento correto de sistema -> nomes reais das colunas
    col_map = {
        'SINASC': ('usou_sinasc', 'qualidade_sinasc'),
        'Vida+': ('usou_vidaplus', 'qualidade_vida_plus'),
        'E-SUS AB/SISAB': ('usou_e_sus_ab/sisab', 'qualidade_esus_sisab'),
        'SINAN': ('usou_sinan', 'qualidade_sinan'),
        'GAL': ('usou_gal', 'qualidade_gal'),
        'SIA-SUS': ('usou_sia_sus', 'qualidade_sia_sus'),
        'SIH-SUS': ('usou_sih_sus', 'qualidade_sih_sus'),
        'SIM': ('usou_sim', 'qualidade_sim'),
        'Sivep-Gripe': ('usou_sivep_gripe', 'qualidade_sivep_gripe'),
        'E-SUS Notifica': ('usou_e_sus_notifica', 'qualidade_esus_notifica'),
        'Sisvan': ('usou_sisvan', 'qualidade_sisvan')
    }

    qualidade_map = {
        'Muito Ruim': 1,
        'Ruim': 2, 
        'Bom': 3,
        'Muito bom': 4,
        'Excelente': 5
    }

    resumo = []
    for sistema in sistemas:
        col_uso, col_qualidade = col_map[sistema]
        
        uso = df[col_uso].sum()
        qualidades = df[col_qualidade].dropna().map(qualidade_map)
        
        if len(qualidades) > 0:
            qualidade_media = qualidades.mean()
        else:
            qualidade_media = None
        
        resumo.append({
            'sistema': sistema,
            'uso': uso,
            'qualidade_media': qualidade_media
        })
    
    resumo_df = pd.DataFrame(resumo)
    resumo_df = resumo_df[resumo_df['uso'] > 0]  # só quem foi usado
    return resumo_df

# FUNÇÕES para INDICADORES – PONTUAÇÃO POR DIMENSÃO

def _pontuar_pessoas(df: pd.DataFrame) -> pd.Series:
    """
    0-100 baseado em:
    - competencia_tecnica_equipe (1-5)
    - participa_qualificacoes
    - cultura_uso_dados
    - qtd_ferramentas (0-4+)
    """
    logger = logging.getLogger("ETL.ip_sala_situacao.pessoas")

    # ---- valores brutos ----
    comp_raw = pd.to_numeric(df['competencia_tecnica_equipe_num'], errors='coerce').fillna(1)
    comp = comp_raw / 5                                    # 0-1
    qual = df['participa_qualificacoes'].map({
        'Sim, regularmente (ao menos uma vez por ano)': 1,
        'Sim, mas esporadicamente': 0.5,
        'Não': 0
    }).fillna(0)
    cult = df['cultura_uso_dados'].map({
        'Sim, a análise de dados é central em nossas reuniões e planejamentos.': 1,
        'Em partes, usamos dados, mas as decisões ainda são muito baseadas na experiência.': 0.5,
        'Não, os dados são vistos mais como uma obrigação de preenchimento do que como uma ferramenta de gestão.': 0
    }).fillna(0)
    ferr = np.minimum(df['qtd_ferramentas'].fillna(0), 4) / 4   # 0-1

    # ---- log de exemplo (primeira linha) ----
    if len(df) > 0:
        logger.info(f"Exemplo linha 0 -> comp: {comp.iloc[0]:.2f}, qual: {qual.iloc[0]:.2f}, "
                    f"cult: {cult.iloc[0]:.2f}, ferr: {ferr.iloc[0]:.2f}")

    # ---- score final 0-100 (sem duplicar *100) ----
    score = (
        comp * 40 +
        qual * 25 +
        cult * 25 +
        ferr * 10
    ) * 1
    return score.clip(0, 100)

def _pontuar_infraestrutura(df: pd.DataFrame) -> pd.Series:
    logger = logging.getLogger("ETL.ip_sala_situacao.infra")

    est = df['estacoes_trabalho_boas_num'].fillna(0) / 25
    note = df['notebooks_boas_num'].fillna(0) / 6
    net_ok = (df['internet_estavel'] == 'Sim').astype(int)
    net_not = pd.to_numeric(df['qualidade_internet_num'], errors='coerce').fillna(0) / 10
    sala = (df['sala_situacao'] == 'Sim, possui uma sala adequada').astype(int)
    cabo = (df['cabos_adaptadores'] == 'Sim, para todos os equipamentos').astype(int)

    if len(df) > 0:
        logger.info(f"Exemplo linha 0 -> est: {est.iloc[0]:.2f}, note: {note.iloc[0]:.2f}, "
                    f"net_ok: {net_ok.iloc[0]:.2f}, net_not: {net_not.iloc[0]:.2f}, "
                    f"sala: {sala.iloc[0]:.2f}, cabo: {cabo.iloc[0]:.2f}")

    score = (
        est * 30 +
        note * 15 +
        net_ok * 20 + net_not * 10 +
        sala * 20 +
        cabo * 5
    )  # <-- removido *100
    return score.clip(0, 100)

def _pontuar_processos(df: pd.DataFrame) -> pd.Series:
    logger = logging.getLogger("ETL.ip_sala_situacao.processos")

    # ---- etapas normalizadas 0-1 ----
    ind_def     = (df['indicadores_definidos']        == 'Sim').astype(int)
    dados_meta  = df['dados_subsidiam_metas'].map({'Sim': 1, 'Parcialmente': 0.5, 'Não': 0, 'Não sei informar': 0})
    meta_dados  = df['metas_base_dados'].map({'Sim': 1, 'Parcialmente': 0.5, 'Não': 0, 'Não sei informar': 0})
    fluxos      = (df['fluxos_formalizados']         == 'Sim').astype(int)
    rotina      = (df['rotina_validacao']            == 'Sim').astype(int)
    paineis     = df['paineis_tomada_decisao'].map({'Sim': 1, 'Parcialmente': 0.5, 'Não': 0, 'Não sei informar': 0})

    # ---- log amostral (5 primeiras) ----
    for i in range(min(5, len(df))):
        logger.info(f"idx {i} -> ind_def:{ind_def.iloc[i]:.2f} dados_meta:{dados_meta.iloc[i]:.2f} "
                    f"meta_dados:{meta_dados.iloc[i]:.2f} fluxos:{fluxos.iloc[i]:.2f} "
                    f"rotina:{rotina.iloc[i]:.2f} paineis:{paineis.iloc[i]:.2f}")

    score = (
        ind_def   * 20 +
        dados_meta * 15 +
        meta_dados * 15 +
        fluxos    * 15 +
        rotina    * 20 +
        paineis   * 15
    )  # já 0-100
    logger.info(f"ESTATÍSTICA processos -> min:{score.min():.1f} | média:{score.mean():.1f} | max:{score.max():.1f}")
    return score.clip(0, 100)

def _pontuar_seguranca(df: pd.DataFrame) -> pd.Series:
    logger = logging.getLogger("ETL.ip_sala_situacao.seguranca")

    # ---- etapas normalizadas 0-1 ----
    lgpd        = df['conhecimento_lgpd'].map({'Sim': 1, 'Tenho uma noção, mas não conheço em detalhes': 0.5, 'Não': 0})
    treino      = df['treinamento_lgpd'].map({'Sim': 1, 'Apenas orientações informais': 0.5, 'Não': 0})
    acesso      = df['acesso_individualizado'].map({'Sim': 1, 'Em parte (alguns sistemas sim, outros não)': 0.5, 'Não, os acessos são compartilhados': 0})
    backup      = (df['protocolos_backup'] == 'Sim').astype(int)

    # ---- log amostral (5 primeiras) ----
    for i in range(min(5, len(df))):
        logger.info(f"idx {i} -> lgpd:{lgpd.iloc[i]:.2f} treino:{treino.iloc[i]:.2f} "
                    f"acesso:{acesso.iloc[i]:.2f} backup:{backup.iloc[i]:.2f}")

    score = (
        lgpd   * 30 +
        treino * 25 +
        acesso * 25 +
        backup * 20
    ) 
    logger.info(f"ESTATÍSTICA seguranca -> min:{score.min():.1f} | média:{score.mean():.1f} | max:{score.max():.1f}")
    return score.clip(0, 100)

def adicionar_ip_sala_situacao(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculando IP-SalaSit...")
    df = df.copy()

    num_cols = ['competencia_tecnica_equipe_num', 'estacoes_trabalho_boas_num',
                'notebooks_boas_num', 'qualidade_internet_num', 'qtd_ferramentas']
    for c in num_cols:
        if c not in df.columns:
            logger.warning(f"Coluna {c} não encontrada – preenchendo com 0")
            df[c] = 0

    df['ip_pessoas'] = _pontuar_pessoas(df)
    df['ip_infra'] = _pontuar_infraestrutura(df)
    df['ip_processos'] = _pontuar_processos(df)
    df['ip_seguranca'] = _pontuar_seguranca(df)

    df['ip_sala_situacao'] = (
        0.30 * df['ip_pessoas'] +
        0.30 * df['ip_infra'] +
        0.25 * df['ip_processos'] +
        0.15 * df['ip_seguranca']
    ).round(2)

    logger.info("IP-SalaSit calculado com sucesso.")
    return df

# -------------------------------------------------------------------
# # TRANSFORM – APLICA TRANSFORMAÇÕES NOS DADOS
# -------------------------------------------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando transformações...")
    
    df = rename_columns(df)
    df = normalizar_area_atuacao(df)
    df = transformar_atuacao_info(df)
    df = transformar_ferramentas_analise(df)
    df = transformar_categoricos_grandes(df)
    df = transformar_categoricos_pequenos(df)
    df = transformar_escalas_zero_dez(df)
    df = transformar_escalas_zero_cinco(df)
    df = tratar_sistemas_e_qualidade(df)
    df = adicionar_ip_sala_situacao(df)

    logger.info("Transformação concluída.")
    return df

# -------------------------------------------------------------------
# LOAD – CRIA ABA E ESCREVE DADOS NA MESMA PLANILHA
# -------------------------------------------------------------------
def load_to_sheet(client, sheet_id: str, df: pd.DataFrame, new_tab: str = "DadosEtl"):
    logger.info(f"Atualizando aba '{new_tab}' sem excluir...")

    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(new_tab)
        logger.info(f"Aba '{new_tab}' encontrada. Limpando e sobrescrevendo...")
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Aba '{new_tab}' não existe. Criando...")
        ws = sh.add_worksheet(title=new_tab, rows=str(len(df) + 5), cols=str(len(df.columns) + 5))

    ws.clear()

    df_preparado = df.copy()
    for col in df_preparado.columns:
        if df_preparado[col].dtype.name == 'category':
            df_preparado[col] = df_preparado[col].astype(str)
    df_preparado = df_preparado.fillna('')

    values = [df_preparado.columns.tolist()] + df_preparado.values.tolist()
    ws.update(values)

    ws.freeze(rows=1)

    logger.info(f"Aba '{new_tab}' atualizada com sucesso — sem excluir!")

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    SHEET_ID = "..."
    TAB = "BaseBruta"
    NEW_TAB = "DadosEtl"

    df, client = extract(SHEET_ID, TAB)
    df = transform(df)
    
    load_to_sheet(client, SHEET_ID, df, NEW_TAB)

    logger.info("ETL COMPLETO! Todas as abas formatadas como tabelas.")


if __name__ == "__main__":
    main()