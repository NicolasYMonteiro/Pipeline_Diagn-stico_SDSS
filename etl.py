import json
import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
from datetime import datetime
import numpy as np
import time


# -------------------------------------------------------------------
# CONFIGURAÇÃO DO LOGGER
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ETL")


# -------------------------------------------------------------------
# CARREGA CREDENCIAIS DO GOOGLE SHEETS DAS VARIÁVEIS DE AMBIENTE
# -------------------------------------------------------------------
def load_google_credentials():
    json_path = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not json_path:
        raise ValueError("Variável GOOGLE_CREDENTIALS_JSON não definida.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    return Credentials.from_service_account_file(json_path, scopes=scopes)



# -------------------------------------------------------------------
# EXTRACT – LE O GOOGLE SHEETS
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# TRANSFORM – TRATAMENTO PADRÃO (MODIFIQUE COMO QUISER)
# -------------------------------------------------------------------
def normalizar_area_atuacao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza a coluna 'area_atuacao' agrupando variações em categorias consistentes
    """
    
    # Mapeamento de normalização
    mapeamento_areas = {
        # Vazio
        '': "Desconhecido",

        # Subcoordenadorias
        'Subcoordenadoria de Atenção e Vigilância à Saúde': 'Subcoordenadoria de Atenção e Vigilância',
        'Subcoordenadoria administrativa': 'Subcoordenadoria Administrativa',
        'Subcoordenadoria de Atenção e Vigilância à Saúde': 'Subcoordenadoria de Atenção e Vigilância',
        
        # Áreas administrativas
        'Área administrativa': 'Administrativo',
        'Técnica administrativa': 'Administrativo',
        'Auxiliar administrativo': 'Administrativo',
        'Técnica Administrativa': 'Administrativo',
        'Área técnica': 'Administrativo',
        'Tecnica Administrativa / RH': 'Administrativo/RH',
        'Administrativo': 'Administrativo',
        'admistrativa de apoio': 'Administrativo',
        'Administração': 'Administrativo',
        'Auxiliar administrativa': 'Administrativo',
        'Administração -': 'Administrativo',
        'Apoio administrativo': 'Administrativo',
        'Administrativa - Programa Saúde da mulher e Fichas do Sinan': 'Administrativo',
        'administrativa': 'Administrativo',
        'Admistrativo': 'Administrativo',
        'Setor administrativo': 'Administrativo',
        'Serviço de ações do administrativo': 'Administrativo',
        'Assistente administrativo - digitação de fichas, documentação, envio e coleta de documentação.': 'Administrativo',
        'Auxiliar administrativo de ações': 'Administrativo',
        'Técnica administrativa - suporte ao serviço': 'Administrativo',
        'Administrativa': 'Administrativo',
        'Administrativo - setor de ações': 'Administrativo',
        'Tecnico de nivel medio I atendimento': 'Administrativo',
        
        # RH
        'Chefe de RH': 'RH',
        'Chefia de RH': 'RH',
        'Setor Administrativo/RH': 'RH',
        'RH': 'RH',
        
        # Vigilância Epidemiológica (VIEP)
        'Vigilância epidemiológica': 'Vigilância Epidemiológica',
        'Vigilância epidemiológica': 'Vigilância Epidemiológica',
        'Tecnica da VIEP': 'Vigilância Epidemiológica',
        'VIEP': 'Vigilância Epidemiológica',
        'Técnica da VIEP': 'Vigilância Epidemiológica',
        'VIEP- VIGILÂNCIA EPIDEMIOLÓGICA': 'Vigilância Epidemiológica',
        'Tec. VIEP': 'Vigilância Epidemiológica',
        'Técnico da viep': 'Vigilância Epidemiológica',
        'Chefia de viep': 'Vigilância Epidemiológica',
        'Chefia da vigilância epidemiológica': 'Vigilância Epidemiológica',
        'Chefia do setor Vigilância Epidemiológica e Sistemas de Informação em Saúde': 'Vigilância Epidemiológica',
        'Operadora de Sistemas da VIEP': 'Vigilância Epidemiológica',
        'Técnica de Enfermagem- VIEP': 'Vigilância Epidemiológica',
        'vigilancia epidemiologica': 'Vigilância Epidemiológica',
        
        # Vigilância Sanitária (VISA)
        'Protocolo da VISA': 'Vigilância Sanitária',
        'Vigilância sanitária': 'Vigilância Sanitária',
        'Fiscal de controle sanitário': 'Vigilância Sanitária',
        'Fiscal dr controle sanitário': 'Vigilância Sanitária',
        'Vigilância Sanitária do município': 'Vigilância Sanitária',
        'Encarregada de Apoio, a frente da recepção, do protocolo, da VISA (vigilância Sanitária)': 'Vigilância Sanitária',
        
        # Enfermagem e Técnicos de Enfermagem
        'Técnico de enfermagem': 'Enfermagem',
        'Tecnico de enfermagem': 'Enfermagem',
        'TECNICA DE ENFERMAGEM': 'Enfermagem',
        'Tecnica de enfermagem': 'Enfermagem',
        'Técnica em enfermagem': 'Enfermagem',
        'Técnica de Enfermagem': 'Enfermagem',
        'Técnico de enfermagem': 'Enfermagem',
        'Enfermeira- PAI DA VISA DSB': 'Enfermagem',
        'Enfermeira da area de ações.': 'Enfermagem',
        'Estagiária de Enfermagem': 'Enfermagem',
        
        # Ações e Serviços / Chefias
        'Chefe de ações ou serviços': 'Chefia de Ações e Serviços',
        'Chefia de ações e saúde': 'Chefia de Ações e Saúde',
        'Chefia de ações e serviços': 'Chefia de Ações e Serviços',
        'Chefia de vigilância epidemiológica': 'Chefia de Vigilância Epidemiológica',
        'Chefia de setor da vigilância Epidemiologia': 'Chefia de Vigilância Epidemiológica',
        'Chefe da Visa do Distrito Sanitário de Brotas': 'Chefia de Vigilância Epidemiológica',
        'AÇÕES E SERVIÇOS': 'Ações e Serviços',
        'Ações e Serviços': 'Ações e Serviços',
        'CHEFIA AÇÕES BÁSICAS': 'Chefia de Ações e Serviços',
        'Ações Básicas': 'Ações e Serviços',
        'Ações': 'Ações e Serviços',
        'Técnica ligada a chefia de ações': 'Ações e Serviços',
        'Area técnica  - Chefia de Ações e Serviços': 'Ações e Serviços',
        'Técnica vinculada a chefia de ações básicas': 'Ações e Serviços',
        'Técnico de referência de pastas na chefia de ações e serviços': 'Ações e Serviços',
        
        # Áreas Técnicas Especializadas
        'Referência técnica das ações de alimentação e nutrição': 'Área Técnica - Nutrição',
        'Técnico de referência programa de nutrição': 'Área Técnica - Nutrição',
        'Área técnica de alimentação e nutrição': 'Área Técnica - Nutrição',
        'Profissional integrada a saúde - Nutricionista.': 'Área Técnica - Nutrição',

        'Técnica de pasta de agravo': 'Área Técnica - Agravos',      

        'Técnica de referência': 'Área Técnica - Referência',  
        'Referência técnica': 'Área Técnica - Referência',
        
        'Referência técnica de saúde da mulher e ist': 'Área Técnica - Saúde da Mulher',
        'Referência técnica Saúde da mulher, curativos especiais, doença renal crônica e oncologia,territorialização': 'Área Técnica - Saúde da Mulher',
        
        'Técnica de refrência em Saúde da Criança, Saúde do Adolescentes, Saúde da Pessoa com Deficiência e Programa de Saúde na Escola': 'Área Técnica - Saúde da Criança/Adolescente',
        'Responsável pelas pastas: saúde do adolescente, saúde da pessoa com deficiência, doenças crônicas, telessaude e violência parte ações': 'Área Técnica - Programas Especiais',
        
        'Responsável técnica pelo programa saúde na escola e do adolescente': 'Área Técnica - PSE',
        'Responsável técnica de imunização': 'Área Técnica - Imunização',
        'referencia técnica de imunização': 'Área Técnica - Imunização',
        
        # Epidemiologia e Análise de Dados
        'Setor de Epidemiologia e Análise da Informação em Saúde': 'Epidemiologia e Análise de Dados',
        'Nugetes/ GT Plan': 'Epidemiologia e Análise de Dados',
        'NUGETS': 'Epidemiologia e Análise de Dados',
        'NUGETES': 'Epidemiologia e Análise de Dados',
        
        # Saúde Bucal
        'Dentista distrital': 'Saúde Bucal',
        'Odontóloga distrital': 'Saúde Bucal',
        'Apoiadora dos Dentistas.': 'Saúde Bucal',
        
        # Farmácia
        'Farmacêutico do distrito e do CAPS': 'Farmácia',
        'Farmacêutica Distrital': 'Farmácia',
        'Assistência Farmacêutica': 'Farmácia',
        'Assistência Farmacêutica Distrital': 'Farmácia',
        
        # Outras categorias específicas
        'Referência Técnica Curativos': 'Curativos Especiais',
        'Técnica de referência de curativos especiais': 'Curativos Especiais',
        
        'Área técnica tuberculose e vigilância em Saúde do Trabalhador': 'Vigilância Saúde do Trabalhador',
        'Área técnica de saúde da criança, adolescente, doenças crônica, tabagismo e PSE.': 'Área Técnica - Programas Especiais',
        
        'Referenccia de Investigação de òbitos Especiais': 'Vigilância do Óbito',
        'Sanitarista da Vigilância Epidemiológica- Vigilância do óbito': 'Vigilância do Óbito',
        
        'Sanitarista': 'Sanitarista',
        
        # TI e Suporte Técnico
        'TI do distrito': 'TI/Suporte Técnico',
        'Técnico de Informática': 'TI/Suporte Técnico',
        'NTI': 'TI/Suporte Técnico',
        'Centro de Processamento de Dados': 'TI/Suporte Técnico',
        
        # Serviços Gerais
        'Serviço gerais': 'Serviços Gerais',
        'Serviços Gerais': 'Serviços Gerais',
        'Motorista': 'Serviços Gerais',
        
        # Coordenação e Assistência
        'Assistente da coordenação': 'Coordenação/Assistência',
        'Subcoordenador Administrativo': 'Coordenação/Assistência',
        
        # Outros
        'POP ruas, Vacina, ações externas...': 'Ações Externas/Comunitárias',
        'Digitadora': 'Administrativo',
        'Técnico': 'Técnico',
        'Técnico Distrital': 'Técnico',
        'técnico distrital': 'Técnico',
        'referencia tecnica': 'Referência Técnica',
        'Técnica de Referência das pastas Rede Alyne / PICS e Saúde do Homem': 'Referência Técnica',
        'vigilancia': 'Vigilância',
        'Ouvidoria': 'Ouvidoria',
        'Sala de Imunização': 'Imunização',
        'Vacinação': 'Imunização',
        'Diretoria de Vigilância à Saúde': 'Diretoria',
        'Setor de Ações e Serviços de Saúde': 'Ações e Serviços',
        'Setor de Administração e Desenvolvimento de Pessoal': 'RH'
    }
    
    # LIMPAR ESPAÇOS EXTRAS ANTES DO MAPEAMENTO
    df['area_atuacao_limpa'] = df['area_atuacao'].str.strip()
    
    # Aplicar o mapeamento na coluna LIMPA
    df['area_atuacao_normalizada'] = df['area_atuacao_limpa'].map(mapeamento_areas)
    
    # Debug: ver quantos foram mapeados
    mapeados = df['area_atuacao_normalizada'].notna().sum()
    print(f"Valores mapeados: {mapeados}/{len(df)}")
    
    # IDENTIFICAR VALORES NÃO MAPEADOS
    nao_mapeados = df[df['area_atuacao_normalizada'].isna()]['area_atuacao_limpa'].unique()
    print(f"Valores não mapeados ({len(nao_mapeados)}):")
    for valor in nao_mapeados:
        print(f"  '{valor}'")
    
    # SUBSTITUIR a coluna original pela normalizada
    df['area_atuacao'] = df['area_atuacao_normalizada'].fillna(df['area_atuacao_limpa'])
    
    # Remover colunas temporárias
    df = df.drop(['area_atuacao_limpa', 'area_atuacao_normalizada'], axis=1)
    
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
    
    
    # Criar colunas binárias de forma mais robusta
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
    
    # Aplicar mapeamento às colunas específicas
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
            continue
            
        # 1. Criar versão numérica
        df[f'{coluna}_num'] = df[coluna].map(mapeamento_numerico)
        
        # 2. Substituir a coluna original pela versão ordenada
        ordem_categorias = ['Nenhum', '1', '2', '3 a 5', '6 ou mais', 'Não sei informar', 'Não se aplica']
        df[coluna] = pd.Categorical(
            df[coluna], 
            categories=ordem_categorias, 
            ordered=True
        )
        
        # 3. Criar categorias simplificadas
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

def criar_resumo_metas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria uma tabela resumo com metas distritais.
    """

    distritos = [
        "Brotas",
        "Cajazeiras",
        "Boca do Rio",
        "Itapuã",
        "São Caetano/Valéria",
        "Barra/Rio Vermelho",
        "Cabula/Beirú",
        "Subúrbio Ferroviário",
        "Pau da Lima",
        "Liberdade",
        "Itapagipe",
        "Centro Histórico"
    ]

    metas = {
        'Brotas': 50,
        'Cajazeiras': 40,
        'Boca do Rio': 32,
        'Itapuã': 43,
        'São Caetano/Valéria': 41,
        'Barra/Rio Vermelho': 34,
        'Cabula/Beirú': 39,
        'Subúrbio Ferroviário': 58,
        'Pau da Lima': 47,
        'Liberdade': 30,
        'Itapagipe': 42,
        'Centro Histórico': 45
    }

    resumo = []
    for distrito in distritos:
        meta = metas.get(distrito, None)
        resumo.append({
            'distrito': distrito,
            'meta_distrital': meta
        })
    
    resumo_df = pd.DataFrame(resumo)
    return resumo_df

def criar_tabelas_dimensao():
    # -------------------------------------------------------------------
    # CRIAÇÃO DAS TABELAS DE DIMENSÃO
    # -------------------------------------------------------------------
    logger.info("Criando tabelas de dimensão...")

    with open("tabelas_dimensionamento.json", "r", encoding="utf-8") as f:
        dims = json.load(f)["dimension_tables"]

    abas = {}
    for nome, conteudo in dims.items():
        df = pd.DataFrame(conteudo["data"])
        aba_nome = f"{nome}"
        abas[aba_nome] = df

    return abas

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

    logger.info("Transformação concluída.")
    return df

# -------------------------------------------------------------------
# LOAD – CRIA ABA E ESCREVE DADOS NA MESMA PLANILHA
# -------------------------------------------------------------------
def load_to_sheet(client, sheet_id: str, df: pd.DataFrame, new_tab: str = "DadosEtl"):
    logger.info(f"Criando aba '{new_tab}' na planilha...")

    sh = client.open_by_key(sheet_id)

    try:
        existing = sh.worksheet(new_tab)
        sh.del_worksheet(existing)
        logger.info("Aba existente encontrada → removida.")
    except gspread.exceptions.WorksheetNotFound:
        pass

    ws = sh.add_worksheet(title=new_tab, rows=str(len(df) + 5), cols=str(len(df.columns) + 5))

    # Preparar dados
    df_preparado = df.copy()
    
    for col in df_preparado.columns:
        if df_preparado[col].dtype.name == 'category':
            df_preparado[col] = df_preparado[col].astype(str)
    
    df_preparado = df_preparado.fillna('')

    # 🔥 ENVIAR DADOS UMA ÚNICA VEZ
    values = [df_preparado.columns.tolist()] + df_preparado.values.tolist()
    ws.update(values)

    # 🔥 FORMATAR COMO TABELA COMPLETA
    try:
        # 1. Congelar primeira linha
        ws.freeze(rows=1)
        
        logger.info(f"Aba '{new_tab}' formatada como tabela completa.")
        
    except Exception as e:
        logger.warning(f"Não foi possível aplicar formatação completa: {e}")

    logger.info(f"Aba '{new_tab}' criada com sucesso.")

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    SHEET_ID = "1GEFCBgoE5ed9yrEjAYe-FbWwXsIfc5G9mU2Gi0Yu_Kw"
    TAB = "BaseBruta"
    NEW_TAB = "DadosEtl"

    df, client = extract(SHEET_ID, TAB)
    df = transform(df)
    
    # 🔥 USAR load_to_sheet PARA TODAS AS ABAS
    load_to_sheet(client, SHEET_ID, df, NEW_TAB)

    # Criar e carregar resumos usando load_to_sheet
    resumo_df = criar_resumo_sistemas(df)
    load_to_sheet(client, SHEET_ID, resumo_df, "ResumoSistemas")

    resumo_metas_df = criar_resumo_metas(df)
    load_to_sheet(client, SHEET_ID, resumo_metas_df, "ResumoMetas")

    # 🔥 CARREGAR TABELAS DE DIMENSÃO COM load_to_sheet E DELAYS
    try:
        dim_abas = criar_tabelas_dimensao()
        for i, (aba_nome, dim_df) in enumerate(dim_abas.items()):
            load_to_sheet(client, SHEET_ID, dim_df, aba_nome)
            
            # Delay entre cada tabela de dimensão (exceto a última)
            if i < len(dim_abas) - 1:
                time.sleep(5)
                logger.info(f"Delay aplicado após criar {aba_nome}")
                
    except Exception as e:
        logger.error(f"Erro ao criar abas de dimensão: {e}")
    
    logger.info("ETL COMPLETO! Todas as abas formatadas como tabelas.")


if __name__ == "__main__":
    main()