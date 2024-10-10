from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from google.auth.transport import requests as g_requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

import requests
import json
import pandas as pd
import time
import aiohttp
import asyncio
import logging
import os
import pendulum
from datetime import datetime, timedelta


raw_path_file = "/home/airflow/raw/"
project_id = "logical-essence-433414-r5"
escolas_data = []

today = datetime.now()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")


destination_path_original = f"raw/flatfile/original/{year}/{month}/{day}/"


key_path = "/home/airflow/logical-essence-433414-r5-6b6fcca101ce.json"



credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

def connectionGoogleBigQuery():

    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client_bq


def uploadFileGCPBucket(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    key_path = "/home/airflow/logical-essence-433414-r5-6b6fcca101ce.json"

    storage_client = storage.Client.from_service_account_json(json_credentials_path=key_path)

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    return "OK"

def delete_files(path):

    try:
        if not os.path.isdir(path):
            print(f"O diretório {path} não existe")

        for arquivo in os.listdir(path):
            caminho_arquivo = os.path.join(path, arquivo)

            if os.path.isfile(caminho_arquivo):
                os.remove(caminho_arquivo)
                print(f'Arquivo {arquivo} apagado.')
    except Exception as delete_file_error:
        logging.error(f"An error occurred: {delete_file_error}")
        raise delete_file_error


async def fetch(session, url):
    while True:
        try:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    return await response.json()
        except asyncio.TimeoutError:
            await asyncio.sleep(5)


# fetch para a segunda API
async def fetch_data(session, ano, dep):
    url = f"https://cors-anywhere.herokuapp.com/https://qedu.org.br/api/v1/infra/35/comparativo?dependencia_id={dep}&ano={ano}&localizacao_id=0&oferta_id=0"
    
    headers = {
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://qedu.org.br/uf/35-sao-paulo/censo-escolar/infraestrutura',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0'
    }
    
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        return data

# Processa os dados da segunda API
async def process_data(ano, dep):
    async with aiohttp.ClientSession() as session:
        data = await fetch_data(session, ano, dep)
        for i in range(len(data)):
            section = data[i]['section']
            #print(section)
            items = data[i]['items']
            for i in range(len(items)):
                label = items[i]['label']
                #print(label)
                values = items[i]['values']
                for i in range(len(values)):
                    entidade = values[i]['entidade']
                    value = values[i]['value']
                    escolas = values[i]['escolas']
                    slug = values[i]['slug']
                    qtd_escolas = values[i]['qtd_escolas']
                    tipo_escolas = 'Privada' if dep == 4 else 'Pública'

                    escolas_data.append([section, label, entidade, value, escolas, slug, qtd_escolas, ano, tipo_escolas])
                




async def api_to_parquet():
    parquet_file_escolas = raw_path_file + "escolas.parquet"
    parquet_file_estatisticas = raw_path_file + "estatisticas.parquet"
    parquet_file_infraestrutura = raw_path_file + "infraestrutura.parquet"
    
    cidades_url = "http://educacao.dadosabertosbr.org/api/cidades/sp"

    async with aiohttp.ClientSession() as session:

        ####################################################
        # EXTRAÍNDO OS DADOS DADOS ESCOLAS DO ESTADO DE SP #
        ####################################################

        # Requisição para obter cidades
        cidades_data = await fetch(session, cidades_url)
        cidades_id = []
        for row in cidades_data:
            if row[0:7] != '3550308':
                cidades_id.append(row[0:7])

        print(cidades_id)
        print('cidades id')

        escolas_id = []
        escolas_cidade_sp_url = [
            f"http://educacao.dadosabertosbr.org/api/escolas/buscaavancada?situacaoFuncionamento=1&estado=SP&enemMin={min}&enemMax={max}&cidade=3550308"
            for min, max in [(1, 507), (507, 530), (530, 551), (551, 566), (566, 585), (585, 627), (627, 999)]
        ]

        # Requisições para a cidade de São Paulo
        sp_urls = [fetch(session, url) for url in escolas_cidade_sp_url]
        print(sp_urls)
        sp_responses = await asyncio.gather(*sp_urls)

        for data in sp_responses:
            for escola in data[1]:
                escolas_id.append(escola['cod'])
                print(escola['cod'])
                print('escolas codigo')


        # Requisições para o resto do estado
        estado_tasks = []
        for id in cidades_id:
            url = f"http://educacao.dadosabertosbr.org/api/escolas/buscaavancada?situacaoFuncionamento=1&estado=SP&enemMin=1&cidade={id}"
            estado_tasks.append(fetch(session, url))

        estado_responses = await asyncio.gather(*estado_tasks)

        for data in estado_responses:
            if data[0] != 0:
                for escola in data[1]:
                    print(escola['cod'])
                    escolas_id.append(escola['cod'])

        print(escolas_id)

        # Requisições detalhadas para cada escola
        escola_tasks = []
        for id in escolas_id:
            url = f"http://educacao.dadosabertosbr.org/api/escola/{id}"
            print(url)
            escola_tasks.append(fetch(session, url))

        escolas_responses = await asyncio.gather(*escola_tasks)

        escolas = []
        for data in escolas_responses:
            print(data)
            escolas.append(data)

        # Criando DataFrame
        escolas_df = pd.DataFrame(escolas)

        column_name = { "cod": "cod", "anoCenso": "ano_censo", "nome": "nome", "situacaoFuncionamento": "situacao_funcionamento", "situacaoCenso": "situacao_censo", "inicioAno": "inicio_ano", "fimAno": "fim_ano", "codUf": "cod_uf", "siglaUf": "sigla_uf", "codMunicipio": "cod_municipio", "nomeMunicipio": "nome_municipio", "codDistrito": "cod_distrito", "nomeDistrito": "nome_distrito", "regiao": "regiao", "dependenciaAdministrativa": "dependencia_administrativa", "tipoLocalizacao": "tipo_localizacao", "regulamentada": "regulamentada", "aguaFiltrada": "agua_filtrada", "aguaPublica": "agua_publica", "aguaPocoArtesiano": "agua_poco_artesiano", "aguaCacimba": "agua_cacimba", "aguaRio": "agua_rio", "aguaInexistente": "agua_inexistente", "energiaPublica": "energia_publica", "energiaGerador":"energia_gerador", "energiaOutros": "energia_outros", "energiaInexistente": "energia_inexistente", "esgotoPublico": "esgoto_publico", "esgotoFossa": "esgoto_fossa", "esgotoInexistente": "esgoto_inexistente", "lixoColetaPeriodica":"lixo_coleta_periodica", "lixoQueima": "lixo_queima", "lixoJogaOutraArea": "lixo_joga_outra_area", "lixoRecicla": "lixo_recicla", "lixoEnterra": "lixo_enterra", "lixoOutros": "lixo_outros", "salaDiretoria": "sala_diretoria", "salaProfessores": "sala_professores", "laboratorioInformatica": "laboratorio_informatica", "laboratorioCiencias": "laboratorio_ciencias", "atendimentoEspecial": "atendimento_especial", "quadraCoberta": "quadra_coberta", "quadraDescoberta": "quadra_descoberta", "cozinha": "cozinha", "biblioteca": "biblioteca", "salaLeitura": "sala_leitura", "parqueInfantil": "parque_infantil", "bercario": "bercario", "sanitarioForaPredio": "sanitario_fora_predio", "sanitarioDentroPredio": "sanitario_dentro_predio", "sanitarioEducInfant": "sanitario_educ_infant", "sanitarioPNE": "sanitario_pne", "dependenciasPNE": "dependencias_pne", "secretaria": "secretaria", "banheiroChuveiro": "banheiro_chuveiro", "refeitorio": "refeitorio", "despensa": "despensa", "almoxarifado": "almoxarifado", "auditorio": "auditorio", "patioCoberto": "patio_coberto", "patioDescoberto": "patio_descoberto", "alojamentoAluno": "alojamento_aluno", "alojamentoProfessor": "alojamento_professor", "areaVerde": "area_verde", "lavanderia": "lavanderia", "salasExistentes": "salas_existentes", "salasUtilizadas": "salas_utilizadas", "televisores": "televisores", "videoCassetes": "video_cassetes", "dvds": "dvds", "parabolicas": "parabolicas", "copiadoras": "copiadoras", "retroprojetores": "retroprojetores", "impressoras": "impressoras", "aparelhosSom": "aparelhos_som", "datashows": "data_shows", "fax": "fax", "foto": "foto", "computadores": "computadores", "computadoresAdm": "computadores_adm", "computadoresAlunos": "computadores_alunos", "internet": "internet", "bandaLarga": "banda_larga", "funcionarios": "funcionarios", "alimentacao": "alimentacao", "aee": "aee", "atividadeComplementar": "atividade_complementar", "ensinoRegular": "ensino_regular", "regCreche": "reg_creche", "regPreescola": "reg_preescola", "regFundamental8": "reg_fundamental8", "regFundamental9": "reg_fundamental19", "regMedioMedio": "reg_medio_medio", "regMedioIntegrado": "reg_medio_integrado", "regMedioNormal": "reg_medio_normal", "regMedioProfissional": "reg_medio_profissional", "ensinoEspecial": "ensino_especial", "espCreche": "esp_creche", "espPreescola": "esp_preescola", "espFundamental8": "esp_fundamental8", "espFundamental9": "esp_fundamental9", "espMedioMedio": "esp_medio_medio", "espMedioIntegrado": "esp_medio_integrado", "espMedioNormal": "esp_medio_normal", "espMedioProfissional": "esp_medio_profissional", "espEjaFundamental": "esp_eja_fundamental", "espEjaMedio": "esp_eja_medio", "ensinoEja": "ensino_eja", "ejaFundamental": "eja_fundamental", "ejaMedio": "eja_medio", "ejaProjovem": "eja_pro_jovem", "ciclos": "ciclos", "fimDeSemana": "fim_de_semana", "pedagogiaAlternancia": "pedagogia_alternancia", "idebAI": "ideb_ai", "idebAF": "ideb_af", "enemPortugues": "enem_portugues", "enemMatematica": "enem_matematica", "enemHumanas": "enem_humanas", "enemNaturais": "enem_naturais", "enemRedacao": "enem_redacao", "enemMediaObjetiva": "enem_media_objetiva", "enemMediaGeral": "enem_media_geral", "socioEconomico": "socio_economico", "formacaoDocente": "formacao_docente", "endereco": "endereco", "latitude": "latitude", "longitude": "longitude", "nomeTitulo": "nome_titulo", "situacaoFuncionamentoTxt": "situacao_funcionamento_txt", "inicioAnoTxt": "inicio_ano_txt", "fimAnoTxt": "fim_ano_txt", "dependenciaAdministrativaTxt": "dependencia_administrativa_txt", "tipoLocalizacaoTxt": "tipo_localizacao_txt", "regulamentadaTxt": "regulamentada_txt" }
        escolas_df = escolas_df.rename(columns=column_name)

        dtypes = { 'cod': 'string', 'ano_censo': 'string', 'nome': 'string', 'situacao_funcionamento': 'string', 'situacao_censo': 'string', 'inicio_ano': 'string', 'fim_ano': 'string', 'cod_uf': 'string', 'sigla_uf': 'string', 'nome_municipio': 'string', 'cod_municipio': 'string', 'cod_distrito': 'string', 'nome_distrito': 'string', 'regiao': 'string', 'dependencia_administrativa': 'string', 'tipo_localizacao': 'string', 'regulamentada': 'string', 'agua_filtrada': 'string', 'agua_publica': 'string', 'agua_poco_artesiano': 'string', 'agua_cacimba': 'string', 'agua_rio': 'string', 'agua_inexistente': 'string', 'energia_publica':'string', 'energia_gerador': 'string', 'energia_outros': 'string', 'energia_inexistente': 'string', 'esgoto_publico': 'string', 'esgoto_fossa': 'string', 'esgoto_inexistente': 'string', 'lixo_coleta_periodica': 'string', 'lixo_queima': 'string', 'lixo_joga_outra_area': 'string', 'lixo_recicla': 'string', 'lixo_enterra': 'string', 'lixo_outros': 'string', 'sala_diretoria': 'string', 'sala_professores': 'string', 'laboratorio_informatica': 'string', 'laboratorio_ciencias': 'string', 'atendimento_especial': 'string', 'quadra_coberta': 'string', 'quadra_descoberta': 'string', 'cozinha': 'string', 'biblioteca': 'string', 'sala_leitura': 'string', 'parque_infantil': 'string', 'bercario': 'string', 'sanitario_fora_predio': 'string', 'sanitario_dentro_predio': 'string', 'sanitario_educ_infant': 'string', 'sanitario_pne': 'string', 'dependencias_pne': 'string', 'secretaria': 'string', 'banheiro_chuveiro': 'string', 'refeitorio': 'string', 'despensa': 'string', 'almoxarifado': 'string', 'auditorio': 'string', 'patio_coberto': 'string', 'patio_descoberto': 'string', 'alojamento_aluno': 'string', 'alojamento_professor': 'string', 'area_verde': 'string', 'lavanderia': 'string', 'salas_existentes': 'string', 'salas_utilizadas': 'string', 'televisores': 'string', 'video_cassetes': 'string', 'dvds': 'string', 'parabolicas': 'string', 'copiadoras': 'string', 'retroprojetores': 'string', 'impressoras': 'string', 'aparelhos_som': 'string', 'data_shows': 'string', 'fax': 'string', 'foto': 'string', 'computadores': 'string', 'computadores_adm': 'string', 'computadores_alunos': 'string', 'internet': 'string', 'banda_larga': 'string', 'funcionarios': 'string', 'alimentacao': 'string', 'aee': 'string', 'atividade_complementar': 'string', 'ensino_regular': 'string', 'reg_creche': 'string', 'reg_preescola': 'string', 'reg_fundamental8': 'string', 'reg_fundamental19': 'string', 'reg_medio_medio': 'string', 'reg_medio_integrado': 'string', 'reg_medio_normal': 'string', 'reg_medio_profissional': 'string', 'ensino_especial': 'string', 'esp_creche': 'string', 'esp_preescola': 'string', 'esp_fundamental8': 'string', 'esp_fundamental9': 'string', 'esp_medio_medio': 'string', 'esp_medio_integrado': 'string', 'esp_medio_normal': 'string', 'esp_medio_profissional': 'string', 'esp_eja_fundamental': 'string', 'esp_eja_medio': 'string', 'ensino_eja': 'string', 'eja_fundamental': 'string', 'eja_medio': 'string', 'eja_pro_jovem': 'string', 'ciclos': 'string', 'fim_de_semana': 'string', 'pedagogia_alternancia': 'string', 'ideb_ai': 'string', 'ideb_af': 'string', 'enem_portugues': 'string', 'enem_matematica': 'string', 'enem_humanas': 'string', 'enem_naturais': 'string', 'enem_redacao': 'string', 'enem_media_objetiva': 'string', 'enem_media_geral': 'string', 'socio_economico': 'string', 'formacao_docente': 'string', 'endereco': 'string', 'latitude': 'string', 'longitude': 'string', 'nome_titulo': 'string', 'situacao_funcionamento_txt': 'string', 'inicio_ano_txt': 'string', 'fim_ano_txt': 'string', 'dependencia_administrativa_txt': 'string', 'tipo_localizacao_txt': 'string', 'regulamentada_txt': 'string' }
        escolas_df = escolas_df.astype(dtypes)

        escolas_df.to_parquet(parquet_file_escolas, engine="pyarrow")
        #escolas_df.to_csv(parquet_file_escolas, sep=";", index=False)

        ######################################################
        # EXTRAÍNDO AS ESTATISTITCAS DOS ESTADOS BRASILEIROS #
        ######################################################
        estatisticas = []

        estados_url = [
            f"http://educacao.dadosabertosbr.org/api/estatisticas?tipoLocal=EST&nomeLocal={sigla}&codMunicipio=0&indice=3"
            for sigla in ['SP', 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SE', 'TO']
        ]

        # Requisições das stats para todos os estados
        estados_urls = [fetch(session, url) for url in estados_url]
        print(estados_urls)
        estados_responses = await asyncio.gather(*estados_urls)

        for data in estados_responses:
            estatisticas.append(data)
            print(data)
        print(len(estatisticas))

        estatisticas_df = pd.DataFrame(estatisticas)

        column_name = { "id": "id", "ano": "ano",  "tipoLocal": "tipo_local", "nomeLocal": "nome_local", "codMunicipio": "cod_municipio", "melhores": "melhores", "situacaoFuncionamentoAtividade": "situacao_funcionamento_atividade", "situacaoFuncionamentoParalisada": "situacao_funcionamento_paralisada", "situacaoFuncionamentoExtinta": "situacao_funcionamento_extinta", "situacaoFuncionamentoExtintaAnoAnterior": "situacao_funcionamento_extinta_ano_anterior", "situacaoFuncionamentoNaoInformado": "situacao_funcionamento_nao_informado", "dependenciaAdministrativaFederal": "dependencia_administrativa_federal", "dependenciaAdministrativaEstadual": "dependencia_administrativa_estadual", "dependenciaAdministrativaMunicipal": "dependencia_administrativa_municipal", "dependenciaAdministrativaPrivada": "dependencia_administrativa_privada", "tipoLocalizacaoRural": "tipo_localizacao_rural", "tipoLocalizacaoUrbana": "tipo_localizacao_urbana", "regulamentadaSim": "regulamentada_sim", "regulamentadaNao": "regulamentada_nao", "regulamentadaTramitacao": "regulamentada_tramitacao", "aguaFiltrada": "agua_filtrada", "aguaPublica": "agua_publica", "aguaPocoArtesiano": "agua_poco_artesiano", "aguaCacimba": "agua_cacimba", "aguaRio": "agua_rio", "aguaInexistente": "agua_inexistente", "aguaNaoInformado": "agua_nao_informado", "energiaPublica": "energia_publica", "energiaGerador": "energia_gerador", "energiaOutros": "energia_outros", "energiaInexistente": "energia_inexistente", "energiaNaoInformado": "energia_nao_informado", "esgotoPublico": "esgoto_publico", "esgotoFossa": "esgoto_fossa", "esgotoInexistente": "esgoto_inexistente", "esgotoNaoInformado": "esgoto_nao_informado", "lixoColetaPeriodica": "lixo_coleta_periodica", "lixoQueima": "lixo_queima", "lixoJogaOutraArea": "lixo_joga_outra_area", "lixoRecicla": "lixo_recicla", "lixoEnterra": "lixo_enterra", "lixoOutros": "lixo_outros", "lixoNaoInformado": "lixo_nao_informado", "salaDiretoria": "sala_diretoria", "salaProfessores": "sala_professores", "laboratorioInformatica": "laboratorio_informatica", "laboratorioCiencias": "laboratorio_ciencias", "atendimentoEspecial": "atendimento_especial", "quadraCoberta": "quadra_coberta", "quadraDescoberta": "quadra_descoberta", "cozinha": "cozinha", "biblioteca": "biblioteca", "salaLeitura": "sala_leitura", "parqueInfantil": "parque_infantil", "bercario": "bercario", "sanitarioForaPredio": "sanitario_fora_predio", "sanitarioDentroPredio": "sanitario_dentro_predio", "sanitarioEducInfant": "sanitario_educ_infant", "sanitarioPNE": "sanitario_pne", "dependenciasPNE": "dependencias_pne", "secretaria": "secretaria", "banheiroChuveiro": "banheiro_chuveiro", "refeitorio": "refeitorio", "despensa": "despensa", "almoxarifado": "almoxarifado", "auditorio": "auditorio", "patioCoberto": "patio_coberto", "patioDescoberto": "patio_descoberto", "alojamentoAluno": "alojamento_aluno", "alojamentoProfessor": "alojamento_professor", "areaVerde": "area_verde", "lavanderia": "lavanderia", "salasExistentes": "salas_existentes", "salasUtilizadas": "salas_utilizadas", "televisores": "televisores", "videoCassetes": "video_cassetes", "dvds": "dvds", "parabolicas": "parabolicas", "copiadoras": "copiadoras", "retroprojetores": "retroprojetores", "impressoras": "impressoras", "aparelhosSom": "aparelhos_som", "datashows": "datashows", "fax": "fax", "foto": "foto", "computadores": "computadores", "computadoresAdm": "computadores_adm", "computadoresAlunos": "computadores_alunos", "internet": "internet", "bandaLarga": "banda_larga", "idebAI": "ideb_ai", "idebAF": "ideb_af", "enemPortugues": "enem_portugues", "enemMatematica": "enem_matematica", "enemHumanas": "enem_humanas", "enemNaturais": "enem_naturais", "enemRedacao": "enem_redacao", "enemMediaObjetiva": "enem_media_objetiva", "enemMediaGeral": "enem_media_geral", "formacaoDocente": "formacao_docente"}
        estatisticas_df = estatisticas_df.rename(columns=column_name)

        dtypes_stats = { 'id': 'string', 'ano': 'string', 'tipo_local': 'string', 'nome_local': 'string', 'cod_municipio': 'string', 'melhores': 'string', 'situacao_funcionamento_atividade': 'string', 'situacao_funcionamento_paralisada': 'string', 'situacao_funcionamento_extinta': 'string', 'situacao_funcionamento_extinta_ano_anterior': 'string', 'situacao_funcionamento_nao_informado': 'string', 'dependencia_administrativa_federal': 'string', 'dependencia_administrativa_estadual': 'string', 'dependencia_administrativa_municipal': 'string', 'dependencia_administrativa_privada': 'string', 'tipo_localizacao_rural': 'string', 'tipo_localizacao_urbana': 'string', 'regulamentada_sim': 'string', 'regulamentada_nao': 'string', 'regulamentada_tramitacao': 'string', 'agua_filtrada': 'string', 'agua_publica': 'string', 'agua_poco_artesiano': 'string', 'agua_cacimba': 'string', 'agua_rio': 'string', 'agua_inexistente': 'string', 'agua_nao_informado': 'string', 'energia_publica': 'string', 'energia_gerador': 'string', 'energia_outros': 'string', 'energia_inexistente': 'string', 'energia_nao_informado': 'string', 'esgoto_publico': 'string', 'esgoto_fossa': 'string', 'esgoto_inexistente': 'string', 'esgoto_nao_informado': 'string', 'lixo_coleta_periodica': 'string', 'lixo_queima': 'string', 'lixo_joga_outra_area': 'string', 'lixo_recicla': 'string', 'lixo_enterra': 'string', 'lixo_outros': 'string', 'lixo_nao_informado': 'string', 'sala_diretoria': 'string', 'sala_professores': 'string', 'laboratorio_informatica': 'string', 'laboratorio_ciencias': 'string', 'atendimento_especial': 'string', 'quadra_coberta': 'string', 'quadra_descoberta': 'string', 'cozinha': 'string', 'biblioteca': 'string', 'sala_leitura': 'string', 'parque_infantil': 'string', 'bercario': 'string', 'sanitario_fora_predio': 'string', 'sanitario_dentro_predio': 'string', 'sanitario_educ_infant': 'string', 'sanitario_pne': 'string', 'dependencias_pne': 'string', 'secretaria': 'string', 'banheiro_chuveiro': 'string', 'refeitorio': 'string', 'despensa': 'string', 'almoxarifado': 'string', 'auditorio': 'string', 'patio_coberto': 'string', 'patio_descoberto': 'string', 'alojamento_aluno': 'string', 'alojamento_professor': 'string', 'area_verde': 'string', 'lavanderia': 'string', 'salas_existentes': 'string', 'salas_utilizadas': 'string', 'televisores': 'string', 'video_cassetes': 'string', 'dvds': 'string', 'parabolicas': 'string', 'copiadoras': 'string', 'retroprojetores': 'string', 'impressoras': 'string', 'aparelhos_som': 'string', 'datashows': 'string', 'fax': 'string', 'foto': 'string', 'computadores': 'string', 'computadores_adm': 'string', 'computadores_alunos': 'string', 'internet': 'string', 'banda_larga': 'string', 'ideb_ai': 'string', 'ideb_af': 'string', 'enem_portugues': 'string', 'enem_matematica': 'string', 'enem_humanas': 'string', 'enem_naturais': 'string', 'enem_redacao': 'string', 'enem_media_objetiva': 'string', 'enem_media_geral': 'string', 'formacao_docente': 'string' }
        estatisticas_df = estatisticas_df.astype(dtypes_stats)

        #estatisticas_df.to_csv(parquet_file_estatisticas, sep=";", index=False)
        estatisticas_df.to_parquet(parquet_file_estatisticas, engine="pyarrow")

        ####################################################
        # EXTRAÍNDO DADOS DE INFRAESTRUTURA DE SP E BRASIL #
        ####################################################
        options = Options()
        options.add_argument('--headless')  # Modo headless
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')  # Desabilitar GPU

        # Criar o serviço do ChromeDriver
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)

        # Abre a URL
        driver.get("https://cors-anywhere.herokuapp.com/corsdemo")

        # Espera um pouco para garantir que a página carregue
        time.sleep(5)

        # Encontra o botão pelo seu seletor
        button = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]')

        # Clica no botão
        button.click()

        # Espera um pouco para ver o resultado
        time.sleep(5)

        anos = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
        infra_tasks = []
        for ano in anos:
            dependencias = [4, 5]
            for dep in dependencias:
                infra_tasks.append(process_data(ano, dep))
        await asyncio.gather(*infra_tasks)


        df_infra = pd.DataFrame({"secao": [i[0] for i in escolas_data],
                           "tema": [i[1] for i in escolas_data],
                           "entidade": [i[2] for i in escolas_data],
                           "porcentagem": [i[3] for i in escolas_data],
                           "escolas": [i[4] for i in escolas_data],
                           "sigla_uf": [i[5] for i in escolas_data],
                           "total_escolas": [i[6] for i in escolas_data],
                           "ano": [i[7] for i in escolas_data],
                           "tipo_escolas": [i[8] for i in escolas_data]})

        infra_dtypes = {"secao": "string", "entidade": "string", "porcentagem": "string", "escolas": "string", "sigla_uf": "string", "total_escolas": "string", "ano": "string", "tipo_escolas": "string" }
        df_infra = df_infra.astype(infra_dtypes)

        df_infra.to_parquet(parquet_file_infraestrutura, engine="pyarrow")

        driver.quit()


def load_bigquery(bigquery_dataset_tabela, parquet_file):

    client_bq = connectionGoogleBigQuery()

    bigquery_dataset_land = 'LAND_FLATFILE'

    table_ref = client_bq.dataset(bigquery_dataset_land).table(bigquery_dataset_tabela)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    path_raw_parquet = "/home/airflow/raw/" + parquet_file

    with open(path_raw_parquet, "rb") as source_file:
        job = client_bq.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    job.result()

    try:
        logging.info(f"Updating table {bigquery_dataset_land}.{bigquery_dataset_tabela}.")
        query_update = ("UPDATE " + project_id + "." + bigquery_dataset_land + "." + bigquery_dataset_tabela + " SET partition_time = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR) WHERE partition_time is null ;")
        client_bq.query(query_update)
        time.sleep(15)
        logging.info(f"Successfully updated table {bigquery_dataset_land}.{bigquery_dataset_tabela}.")
    except Exception as update_error:
        logging.error(f"An error has occured during UPDATE process in {bigquery_dataset_land}.{bigquery_dataset_tabela}. Error: {update_error}")
        raise update_error

    bucket_name = "api-datalake"
    path_raw_local = "/home/airflow/raw/" + parquet_file
    destination_blob_name = destination_path_original + parquet_file
    uploadFileGCPBucket(bucket_name, path_raw_local, destination_blob_name)




def run():
    # Executar a função principal
    asyncio.run(api_to_parquet())
    load_bigquery('api_escolas', 'escolas.parquet')
    load_bigquery('api_estatisticas', 'estatisticas.parquet')
    load_bigquery('api_infraestrutura', 'infraestrutura.parquet')
    delete_files(raw_path_file)


with DAG(
        dag_id="extract_and_load_api_to_bq",
        schedule_interval='@daily',
        start_date=pendulum.datetime(2024, 9, 23, tz="America/Sao_Paulo"),
        catchup=False,
        is_paused_upon_creation=True,
        tags=["Extração e Carregamento", "api"]
) as dag:
    
    start_task = DummyOperator(
             task_id='start_task'
    )

    task = PythonOperator(
            task_id='extract_and_load_api_to_bq',
            python_callable=run,
            provide_context=True,
            retries=0,
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='dataform_execution',
    )

    end_task = DummyOperator(
        task_id='end_task'
    )

    start_task >> task >> trigger_task >> end_task