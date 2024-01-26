### INCIANDO O SCRIPT ###
from config import *
from getToken import *
from functions import *
from variables import *
import traceback
import pandas as pd
import datetime
import pymssql
infos = info()
pd.options.mode.chained_assignment = None


# dados do json
base = infos['BaseBanco']
server = str(infos['ServidorBanco'])
username = str(infos['usernamebanco'])
password = str(infos['passwordbanco'])

# credenciais de acesso do Portal do Azure
user_id, tenant_id, client_id, client_credential = check_credentials()
data_ini = data_hora_atual()
registrar_print("FLOW FUNCTIONS REALIZADO COM SUCESSO")


# OBS: Verifique no script Rascunho_RPA como acessar os diretorios
try:
    # acessando a pasta do one drive para pegar os ids
    # PASSAR OS NOMES DAS PASTAS NA ORDEM QUE ESTÁ NO ONEDRIVE
    capturar_id('Pasta 1', 'Pasta 2', 'Pasta 3')

    # criando a pasta log
    criar_pasta_log(user_id, pasta_id=id_pasta_output[0])
    registrar_print("INICIOU FLOW MAIN")

    # Acessar o arquivo config.xlsx na pasta input caso precisso ler variaveis
    # apos a leitura ele irá printar o id de cada arquivo dentro da pasta input
    # listando os arquivos na pasta Input
    config = listar_conteudo_pasta(user_id, id_pasta_input[0])

    # ler csv, xlsx, pdf ou txt - se for csv, passar o delimitador
    value = ler_arquivo(user_id, config['ID'][0], format_, header=0)

    # caso precise exportar o df
    # exportando dataframe na pasta output de exemplo
    extensao = 'csv'  # escolha a extensão que precisar
    nome = 'jan_2023'  # nome do arquivo para exportar
    exportar_df(
        user_id, pasta_id=id_pasta_log[0], arquivo='''variavel_do_df_aqui''', extensao_arquivo=extensao, nome=nome)

    # caso queira baixar um arquivo da internet
    url_arquivo = 'minha_url_aqui'
    nome_arquivo = 'nome_do_arquivo'
    extensao_arquivo = 'zip'
    baixar_arquivo_online(user_id, id_pasta_log[0], url_arquivo,
                          nome_arquivo, extensao_arquivo)
    ##### desenvolva seu trabalho abaixo, depois remova o comentário #####

    # INJETANDO OS DADOS NO BANCO
    registrar_print('INSERINDO OS DADOS NO BANCO')

    # Conectando ao banco de dados
    conn = pymssql.connect(server=server, user=username,
                           password=password, database=base)

    # Criando um cursor para executar as operações no banco de dados
    cursor = conn.cursor()

    # Verificando se a tabela já existe
    tabela = 'nome_da_tabela_bd'
    table_exists_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tabela}'"
    cursor.execute(table_exists_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # AQUI VOCE COLOCA A ESTRUTURA DE SUA TABELA DO BANCO
        # Criando a tabela Tabela1736_INPC caso não exista
        create_table_query = f"""
                  CREATE TABLE [dbo].{tabela}(
                  [NOME] [varchar](100) NULL,
                  [DATA_HORA_CARGA] [smalldatetime] NULL
            ) ON [PRIMARY]    
            ALTER TABLE [dbo].{tabela} ADD  DEFAULT (getdate()) FOR [DATA_HORA_CARGA]            
            """
        cursor.execute(create_table_query)
        print(f"Tabela {tabela} criada com sucesso.")
    else:
        print(f"Tabela {tabela} já existe no banco de dados.")

    # Limpando as tabelas
    cursor.execute(f'TRUNCATE TABLE {tabela}')

    # Inserindo os dados do DataFrame database na tabela tabela1736
    # data e hora que iniciou
    data_ini_ = data_hora_atual()
    print('iniciou inserção na tabela: ', data_ini_)

    # SIMULANDO UM DATAFRAME, SUBSTITUA PELO SEU DATAFRAME FINAL, JA COM OS DADOS TRATADOS
    df_inpc = pd.DataFrame()

    # Inserindo os dados
    # SUBSTITUA A ESTRUTURA ABAIXO PELO SEU DF E TABELA
    for index, row in df_inpc.iterrows():
        cursor.execute(f"INSERT INTO {tabela} (NOME, NIVEL_TERRITORIAL, DATA_HORA_CARGA) \
            VALUES (%s, %s)", (row['nome'], row['data_hora_carga']))

    # data e hora que finalizou
    data_fim = datetime.datetime.now()
    print('finalizou a inserção na tabela: ', data_fim)

    conn.commit()  # commitando as mudanças
    conn.close()  # Fechando a conexão com o banco de dados
    registrar_print("DADOS INJETADOS COM SUCESSO.")

    # criando o txt log dentro da pasta log
    criar_log(user_id, pasta_id=id_pasta_log[0], lista_prints=lista_prints)
except Exception as e:
    traceback_str = traceback.format_exc()
    registrar_print(f"Ocorreu um erro:\n{traceback_str}, {data_ini}, {e}")
    enviar_email(lista_prints, lista_prints[0])
    criar_log(user_id, pasta_id=id_pasta_log[0], lista_prints=lista_prints)
