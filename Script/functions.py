### INCIANDO O SCRIPT ###
from variables import *
from config import *
from email.mime.text import MIMEText
import smtplib
from getToken import *
import requests
import pandas as pd
from io import BytesIO
import msal
import tabula
infos = info()
pd.options.mode.chained_assignment = None  # ignorar mensagens do pandas


# dados do json
email = infos['Email']
senha = infos['PasswordEmail']


# Obter as credenciais usando a função get_credentials
user_id, tenant_id, client_id, client_credential = check_credentials()
registrar_print("INICIOU O FLOW FUNCTIONS")


# função para obter o token do portal azure
def token():
    authority_url = f'https://login.microsoftonline.com/{tenant_id}'
    app = msal.ConfidentialClientApplication(
        authority=authority_url,
        client_id=client_id,
        client_credential=client_credential
    )
    token = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"])
    return token


# obter o ID do drive (unidade) do OneDrive associado a um usuário específico no Microsoft Graph
def obter_drive_id(user_id):
    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if 'value' in data and len(data['value']) > 0:
            # Obtém o ID do primeiro drive (OneDrive pessoal) do usuário
            drive_id = data['value'][0]['id']
            return drive_id
        else:
            registrar_print("Usuário não tem nenhum drive associado.")
            return None
    else:
        registrar_print("Erro ao obter os drives do usuário.")
        return None


# função para ler todos os conteudos e pastas do oneddrive
def listar_conteudo_pasta(user_id, pasta_id):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        item_list = []
        for item in data['value']:
            if 'folder' in item:
                print(f"Pasta: {item['name']} - ID: {item['id']}")
                item_list.append({"Pasta": item['name'], "ID": item['id']})
            else:
                print(f"Arquivo: {item['name']} - ID: {item['id']}")
                item_list.append({"Pasta": item['name'], "ID": item['id']})
        df = pd.DataFrame(item_list)
        return df
    else:
        registrar_print("Erro ao listar o conteúdo da pasta.")
    return data


# função para ler os arquivos pdf, xlsx, csv ou txt
def ler_arquivo(user_id, arquivo_id, format_, delimitador=None, header=None, skiprows=None):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        return

    token_info = token()
    if not token_info:
        print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        if format_ == 'csv':
            csv_data = response.content.decode('utf-8')
            df_csv = pd.read_csv(pd.compat.StringIO(
                csv_data), delimiter={delimitador})
            return df_csv
        elif format_ == 'xlsx':
            df_xlsx = pd.read_excel(
                BytesIO(response.content), header=header, skiprows=skiprows)
            return df_xlsx
        elif format_ == 'pdf':
            try:
                pdf_data = response.content
                dfs = tabula.read_pdf(
                    pdf_data, pages='all', multiple_tables=True)
                lista_dataframes = []    # Criar uma lista vazia para armazenar os DataFrames
                for i, df in enumerate(dfs, start=1):
                    print(f"Tabela {i}:")
                    print(df)
                    # Adicionar o DataFrame à lista
                    lista_dataframes.append(df)
                return lista_dataframes
            except Exception as e:
                print("Erro ao extrair tabelas do PDF:", e)
                return None
        elif format_ == 'txt':
            txt_data = response.content.decode('utf-8')
            txt_df = pd.read_table(pd.compat.StringIO(txt_data))
            return txt_df
        else:
            print(f"Formato {format_} não suportado.")
            return None
    else:
        print(
            f"Erro ao baixar o arquivo no formato {format_}: {response.status_code}, {response.text}")
        return None


# função para criar pasta log no output
def criar_pasta_log(user_id, pasta_id):
    global id_pasta_log
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return None

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}

    data_hora = data_hora_atual()

    # Verificar se a pasta "log" já existe e removê-la, se for o caso
    url_list_children = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children?$select=id,name"
    response_list_children = requests.get(url_list_children, headers=headers)

    if response_list_children.status_code != 200:
        registrar_print("Erro ao listar itens da pasta pai.")
        return None

    for item in response_list_children.json().get('value', []):
        if item['name'] == data_hora:
            url_delete_folder = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{item['id']}"
            response_delete_folder = requests.delete(
                url_delete_folder, headers=headers)
            if response_delete_folder.status_code != 204:
                registrar_print("Erro ao excluir a pasta log.")
                return None

    # Criar a pasta "log" dentro da pasta pai com o nome da data e hora local
    url_create_folder = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": data_hora,
        "folder": {}
    }
    response_create_folder = requests.post(
        url_create_folder, headers=headers, json=data)

    if response_create_folder.status_code == 201:
        id_pasta_log.clear()
        pasta_log_id = response_create_folder.json().get('id')
        print(f"Pasta 'log' criada com sucesso no OneDrive.")
        return id_pasta_log.append(pasta_log_id)
    else:
        registrar_print("Erro ao criar a pasta log.")
        return None


# função para criar o log TXT
def criar_log(user_id, pasta_id, lista_prints):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    data_hora = data_hora_atual()
    nome_arquivo = f"{data_hora}.txt"

    # Verificar se o arquivo já existe na pasta pai
    url_list_children = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children?$select=id,name"
    response_list_children = requests.get(url_list_children, headers=headers)

    if response_list_children.status_code == 200:
        for item in response_list_children.json().get('value', []):
            if item['name'] == nome_arquivo:
                # Se o arquivo já existe, deletá-lo
                url_delete_file = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{item['id']}"
                response_delete_file = requests.delete(
                    url_delete_file, headers=headers)
                if response_delete_file.status_code == 204:
                    print(f"Arquivo '{nome_arquivo}' existente foi excluído.")
                else:
                    registrar_print("Erro ao excluir o arquivo existente.")

    # Criar o arquivo TXT diretamente na pasta pai com o mesmo nome da data e hora atual
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": nome_arquivo,
        "@microsoft.graph.conflictBehavior": "rename",
        "file": {}
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        arquivo_id = response.json().get('id')
        url_upload = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"

        # Cria o conteúdo do arquivo de texto com os registros da lista de prints
        conteudo_arquivo = '\n'.join(lista_prints) + '\n'

        response_upload = requests.put(
            url_upload, headers=headers, data=conteudo_arquivo.encode('utf-8'))

        if response_upload.status_code == 200:
            print(f"Arquivo '{nome_arquivo}' criado com sucesso no OneDrive.")


# função para exportar qualquer dataframe em qualquer formato
def exportar_df(user_id, pasta_id, arquivo, extensao_arquivo, nome):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    # data_hora = data_hora_atual()
    nome_arquivo = f"{nome}.{extensao_arquivo}"

    # Criar o arquivo diretamente na pasta pai com o mesmo nome do DataFrame concatenado com a extensão desejada
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": nome_arquivo,
        "@microsoft.graph.conflictBehavior": "rename",
        "file": {}
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        arquivo_id = response.json().get('id')
        url_upload = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"

        # Convertendo o DataFrame em uma string para enviar ao OneDrive
        conteudo_arquivo = str(arquivo)

        response_upload = requests.put(
            url_upload, headers=headers, data=conteudo_arquivo.encode('utf-8'))

        if response_upload.status_code == 200:
            print(f"Arquivo '{nome_arquivo}' criado com sucesso no OneDrive.")


# função para baixar qualquer arquivo online pela URL DIRETA
def baixar_arquivo_online(user_id, pasta_id, url_arquivo, nome_arquivo, extensao_arquivo=None):
    global id_arquivo_log
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        return

    token_info = token()
    if not token_info:
        print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

   # Fazer o download do arquivo da URL
    response_download = requests.get(url_arquivo)

    if response_download.status_code == 200:
        # Criar o arquivo diretamente na pasta pai com o nome do arquivo baixado
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
        data = {
            "name": f'{nome_arquivo}.{extensao_arquivo}',
            "@microsoft.graph.conflictBehavior": "rename",
            "file": {}
        }
        response_create = requests.post(url, headers=headers, json=data)

        if response_create.status_code == 201:
            arquivo_id = response_create.json().get('id')
            url_upload = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"

            conteudo_arquivo = response_download.content

            response_upload = requests.put(
                url_upload, headers=headers, data=conteudo_arquivo)
            if response_upload.status_code == 200:
                print(
                    f"Arquivo '{nome_arquivo}' baixado e salvo com sucesso no OneDrive e id {arquivo_id}.")
            return id_arquivo_log.append(arquivo_id)


# função para copiar, mover e renomear planilha original
def copiar_mover_renomear_arquivo(user_id, arquivo_id, pasta_destino_id, novo_nome_arquivo):
    global id_novo_arquivo
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return None

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}

    # Construir a URL para criar uma cópia do arquivo
    url_criar_copia_arquivo = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{arquivo_id}/copy"

    data = {
        "parentReference": {
            "id": pasta_destino_id
        },
        "name": novo_nome_arquivo
    }

    # Realizar a requisição para criar uma cópia do arquivo
    response_criar_copia_arquivo = requests.post(
        url_criar_copia_arquivo, headers=headers, json=data)

    if response_criar_copia_arquivo.status_code == 201:
        try:
            novo_arquivo_info = response_criar_copia_arquivo.json()
            novo_arquivo_id = novo_arquivo_info['id']
            print(
                f'Cópia do arquivo criada com sucesso com o nome: {novo_nome_arquivo}, ID do novo arquivo: {novo_arquivo_id}')
        except json.JSONDecodeError:
            print(
                "A resposta da requisição para criar a cópia não contém um JSON válido.")
            return None

        # Agora, mover a cópia do arquivo para a pasta de destino
        url_mover_arquivo = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{arquivo_id}"
        data_mover = {
            "parentReference": {
                "id": pasta_destino_id
            }
        }

        response_mover_arquivo = requests.patch(
            url_mover_arquivo, headers=headers, json=data_mover)

        if response_mover_arquivo.status_code == 200:
            print(f'Arquivo movido para a pasta de destino com sucesso.')
            id_novo_arquivo.append(novo_arquivo_id)
            return novo_arquivo_id
        else:
            print("Erro ao mover o arquivo para a pasta de destino.")
        print("Copiado com sucesso.")


# Função para enviar e-mail de falha do robô
def enviar_email(lista, rpa):
    # Configurar informações de envio de e-mail
    assunto = f'Falha ao Executar o Robô - {rpa}'
    if senha is None:
        registrar_print(
            'A senha do e-mail não foi configurada nas variáveis de ambiente.')
        return

    # Criar o corpo do e-mail
    corpo_email = '\n'.join(lista)

    try:
        # Configurar a conexão SMTP
        servidor = smtplib.SMTP(servidor_smtp, porta_smtp)
        servidor.starttls()

        # Faça login na conta de e-mail
        servidor.login(email, senha)

        # Crie a mensagem de e-mail
        msg = MIMEText(corpo_email)
        msg['Subject'] = assunto
        msg['From'] = email
        msg['To'] = para_email

        # Envie o e-mail
        servidor.sendmail(email, para_email, msg.as_string())
        servidor.quit()

        registrar_print('E-mail enviado com sucesso!')
    except Exception as e:
        registrar_print(f'Erro ao enviar o e-mail: {str(e)}')


# FUNÇÃO PARA VERIFICAR O MES E ANO ATUAL DOS DADOS NA TABELA
def verificar_deletar_mes_ano_atual(cursor, tabela, ano_atual, mes_atual, colunatable):
    # Executar a consulta para verificar se há dados para o ano e mês atual
    cursor.execute(
        f'SELECT * FROM {tabela} WHERE YEAR({colunatable}) = {ano_atual} AND MONTH({colunatable}) = {mes_atual}')

    # Verificar se há dados para o ano e mês atual
    dados_mes_ano_atual = cursor.fetchall()

    if dados_mes_ano_atual:
        # Se houver dados para o ano e mês atual, excluir todas as linhas correspondentes
        cursor.execute(
            f'DELETE FROM {tabela} WHERE YEAR({colunatable}) = {ano_atual} AND MONTH({colunatable}) = {mes_atual}')

        # Obter o número de linhas afetadas pela operação DELETE
        num_linhas_afetadas = cursor.rowcount
        print(
            f'Foram excluídas {num_linhas_afetadas} linhas para o ano {ano_atual} e mês {mes_atual}.')
    else:
        print(f'Não há dados para o ano {ano_atual} e mês {mes_atual}.')


# função para capturar o id das pasta input e output do diretorio especifico
def capturar_id(x, y, z):
    global id_pasta_input
    global id_pasta_output
    root = listar_conteudo_pasta(user_id, pasta_id='root')
    for a, pasta_a in enumerate(root['Pasta']):
        if pasta_a == x:
            id_pasta_a = root['ID'][a]
            pasta_1 = listar_conteudo_pasta(user_id, id_pasta_a)
            for b, pasta_b in enumerate(pasta_1['Pasta']):
                if pasta_b == y:
                    id_pasta_b = pasta_1['ID'][b]
                    pasta_2 = listar_conteudo_pasta(user_id, id_pasta_b)
                    for c, pasta_c in enumerate(pasta_2['Pasta']):
                        if pasta_c == z:
                            id_pasta_c = pasta_2['ID'][c]
                            pasta_3 = listar_conteudo_pasta(
                                user_id, id_pasta_c)
                            for d, pasta_d in enumerate(pasta_3['Pasta']):
                                if pasta_d == 'Input':
                                    id_pasta_d = pasta_3['ID'][d]
                                    id_pasta_input.append(id_pasta_d)
                                elif pasta_d == 'Output':
                                    id_pasta_e = pasta_3['ID'][d]
                                    id_pasta_output.append(id_pasta_e)

# função para capturar o id das pasta input e output do diretorio especifico
def capturar_id_v2(x, y, z):
    global id_pasta_input
    global id_pasta_output
    root = listar_conteudo_pasta(user_id, pasta_id='root')
    if root and 'value' in root:
        for a, item in enumerate(root['value']):
            pasta_a = item.get('name', '')
            if pasta_a == x and 'folder' in item:
                id_pasta_a = item['id']
                pasta_1 = listar_conteudo_pasta(user_id, id_pasta_a)
                if pasta_1 and 'value' in pasta_1:
                    for b, item_b in enumerate(pasta_1['value']):
                        pasta_b = item_b.get('name', '')
                        if pasta_b == y and 'folder' in item_b:
                            id_pasta_b = item_b['id']
                            pasta_2 = listar_conteudo_pasta(
                                user_id, id_pasta_b)
                            if pasta_2 and 'value' in pasta_2:
                                for c, item_c in enumerate(pasta_2['value']):
                                    pasta_c = item_c.get('name', '')
                                    if pasta_c == z and 'folder' in item_c:
                                        id_pasta_c = item_c['id']
                                        pasta_3 = listar_conteudo_pasta(
                                            user_id, id_pasta_c)
                                        if pasta_3 and 'value' in pasta_3:
                                            for d, item_d in enumerate(pasta_3['value']):
                                                pasta_d = item_d.get(
                                                    'name', '')
                                                if pasta_d == 'Input':
                                                    id_pasta_d = item_d['id']
                                                    id_pasta_input.append(
                                                        id_pasta_d)
                                                elif pasta_d == 'Output':
                                                    id_pasta_e = item_d['id']
                                                    id_pasta_output.append(
                                                        id_pasta_e)
