### INCIANDO O SCRIPT ###
from azure.storage.blob import BlobServiceClient
from variables import *
import traceback
from config import *
check_install_libs()
infos = info()

# Configurações do Azure Blob Storage
blob_account_key = infos['key_portal_azure']
registrar_print('INICIOU O FLOW DO TOKEN')


# Função de conexão com o portal do azure e obter os dados do Json
def check_credentials():
    try:
        # Conecta ao Azure Storage usando a chave de conta diretamente
        account_url = f"https://{blob_account_name}.blob.core.windows.net/"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=blob_account_key)
        # Obter o cliente de blob para o arquivo JSON específico
        blob_client = blob_service_client.get_blob_client(
            container=blob_container_name, blob=f"{blob_directory_path}/{blob_file_name}")

        # Ler o conteúdo do arquivo JSON remotamente
        file_contents = blob_client.download_blob().readall()

        # Decodificar o conteúdo JSON
        parameters = json.loads(file_contents)

        # Obter os valores das variáveis
        user_id = parameters['user_id']
        tenant_id = parameters['tenant_id']
        client_id = parameters['client_id']
        client_credential = parameters['client_credential']
        registrar_print('FLOW TOKEN REALIZADO COM SUCESSO')
        return user_id, tenant_id, client_id, client_credential
    except Exception as e:
        data_error = data_hora_atual()
        traceback_str = traceback.format_exc()
        registrar_print(
            f"OCORREU UM ERRO NO FLOW TOKEN:\n{traceback_str}, {data_error}, {e}")
