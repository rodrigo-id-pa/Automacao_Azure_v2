### INCIANDO O SCRIPT ###
""" 
bibliotecas padrão necessárias:
'msal', 'traceback', 'requests', 'azure.storage.blob', 'azure.identity', 'tabula', 'smtplib'

adicione suas bibliotecas para o desenvolvimento do rpa
"""
from variables import *
import importlib
import subprocess
import datetime
import json
import os
from config import *


# função para ler o json
def load_json(path):
    if os.path.exists(path):
        with open(path, 'r') as arquivo_json:
            return json.load(arquivo_json)
    return None


# função para interar pelos paths e depois chama a função para ler o json
def info():
    for path in paths:
        dados = load_json(path)
        if dados:
            return dados

    print("Caminho para o arquivo JSON não encontrado.")
    return None


# Obter a data e hora local atual no formato "19-07-2023_19-04"
def data_hora_atual():
    # Obtenha o horário atual
    now = datetime.datetime.now()

    # Crie um deslocamento de tempo de -3 horas
    offset = datetime.timedelta(hours=3)

    # Subtraia o deslocamento do horário atual para obter o horário com 3 horas a menos
    data = now - offset

    # Formate a data e hora no formato desejado (dd-mm-aaaa_hh-mm)
    return data.strftime("%d-%m-%Y_%H-%M")


# Função para registrar o print na lista de prints
def registrar_print(msg):
    global lista_prints
    print(f'{msg}')  # Exibe o print na saída padrão
    lista_prints.append(msg)  # Adiciona o print à lista


data_ini = data_hora_atual()
registrar_print(f'NOME_DO_RPA\n'  # NOME DO RPA
                f'{data_ini}\n'
                'INICIOU A EXECUÇÃO FLOW CONFIG')


# verificando se o pip está atualizado
def check_install_libs():
    try:
        try:
            import pip
            print('pip já está instalado.')
        except ImportError:
            print('pip não está instalado. Atualizando...')
            subprocess.check_call(
                ['python.exe', '-m', 'pip', 'install', '--upgrade', 'pip'])
            print('pip atualizado com sucesso.')

        # Lista de bibliotecas que você precisa verificar
        bibliotecas = ['msal', 'traceback', 'requests', 'azure.storage.blob',
                       'azure.identity', 'tabula', 'sidrapy', 'datetime', 'smtplib', 'pymssql']

        # Verifica se cada biblioteca está instalada e, se necessário, instala
        for biblioteca in bibliotecas:
            try:
                importlib.import_module(biblioteca)
                print(f'{biblioteca} já está instalada.')
            except ImportError:
                print(f'{biblioteca} não está instalada. Instalando...')
                subprocess.check_call(['pip', 'install', biblioteca])
                print(f'{biblioteca} instalada com sucesso.')
        registrar_print('FLOW CONFIG REALIZADO COM SUCESSO')
    except Exception as e:
        import traceback
        data_error = data_hora_atual()
        traceback_str = traceback.format_exc()
        registrar_print(
            f"Ocorreu um erro:\n{traceback_str}, {data_error}, {e}")


check_install_libs()
