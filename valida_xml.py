# -*- coding: utf-8 -*-
# Imports

import requests
import json
import pandas as pd
import glob
import random
import time
import sys
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2

# Recuperar arquivos

caminho = open('C:/Users/USUARIO/validar_xml_datajud_cnj/config/caminho.txt','r')
arquivos = glob.glob(caminho.read())

# parametro para deletar ou não os arquivos do banco 
try:
    delete = sys.argv[1]
except:
    delete = '0'

#if len(arquivos) > 10000:
#    arquivos_aleatorios = random.sample(range(0, len(arquivos)), 10000)  
#else:
arquivos_aleatorios = range(0, len(arquivos))
caminho.close()

print('NUMERO DE ARQUIVOS -> ', len(arquivos))
print('TAMANHO DA AMOSTRA ->', len(arquivos_aleatorios))

file = open('C:/Users/USUARIO/validar_xml_datajud_cnj/config/reprocessar.txt','r')
processos = file.read().split('\n')
file.close()

if(processos[0] == ''):
    reprocessar = ''
else:
    reprocessar = [a for p in processos for a in arquivos if p in a]

#reprocessar
#"('"+"','".join(processos)+"')"

# Funções de apoio

def retorna_campos(arquivo):
    campos = arquivo.split('\\')[-1].split('_')
    grau = campos[1]
    cod_classe = campos[2]
    sistema = campos[3]
    num_processo = campos[-1][0:20]
    
    return grau, cod_classe, sistema, num_processo

def request_cnj(xml, num_processo):
    
    valida = 'https://validador-datajudh.cnj.jus.br/v1/valida'
    valida_xsd = 'https://validador-datajudh.cnj.jus.br/v1/validaXSD'
    
    print('PROCESSANDO -> ', num_processo, ' - DATA/HORA -> ', str(datetime.now()))
    erro_sintaxe = processa_retorno(xml, valida, num_processo)
    #time.sleep(1)
    erro_estrutura = processa_retorno(xml, valida_xsd, num_processo)
    
    return erro_sintaxe,erro_estrutura

def processa_retorno(xml, cnj_service, num_processo):
    
    files = {'arquivo': ('arquivo', open(xml, 'rb'), 'multipart/form-data')}
    
    try:
        r = requests.post(cnj_service, files=files, timeout=10)

        files['arquivo'][1].close()

        try:
            retorno = json.loads(r.text)
            if(retorno['qtdErros'] == 0):
                erros = [{'id':'NAO_POSSUI_ERROS', 'descricao':r.text}]
            else:
                erros = retorno['errosPorProcesso'][num_processo]
        except:
            erros = [{'id':'ERRO_PROCESSAMENTO', 'descricao':r.text}]
    except:
        erros = [{'id':'NAO_PROCESSADO', 'descricao':'REPROCESSAR XML'}]
    
    return erros

def processa_json(lista_erros, grau, cod_classe, sistema, num_processo):
    df = pd.DataFrame(lista_erros)
    df['num_processo'] = num_processo
    df['sistema'] = sistema
    df['grau'] = grau
    df['cod_classe'] = cod_classe
    
    return df

def processa_requisicao(arquivos, arquivos_aleatorios):

    df_erros_semantico = pd.DataFrame(columns=['id','descricao','num_processo','sistema','grau','cod_classe'])
    df_erros_estrutura = pd.DataFrame(columns=['id','descricao','num_processo','sistema','grau','cod_classe'])

    for a in arquivos_aleatorios:
        grau, cod_classe, sistema, num_processo = retorna_campos(arquivos[a])
        erros_semantico, erros_estrutura = request_cnj(arquivos[a], num_processo)
        
        df_erros_semantico = pd.concat([df_erros_semantico, processa_json(erros_semantico, grau, cod_classe, sistema, num_processo)]
                                     , ignore_index=True)
        df_erros_estrutura = pd.concat([df_erros_estrutura, processa_json(erros_estrutura, grau, cod_classe, sistema, num_processo)]
                                     , ignore_index=True)

    return df_erros_semantico, df_erros_estrutura

def exportar_excel(erros_semantico,erros_estrutura):
    # exportar
    erros_semantico.to_excel(r"Amostra Erros Semantico.xlsx", index=False)
    erros_estrutura.to_excel(r"Amostra Erros Estrutura.xlsx", index=False)
    #erros_semantico.to_csv(r"../Arquivos/Amostra Erros Sintaxe.xlsx", index=False)
    #erros_estrutura.to_csv(r"../Arquivos/Amostra Erros Estrutura.xlsx", index=False)

def inserir_banco(erros_semantico,erros_estrutura):    
    engine = conecta_db('insert')
    
    erros_semantico.to_sql('xml_erros_semantico', con=engine, schema='stage', 
                         index=False, if_exists='append', chunksize=1000)
    erros_estrutura.to_sql('xml_erros_estrutura', con=engine, schema='stage', 
                           index=False, if_exists='append', chunksize=1000)

def delete_processos(lista_processos,tabela, tudo):
    connection = conecta_db('delete')
    
    cursor = connection.cursor()

    if(tudo == 1):
        query = "DELETE FROM " + tabela
    else:    
        query = "DELETE FROM " + tabela + " WHERE num_processo IN " + "('"+"','".join(lista_processos)+"')"

    cursor.execute(query)
    
    connection.commit()
    
    print(cursor.rowcount, "REGISTROS EXCLUIDOS")
    
    connection.close()

def conecta_db(operacao):
    credenciais = open('C:/Users/USUARIO/validar_xml_datajud_cnj/config/credenciais.txt','r')
    user, pw, host, db = credenciais.read().split(';')
    credenciais.close()
    
    if(operacao == 'delete'):
        con = psycopg2.connect(host=host, database=db,
                                user=user, password=pw)
    elif(operacao == 'insert'):
        con = create_engine('postgresql+psycopg2://{user}:{pw}@{host}/{db}'.format(
                            user=user,pw=pw,host=host,db=db))
    
    return con

# Rodar requisições

print('INICIANDO PROCESSAMENTO...')
inicio = datetime.now()

if reprocessar == '':
    if(delete == '1'):
        delete_processos(processos, 'stage.xml_erros_semantico', 1)
        delete_processos(processos, 'stage.xml_erros_estrutura', 1)
    erros_semantico, erros_estrutura = processa_requisicao(arquivos, arquivos_aleatorios)
    inserir_banco(erros_semantico,erros_estrutura)
    #exportar_excel(erros_semantico,erros_estrutura)
else:
    delete_processos(processos, 'stage.xml_erros_semantico', 0)
    delete_processos(processos, 'stage.xml_erros_estrutura', 0)
    erros_semantico, erros_estrutura = processa_requisicao(reprocessar,range(0,len(reprocessar)))
    inserir_banco(erros_semantico,erros_estrutura)
    
fim = datetime.now()
print('PROCESSAMENTO ENCERRADO...')
print('EXECUÇÃO ENCERRADA... TEMPO DE PROCESSAMENTO -> ', str(fim-inicio))

print('NUM REGISTROS ERRO SINTAXE -> ',len(erros_semantico.num_processo.unique()))
print('NUM REGISTROS ERRO ESTRUTURA -> ',len(erros_estrutura.num_processo.unique()))

print('ERROS DE SINTAXE')
print(erros_semantico.groupby('id').descricao.count().sort_values(ascending=False))
print('')
print('ERROS DE ESTRUTURA')
print(erros_estrutura.groupby('id').descricao.count().sort_values(ascending=False))