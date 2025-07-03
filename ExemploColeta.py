#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf-8
# executa_v2.py
# ###################################################################################################################
# INICIA O PYSPARK
# caminho para modelos  /opt/ingestao/automatizador/projetos/cadastro_positivo/XMLs servidor 1706
# Historico_Retorno_ACPT112.py dataframe
# Envio_Contestacao_ACPT113.py dataframe
# ###################################################################################################################


# pyspark --jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-1.0.0.7.1.6.0-297.jar --py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.6.0-297.zip --properties-file /home/SPDQ/_working/RedBill/spark/sparkDevSubmit.conf


# ###################################################################################################################
#                                                       imports
# ###################################################################################################################

from ast import Lambda
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext   # HiveContext
from pyspark.sql.functions import *
from datetime import datetime, timedelta

import sys
# import subprocess                    # os
import os
from os.path import exists
from xml.etree.ElementTree import Element, SubElement, tostring

from dateutil.relativedelta import relativedelta
from pyspark_llap import HiveWarehouseSession
import time
from time import ctime, gmtime, strftime, sleep


import hashlib

# ###################################################################################################################
#                                                       Funcoes
# ###################################################################################################################

def gravaErro(tabela, sql):
    '''
    Grava mensagem de erro no arquivo de log

    Parametros:
        tabela (string):  nome da tabela que gerou o erro
        sql    (string):  Query que gerou o erro

    Retorno:
        NaN
    '''
    global nomeLog
    global idProcesso

    # Guarda o standard output
    original_stdout = sys.stdout

    with open(nomeLog, 'a') as f:
        # Direciona a saida para o arquivo de log
        sys.stdout = f
        print(84*'#' )
        print("erro na geração da tabela {}".format(tabela))
        print(84*'-' )
        print('DEBUG')
        print(sql)
        print(84*'-' )

        # Retorna a saida para o standard output
        sys.stdout = original_stdout

        
    gravaLog('Inicio do processamento (PID:{})'.format(idProcesso))


    # Conexao
    gravaLog('Cria objetos spark e hive')




def finalProcesso():
    '''
    Grava no arquivo de log o final do processo

    Parametros:
        NaN

    Retorno:
        NaN
    '''
    global inicioColeta 
    global nomeLog

    # Calcula a duração em segundos
    duracao    = time.time() - inicioColeta

    # Converte a duração de segundos para HH:MM:SS
    duracaoHMS = time.strftime('%H:%M:%S', time.gmtime(duracao))
    momento    = str(datetime.now())[0:16]

    # Guarda o standard output
    original_stdout = sys.stdout

    with open(nomeLog, 'a') as f:
        # Direciona a saida para o arquivo de log
        sys.stdout = f
        print(84*'-' +'\n')
        print('Fim do processamento: {} '.format(momento))
        print('Tempo total da Coleta: {}'.format(duracaoHMS))

        print(84*'#' )

        # Retorna a saida para o standard output
        sys.stdout = original_stdout
        

def gravaLog(texto, tipo='s'):
    '''
    Inclui linha na log do processo

    Parametros:
        texto (string):  Texto descritivo do log
        tipo  (string):  S para log de secao,  D para Detalhe

    Retorno:
        NaN
    '''

    if tipo.lower() not in ['d', 's']:
        raise ValueError("O parâmetro 'tipo' deve ser 'd' ou 's'")

    global nomeLog
    global inicioProcesso

    # Calcula a duração em segundos
    duracao = time.time() - inicioProcesso

    tipo_selecionado  = tipo.lower()

    # Converte a duração de segundos para HH:MM:SS
    duracaoHMS = time.strftime('%H:%M:%S', time.gmtime(duracao))

    momento = str(datetime.now())[0:16]

    # Guarda o standard output
    original_stdout = sys.stdout

    with open(nomeLog, 'a') as f:
        # Direciona a saida para o arquivo de log
        sys.stdout = f

        if tipo_selecionado == 's':   #Seção
            print('\n'+ 84*'-' )
            print(texto.center(84))
            print(      84*'-' )

        else:                         #Detalhe
            print('{} ({}) - {}'.format(momento, duracaoHMS, texto))

        # Retorna a saida para o standard output
        sys.stdout = original_stdout
        
    inicioProcesso = time.time()


def separador():
    '''
   Grava uma linha de separador no arquivo do log para melhorar a organização desse.

    Parametros:
         Nenhum

    Retorno:
         Nenhum
    '''
    # Guarda o standard output
    original_stdout = sys.stdout

    with open(nomeLog, 'a') as f:
        # Direciona a saida para o arquivo de log
        sys.stdout = f

        print(84*'-' + '\n')

        # Retorna a saida para o standard output
        sys.stdout = original_stdout
        

def inicializaProcesso():
    '''
    Inicializa os objetos spark, hive e filesystem
    Cria o arquivo de log

    Parametros:
        texto (string):  Texto descritivo do log
        tipo  (string):  S para log de secao,  D para Detalhe

    Retorno:
        NaN
    '''
    global idProcesso

    idProcesso     = os.getpid()
    inicioProcesso = time.time()
    inicioColeta   = inicioProcesso

    duracao    = time.time() - inicioProcesso
    data       = str(datetime.now()).replace('-', '').replace(':', '')[0:8]
    hora       = str(datetime.now()).replace('-', '').replace(':', '')[9:15]
    nomeLog    = '../log/Coleta_Tsuru_B2C_{}_{}.log'.format(data, hora)
    duracaoHMS = time.strftime('%H:%M:%S', time.gmtime(duracao))
    momento    = str(datetime.now())[0:16]


    # Guarda o standard output
    original_stdout = sys.stdout

    with open(nomeLog, 'a') as f:
        # Direciona a saida para o arquivo de log
        sys.stdout = f
        linha =  '{} ({}) - {}'.format(momento, duracaoHMS, 'Inicio do processamento (PID:{})'.format(idProcesso))
        print(84*'#' + '\n')
        print(linha)

        # Conexao
        linha =  '{} ({}) - {}'.format(momento, duracaoHMS, 'Cria objetos spark e hive')
        spark = cria_spark()
        hive  = cria_hive(spark)
        Path  = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
        fs    = cria_fs(spark)

        # Retorna a saida para o standard output
        sys.stdout = original_stdout

    return spark, hive, Path, fs, inicioProcesso, inicioColeta, nomeLog


def cria_spark():
    '''
    Builder do Spark do SQL de consulta a tabela que será metrificada

    Parametros:
         Nenhum

    Retorno:
        sp (objeto): Objeto Spark
    '''

    appName = "coleta_inconsistencias_tsuru"

    db = "jdbc:hive2://host1.bigdata.dominio.br:2181,host2.bigdata.rede.br:2181,host3.bigdata.dominio.br:2181,host4.bigdata.dominio.br:2181,host5.bigdata.dominio.br:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

    sp = SparkSession.builder.enableHiveSupport().getOrCreate()
    sp = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()

    sp.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sp.conf.set("spark.hive.mapred.supports.subdirectories", "true")
    sp.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sp.conf.set("spark.hive.input.dir.recursive", "true")
    sp.conf.set("spark.hive.supports.subdirectories", "true")
    sp.conf.set("spark.hive.input.dir.recursive", "true")
    sp.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    sp.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    sp.conf.set("spark.sql.hive.manageFilesourcePartitions", "False")

    sp.conf.set("tez.queue.name", "NomedaFila")
    
    return sp


def cria_fs(spark):
    '''
    Builder do File system

    Parametros:
         Nenhum

    Retorno:
        Nenhum
    '''

    # iniciando sessao hdfs   nome do host "hdfs://hostexemplo"
    v_nome_host_hadoop = "hdfs://hostexemplo"
    URI = spark.sparkContext._gateway.jvm.java.net.URI

    # Path               = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
    c_fs = FileSystem.get(URI(v_nome_host_hadoop), Configuration())
    return c_fs


def cria_hive(spark):
    '''
    Builder do hive

    Parametros:
         spark(Objeto): Objeto Spark

    Retorno:
        c_hive (objeto): Objeto Hive
    '''
    # Cria a seção hive apos todos os sets
    c_hive = HiveWarehouseSession.session(spark).build()
    return c_hive


# ###################################################################################################################
#                                                       Fluxo Principal
# ###################################################################################################################

# Inicializa Ambiente e variaveis
spark , hive, Path, fs, inicioProcesso, inicioColeta, nomeLog = inicializaProcesso()
global idProcesso

#define o nivel de log WARN (mais limpo)/INFO (muito mais verboso)
spark.sparkContext.setLogLevel('WARN')

gravaLog('Especificação das coletas', 'd')

#nome amigavel, view de origem, tabela do dia
coletas = [
              ('Nome amigavel da primeira coleta'    , 'primeiraView'   , 'tb_destino1'     )
             ,('Nome amigavel da segunda coleta'     , 'segundaView'    , 'tb_destino2'     )
             ,('Nome amigavel da terceira coleta'    , 'terceiraView'   , 'tb_destino3'     )
             ,('Nome amigavel da quarta coleta'      , 'quartaView'     , 'tb_destino4'     )
          ]

gravaLog('Inicio geração de tabelas', 'd')

for amigavel, view, tabela in coletas:

    try:
        gravaLog('{} ({})'.format(amigavel, tabela), 's')

        gravaLog('Apaga tabela de coleta do dia (caso exista)','d')
        sql = 'DROP TABLE IF EXISTS banco.{}_dia'.format(tabela)
        hive.executeUpdate(sql)

        gravaLog('Apaga tabela auxiliar do histórico (caso exista)', 'd')
        sql = 'DROP TABLE IF EXISTS banco.{}_hist'.format(tabela)
        hive.executeUpdate(sql)

        gravaLog('Cria a tabela de coleta do dia', 'd')
        sql = '''
              CREATE table banco.{}_dia  as (
              SELECT * 
              FROM banco.{})
              '''.format(tabela, view)
        hive.executeUpdate(sql)

        gravaLog('Cria a tabela para mesclar o historico', 'd')
        sql = '''
              CREATE TABLE banco.{0}_hist as (
              SELECT * 
              FROM banco.{0}_dia limit 0)
              '''.format(tabela)
        hive.executeUpdate(sql)
        gravaLog('Gerou tabelas auxiliares','d')

        gravaLog('Grava o histórico na tabela de mesclagem', 'd')
        sql = '''
              INSERT INTO banco.{0}_hist  
              SELECT A.* 
              FROM banco.{0}             AS A
              LEFT JOIN banco.{0}_dia    AS B ON  B.sistema_origem  = A.sistema_origem
                                              AND B.dt_foto         = A.dt_foto
              WHERE B.sistema_origem IS NULL 
              '''.format(tabela)
        hive.executeUpdate(sql)

        gravaLog('Carrega a coleta do dia na tabela de mesclagem', 'd')
        sql = '''
              INSERT INTO banco.{0}_hist 
              SELECT * 
              FROM banco.{0}_dia
              '''.format(tabela)
        hive.executeUpdate(sql)
        gravaLog('Limpa a tabela das coletas', 'd')
        sql = 'TRUNCATE TABLE banco.{0} '.format(tabela)
        hive.executeUpdate(sql)

        gravaLog('Atualiza tabela {} (tabela final)'.format(amigavel), 'd' )
        sql = '''
              INSERT INTO banco.{0} 
              SELECT * 
              FROM banco.{0}_hist
              '''.format(tabela)
        hive.executeUpdate(sql)
        gravaLog('Efetou mescla de dados com sucesso (Histórico+Coleta)', 'd')

    except:
        gravaErro (tabela, sql)

finalProcesso()
