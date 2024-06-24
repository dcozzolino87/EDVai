#!/bin/bash

## borro todo del directorio landing
rm -f /home/hadoop/landing/*

## obtengo los archivos
wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

## borro el contenido del directorio ingest de HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

## muevo los archivos bajados a landing al ingest de HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/2021-informe-ministerio.csv /ingest

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/202206-informe-ministerio.csv /ingest

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/aeropuertos_detalle.csv /ingest
