#!/bin/bash

## borro todo del directorio landing
rm -f /home/hadoop/landing/*

## obtengo los archivos
wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv

## borro el contenido del directorio ingest de HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

## muevo los archivos bajados a landing al ingest de HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/CarRentalData.csv /ingest

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/georef-united-states-of-america-state.csv /ingest
