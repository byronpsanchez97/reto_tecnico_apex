#!/bin/bash
set -e

ENTORNO=${1:-develop}
FECHA_INICIO=$2
FECHA_FIN=$3
PAIS=$4

CONFIG_FILE="configuracion/base.yaml"
LOG_DIR="logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/etl_${ENTORNO}_${TIMESTAMP}.log"

mkdir -p $LOG_DIR

echo "======================================" | tee -a $LOG_FILE
echo " Ejecutando ETL Entregas"              | tee -a $LOG_FILE
echo " Entorno      : $ENTORNO"              | tee -a $LOG_FILE
echo " Fecha inicio : ${FECHA_INICIO:-cfg}" | tee -a $LOG_FILE
echo " Fecha fin    : ${FECHA_FIN:-cfg}"    | tee -a $LOG_FILE
echo " País         : ${PAIS:-todos}"       | tee -a $LOG_FILE
echo "======================================" | tee -a $LOG_FILE

export PYTHONPATH=$(pwd)

CMD="spark-submit src/principal.py --config $CONFIG_FILE"

if [ -n "$FECHA_INICIO" ]; then
  CMD="$CMD --fecha_inicio $FECHA_INICIO"
fi

if [ -n "$FECHA_FIN" ]; then
  CMD="$CMD --fecha_fin $FECHA_FIN"
fi

if [ -n "$PAIS" ]; then
  CMD="$CMD --pais $PAIS"
fi

echo "Ejecutando comando:" | tee -a $LOG_FILE
echo "$CMD"               | tee -a $LOG_FILE
echo "--------------------------------------" | tee -a $LOG_FILE

eval $CMD >> $LOG_FILE 2>&1

echo "ETL finalizado correctamente " | tee -a $LOG_FILE
