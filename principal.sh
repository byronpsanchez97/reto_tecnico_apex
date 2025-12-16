#!/bin/bash
set -e

# ============================
# Configuración general
# ============================
ENTORNO=${1:-develop}

CONFIG_FILE="configuracion/base.yaml"

# Parámetros opcionales
FECHA_INICIO=$2
FECHA_FIN=$3
PAIS=$4

echo "======================================"
echo " Ejecutando ETL Entregas"
echo " Entorno        : $ENTORNO"
echo " Fecha inicio   : ${FECHA_INICIO:-config}"
echo " Fecha fin      : ${FECHA_FIN:-config}"
echo " País           : ${PAIS:-todos}"
echo "======================================"

# ============================
# Variables de entorno
# ============================
export PYTHONPATH=$(pwd)

# ============================
# Construcción del comando
# ============================
CMD="spark-submit src/principal.py --config $CONFIG_FILE"

if [ ! -z "$FECHA_INICIO" ]; then
  CMD="$CMD --fecha_inicio $FECHA_INICIO"
fi

if [ ! -z "$FECHA_FIN" ]; then
  CMD="$CMD --fecha_fin $FECHA_FIN"
fi

if [ ! -z "$PAIS" ]; then
  CMD="$CMD --pais $PAIS"
fi

# ============================
# Ejecución
# ============================
echo "Ejecutando comando:"
echo "$CMD"
echo "--------------------------------------"

eval $CMD

echo "ETL finalizado correctamente"
