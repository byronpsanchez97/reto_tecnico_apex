from __future__ import annotations

import argparse
from omegaconf import OmegaConf
from pyspark.sql import functions as F

from src.funciones import crear_sesion_spark, validar_fecha_iso, log, construir_ruta
from src.transformaciones import (
    estandarizar_columnas,
    aplicar_calidad_datos,
    filtrar_por_fechas_y_pais,
    normalizar_unidades_a_st,
    clasificar_entregas,
    agregar_columnas_adicionales,
    seleccionar_columnas_finales,
)


def _leer_csv_bronze(spark, cfg):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(cfg.input.ruta_raw)

    )


def _escribir_silver(df, cfg):
    ruta = cfg.output.ruta_silver
    (
        df.write
        .mode(cfg.output.modo_escritura)
        .format(cfg.output.formato)
        .save(ruta)
    )


def _escribir_processed_por_fecha(df, cfg):
    """
    Requerimiento: data/processed/${fecha_proceso}
    """
    fechas = [r["fecha_proceso"] for r in df.select("fecha_proceso").distinct().collect()]

    for f in fechas:
        ruta = construir_ruta(cfg.output.ruta_processed, str(f))
        (
            df.filter(F.col("fecha_proceso") == F.lit(f))
              .write
              .mode(cfg.output.modo_escritura)
              .format(cfg.output.formato)
              .save(ruta)
        )



def parse_args():
    p = argparse.ArgumentParser(description="ETL Entregas")
    p.add_argument("--config", required=True, help="Ruta YAML base. Ej: configuracion/base.yaml")
    p.add_argument("--entorno", default=None, help="develop|qa|main (merge configuracion/<entorno>.yaml)")
    p.add_argument("--fecha_inicio", default=None, help="Override YYYY-MM-DD")
    p.add_argument("--fecha_fin", default=None, help="Override YYYY-MM-DD")
    p.add_argument("--pais", default=None, help="Override país (ej: GT)")
    return p.parse_args()


def main():
    args = parse_args()

    cfg = OmegaConf.load(args.config)

    if args.entorno:
        cfg_env = OmegaConf.load(f"configuracion/{args.entorno}.yaml")
        cfg = OmegaConf.merge(cfg, cfg_env)
        cfg.app.entorno = args.entorno

    # Overrides por CLI (útil para reproceso)
    if args.fecha_inicio:
        cfg.filtros.fecha_inicio = args.fecha_inicio
    if args.fecha_fin:
        cfg.filtros.fecha_fin = args.fecha_fin
    if args.pais:
        cfg.filtros.pais = args.pais

    # Validaciones
    validar_fecha_iso(cfg.filtros.fecha_inicio, "filtros.fecha_inicio")
    validar_fecha_iso(cfg.filtros.fecha_fin, "filtros.fecha_fin")
    if cfg.filtros.fecha_inicio > cfg.filtros.fecha_fin:
        raise ValueError("filtros.fecha_inicio no puede ser mayor que filtros.fecha_fin")

    log(f"Entorno: {cfg.app.entorno}")
    log(f"Rango: {cfg.filtros.fecha_inicio} -> {cfg.filtros.fecha_fin}, país={cfg.filtros.pais}")

    spark = crear_sesion_spark(cfg.app.nombre)

    # =====================
    # Leer Origen
    # =====================
    log("Leyendo Origen...")
    df = _leer_csv_bronze(spark, cfg)

    # =====================
    # Persistencia intermedia
    # =====================
    log("Estandarizando columnas...")
    df = estandarizar_columnas(df)

    log("Aplicando calidad de datos...")
    df = aplicar_calidad_datos(df, cfg)

    log("Filtrando por rango de fechas y país...")
    df = filtrar_por_fechas_y_pais(df, cfg)

    log("Normalizando unidades (CS->ST)...")
    df = normalizar_unidades_a_st(df, cfg)

    log("Persistenciendo df con limpieza...")
    _escribir_silver(df, cfg)

    # =====================
    # Processed
    # =====================
    log("Clasificando tipos de entrega (rutina/bonificación) y agregando métricas...")
    df_gold = clasificar_entregas(df, cfg)
    df_gold = agregar_columnas_adicionales(df_gold, cfg)
    df_gold = seleccionar_columnas_finales(df_gold)

    log("Escribiendo GOLD por fecha_proceso...")
    _escribir_processed_por_fecha(df_gold, cfg)


    log("Proceso completado.")
    spark.stop()


if __name__ == "__main__":
    main()
