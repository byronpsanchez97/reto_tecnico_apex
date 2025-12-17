import argparse
from omegaconf import OmegaConf
from pyspark.sql import functions as F

from src.funciones import (
    fun_crear_sesion_spark,
    fun_validar_fecha_iso,
    fun_log,
    fun_construir_ruta,
    fun_aplicar_calidad,
)

from src.transformaciones import (
    fun_trf_estandarizar_columnas,
    fun_trf_filtrar_fecha_pais,
    fun_trf_normalizar_unidades_st,
    fun_trf_clasificar_entregas,
    fun_trf_agregar_columnas_adicionales,
    fun_trf_seleccionar_columnas_finales,
)


# =====================
# I/O
# =====================
def fun_leer_csv(spark, cfg):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(cfg.input.ruta_raw)
    )


def fun_escribir_curated(df, cfg):
    (
        df.write
        .mode(cfg.output.modo_escritura)
        .format(cfg.output.formato)
        .save(cfg.output.ruta_cleaned)
    )


def fun_escribir_processed(df, cfg):
    """
    Requerimiento: data/processed/${fecha_proceso}
    """
    fechas = [
        r["fecha_proceso"]
        for r in df.select("fecha_proceso").distinct().collect()
    ]

    for f in fechas:
        ruta = fun_construir_ruta(cfg.output.ruta_processed, str(f))
        fun_log(f"Escribiendo processed para fecha_proceso={f}")

        (
            df.filter(F.col("fecha_proceso") == F.lit(f))
              .write
              .mode(cfg.output.modo_escritura)
              .format(cfg.output.formato)
              .save(ruta)
        )


# =====================
# CLI
# =====================
def parse_args():
    p = argparse.ArgumentParser(description="ETL Entregas")
    p.add_argument("--config", required=True, help="Ruta YAML base")
    p.add_argument("--entorno", default=None, help="develop|main")
    p.add_argument("--fecha_inicio", default=None, help="YYYY-MM-DD")
    p.add_argument("--fecha_fin", default=None, help="YYYY-MM-DD")
    p.add_argument("--pais", default=None, help="Ej: EC, GT")
    return p.parse_args()


# =====================
# Main
# =====================
def main():
    args = parse_args()

    cfg = OmegaConf.load(args.config)

    if args.entorno:
        cfg_env = OmegaConf.load(f"configuracion/{args.entorno}.yaml")
        cfg = OmegaConf.merge(cfg, cfg_env)
        cfg.app.entorno = args.entorno

    # Overrides CLI
    if args.fecha_inicio:
        cfg.filtros.fecha_inicio = args.fecha_inicio
    if args.fecha_fin:
        cfg.filtros.fecha_fin = args.fecha_fin
    if args.pais:
        cfg.filtros.pais = args.pais

    # Validaciones
    fun_validar_fecha_iso(cfg.filtros.fecha_inicio, "filtros.fecha_inicio")
    fun_validar_fecha_iso(cfg.filtros.fecha_fin, "filtros.fecha_fin")

    if cfg.filtros.fecha_inicio > cfg.filtros.fecha_fin:
        raise ValueError("fecha_inicio no puede ser mayor que fecha_fin")

    fun_log(f"Entorno: {cfg.app.entorno}")
    fun_log(
        f"Rango: {cfg.filtros.fecha_inicio} -> {cfg.filtros.fecha_fin}, "
        f"país={cfg.filtros.pais}"
    )

    spark = fun_crear_sesion_spark(cfg.app.nombre)

    # =====================
    # Raw
    # =====================
    fun_log("Leyendo datos de origen...")
    df = fun_leer_csv(spark, cfg)

    # =====================
    # Curated
    # =====================
    fun_log("Estandarizando nombres de columnas...")
    df = fun_trf_estandarizar_columnas(df)


    fun_log("Aplicando calidad de datos...")
    df = fun_aplicar_calidad(df, cfg)

    fun_log("Filtrando por fechas y país...")
    df = fun_trf_filtrar_fecha_pais(df, cfg)

    fun_log("Normalizando unidades (CS a ST)...")
    df = fun_trf_normalizar_unidades_st(df, cfg)

    fun_log("Persistiendo cleaned...")
    fun_escribir_curated(df, cfg)

    # =====================
    # Processed
    # =====================
    fun_log("Aplicando reglas de negocio y métricas...")
    df_gold = fun_trf_clasificar_entregas(df, cfg)
    df_gold = fun_trf_agregar_columnas_adicionales(df_gold, cfg)
    df_gold = fun_trf_seleccionar_columnas_finales(df_gold)

    

    fun_log("Escribiendo proccesed particionado por fecha_proceso...")
    fun_escribir_processed(df_gold, cfg)

    fun_log("Proceso completado.")
    spark.stop()


if __name__ == "__main__":
    main()
