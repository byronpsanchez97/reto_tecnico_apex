from pyspark.sql import functions as F
from src.funciones import  seguimiento


# =========================
# Estandarización
# =========================
@seguimiento
def fun_trf_estandarizar_columnas(df):
    """
    Estandariza los nombres de las columnas a formato snake_case.
    """
    nuevas_columnas = []

    for columna in df.columns:
        # eliminar acentos
        nombre = unicodedata.normalize("NFKD", columna)
        nombre = nombre.encode("ascii", "ignore").decode("ascii")

        # convertir a minúsculas
        nombre = nombre.lower().strip()

        # reemplazar caracteres no alfanuméricos por '_'
        nombre = re.sub(r"[^a-z0-9]+", "_", nombre)

        # colapsar multiples '_'
        nombre = re.sub(r"_+", "_", nombre)

        # eliminar '_' iniciales y finales
        nombre = nombre.strip("_")

        nuevas_columnas.append(nombre)

    return df.toDF(*nuevas_columnas)


@seguimiento
def fun_trf_convertir_fecha_proceso(df):
    """
    Convierte fecha_proceso de YYYYMMDD a date (YYYY-MM-DD).
    """
    return df.withColumn(
        "fecha_proceso",
        F.to_date(F.col("fecha_proceso").cast("string"), "yyyyMMdd")
    )


# =========================
# Filtros
# =========================
@seguimiento
def fun_trf_filtrar_por_fechas_y_pais(df, cfg):
    fecha_inicio = int(cfg.filtros.fecha_inicio.replace("-", ""))
    fecha_fin = int(cfg.filtros.fecha_fin.replace("-", ""))

    df_filtrado = df.filter(
        (F.col("fecha_proceso") >= fecha_inicio) &
        (F.col("fecha_proceso") <= fecha_fin)
    )

    # Filtro por pais si se especifica
    if "pais" in cfg.filtros and cfg.filtros.pais:
        df_filtrado = df_filtrado.filter(F.col("pais") == cfg.filtros.pais)

    return df_filtrado


# =========================
# Reglas de negocio
# =========================
@seguimiento
def fun_trf_normalizar_unidades_a_st(df, cfg):
    """
    Normaliza columnas base y convierte unidades a estándar (ST).
    """
    factor = int(cfg.reglas_negocio.conversion_cs_a_st)

    df = (
        df
        # Normalización semántica (UNA SOLA VEZ)
        .withColumnRenamed("cantidad", "cantidad_origen")
        .withColumnRenamed("unidad", "unidad_origen")
    )

    return (
        df
        .withColumn(
            "cantidad_estandar",
            F.when(
                F.col("unidad_origen") == F.lit("CS"),
                F.col("cantidad_origen") * F.lit(factor)
            )
            .when(
                F.col("unidad_origen") == F.lit("ST"),
                F.col("cantidad_origen")
            )
            .otherwise(F.lit(None))
        )
        .withColumn("unidad_estandar", F.lit("ST"))
    )


@seguimiento
def fun_trf_clasificar_entregas(df, cfg):
    """
    Crea columnas:
    - cantidad_rutina
    - cantidad_bonificacion
    - cantidad_total_estandar
    """
    entregas_rutina = list(cfg.reglas_negocio.entregas_rutina)
    entregas_bonificacion = list(cfg.reglas_negocio.entregas_bonificacion)
    entregas_validas = list(set(entregas_rutina + entregas_bonificacion))

    df = df.filter(F.col("tipo_entrega").isin(entregas_validas))

    return (
        df
        .withColumn(
            "cantidad_rutina",
            F.when(
                F.col("tipo_entrega").isin(entregas_rutina),
                F.col("cantidad_estandar")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "cantidad_bonificacion",
            F.when(
                F.col("tipo_entrega").isin(entregas_bonificacion),
                F.col("cantidad_estandar")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "cantidad_total_estandar",
            F.col("cantidad_rutina") + F.col("cantidad_bonificacion")
        )
    )


@seguimiento
def fun_trf_agregar_columnas_adicionales(df, cfg):
    """
    Agrega métricas monetarias y auditoría ETL.
    """
    return (
        df
        .withColumn(
            "valor_rutina",
            F.when(F.col("cantidad_rutina") > 0, F.col("precio")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "valor_bonificacion",
            F.when(F.col("cantidad_bonificacion") > 0, F.col("precio")).otherwise(F.lit(0.0))
        )
        .withColumn("etl_entorno", F.lit(cfg.app.entorno))
        .withColumn("etl_timestamp", F.current_timestamp())
    )


@seguimiento
def fun_trf_seleccionar_columnas_finales(df):
    """
    Selecciona el conjunto estándar de columnas finales.
    """
    columnas = [
        "pais",
        "fecha_proceso",
        "transporte",
        "ruta",
        "material",
        "tipo_entrega",
        "precio",
        "cantidad_origen",
        "unidad_origen",
        "cantidad_estandar",
        "unidad_estandar",
        "cantidad_rutina",
        "cantidad_bonificacion",
        "cantidad_total_estandar",
        "valor_rutina",
        "valor_bonificacion",
        "etl_timestamp",
    ]

    return df.select(*[c for c in columnas if c in df.columns])
