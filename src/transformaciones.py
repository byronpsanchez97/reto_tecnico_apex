from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import re
import unicodedata
from pyspark.sql import DataFrame

from src.funciones import normalizar_texto, seguimiento


# =========================
# Estandarizacion
# =========================



@seguimiento
def estandarizar_columnas(df: DataFrame) -> DataFrame:
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
def convertir_fecha_proceso(df: DataFrame) -> DataFrame:
    """
    Convierte fecha_proceso de YYYYMMDD a date (YYYY-MM-DD).
    """
    return df.withColumn(
        "fecha_proceso",
        F.to_date(F.col("fecha_proceso").cast("string"), "yyyyMMdd")
    )


# =========================
# Calidad de datos
# =========================
@seguimiento
def aplicar_calidad_datos(df: DataFrame, cfg) -> DataFrame:
    """
    Elimina anomalias y normaliza campos clave:
    - Nulls criticos
    - Cantidad negativa
    - Unidad y tipo_entrega fuera de dominio
    - Duplicados logicos
    """
    # Normalizacion de campos strings 
    df = (
        df.withColumn("pais", normalizar_texto(F.col("pais")))
          .withColumn("unidad", normalizar_texto(F.col("unidad")))
          .withColumn("tipo_entrega", normalizar_texto(F.col("tipo_entrega")))
          .withColumn("material", F.trim(F.col("material")))
    )

    # Nulls criticos
    df = (
        df.filter(F.col("fecha_proceso").isNotNull())
          .filter(F.col("pais").isNotNull())
          .filter(F.col("material").isNotNull())
          .filter(F.col("tipo_entrega").isNotNull())
          .filter(F.col("unidad").isNotNull())
    )

    # Tipos numericos + anomalias
    df = (
        df.withColumn("cantidad", F.col("cantidad").cast("double"))
          .withColumn("precio", F.col("precio").cast("double"))
          .filter(F.col("cantidad").isNotNull() & (F.col("cantidad") >= 0))
          .filter(F.col("precio").isNotNull() & (F.col("precio") >= 0))
    )

    # Dominios validos
    unidades_validas = [u for u in cfg.calidad_datos.unidades_validas]
    tipos_validos = [t for t in cfg.calidad_datos.tipos_entrega_validos]

    df = df.filter(F.col("unidad").isin(unidades_validas))
    df = df.filter(F.col("tipo_entrega").isin(tipos_validos))

    # Dedupe logico (ajustado a tu dataset)
    df = df.dropDuplicates(["fecha_proceso", "pais", "material", "tipo_entrega", "ruta", "transporte"])

    return df


# =========================
# Filtros
# =========================
@seguimiento
def filtrar_por_fechas_y_pais(df, cfg):
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
@seguimiento
def normalizar_unidades_a_st(df: DataFrame, cfg) -> DataFrame:
    """
    Normaliza columnas base y convierte unidades a estándar (ST).

    Contrato resultante:
    - precio_unitario
    - cantidad_origen
    - unidad_origen
    - cantidad_estandar
    - unidad_estandar
    """
    factor = int(cfg.reglas_negocio.conversion_cs_a_st)

    df = (
        df
        # Normalización semántica (UNA SOLA VEZ)
        .withColumnRenamed("precio", "precio_unitario")
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
def clasificar_entregas(df: DataFrame, cfg) -> DataFrame:
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
def agregar_columnas_adicionales(df: DataFrame, cfg) -> DataFrame:
    """
    Agrega métricas monetarias y auditoría ETL.
    """
    df = df.withColumn(
        "valor_total",
        F.col("cantidad_estandar") * F.col("precio_unitario")
    )

    return (
        df
        .withColumn(
            "valor_rutina",
            F.when(F.col("cantidad_rutina") > 0, F.col("valor_total")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "valor_bonificacion",
            F.when(F.col("cantidad_bonificacion") > 0, F.col("valor_total")).otherwise(F.lit(0.0))
        )
        .withColumn("etl_entorno", F.lit(cfg.app.entorno))
        .withColumn("etl_timestamp", F.current_timestamp())
    )



@seguimiento
def seleccionar_columnas_finales(df: DataFrame) -> DataFrame:
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
        "precio_unitario",
        "cantidad_origen",
        "unidad_origen",
        "cantidad_estandar",
        "unidad_estandar",
        "cantidad_rutina",
        "cantidad_bonificacion",
        "cantidad_total_estandar",
        "valor_total",
        "valor_rutina",
        "valor_bonificacion",
        "etl_timestamp",
    ]

    return df.select(*[c for c in columnas if c in df.columns])
