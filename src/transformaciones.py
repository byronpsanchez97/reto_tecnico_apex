from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.funciones import normalizar_texto, seguimiento


# =========================
# Estandarización / parsing
# =========================

@seguimiento
def estandarizar_columnas(df: DataFrame) -> DataFrame:
    """
    Aplica estándar simple: minúsculas. (El CSV ya viene en snake_case)
    """
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df

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
    Elimina anomalías y normaliza campos clave:
    - Nulls críticos
    - Cantidad negativa
    - Unidad y tipo_entrega fuera de dominio
    - Duplicados lógicos
    """
    # Normalización de strings (evita " gt ", "cs", etc.)
    df = (
        df.withColumn("pais", normalizar_texto(F.col("pais")))
          .withColumn("unidad", normalizar_texto(F.col("unidad")))
          .withColumn("tipo_entrega", normalizar_texto(F.col("tipo_entrega")))
          .withColumn("material", F.trim(F.col("material")))
    )

    # Nulls críticos
    df = (
        df.filter(F.col("fecha_proceso").isNotNull())
          .filter(F.col("pais").isNotNull())
          .filter(F.col("material").isNotNull())
          .filter(F.col("tipo_entrega").isNotNull())
          .filter(F.col("unidad").isNotNull())
    )

    # Tipos numéricos + anomalías
    df = (
        df.withColumn("cantidad", F.col("cantidad").cast("double"))
          .withColumn("precio", F.col("precio").cast("double"))
          .filter(F.col("cantidad").isNotNull() & (F.col("cantidad") >= 0))
          .filter(F.col("precio").isNotNull() & (F.col("precio") >= 0))
    )

    # Dominios válidos
    unidades_validas = [u for u in cfg.calidad_datos.unidades_validas]
    tipos_validos = [t for t in cfg.calidad_datos.tipos_entrega_validos]

    df = df.filter(F.col("unidad").isin(unidades_validas))
    df = df.filter(F.col("tipo_entrega").isin(tipos_validos))

    # Dedupe lógico (ajustado a tu dataset)
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

    # 🔑 Filtro por país opcional
    if "pais" in cfg.filtros and cfg.filtros.pais:
        df_filtrado = df_filtrado.filter(F.col("pais") == cfg.filtros.pais)

    return df_filtrado



# =========================
# Reglas de negocio
# =========================
@seguimiento
def normalizar_unidades_a_st(df: DataFrame, cfg) -> DataFrame:
    """
    Convierte todo a ST (unidades). CS equivale a 20 ST.
    Genera:
      - cantidad_st
      - unidad_final = 'ST'
    """
    factor = int(cfg.reglas_negocio.conversion_cs_a_st)

    return (
        df.withColumn(
            "cantidad_st",
            F.when(F.col("unidad") == F.lit("CS"), F.col("cantidad") * F.lit(factor))
             .when(F.col("unidad") == F.lit("ST"), F.col("cantidad"))
             .otherwise(F.lit(None))
        )
        .withColumn("unidad_final", F.lit("ST"))
    )

@seguimiento
def clasificar_entregas(df: DataFrame, cfg) -> DataFrame:
    """
    Crea columnas:
      - qty_rutina: ZPRE/ZVE1
      - qty_bonificacion: Z04/Z05
      - qty_total_st: suma
    Descarta cualquier otro tipo_entrega (ya filtrado por DQ, pero reforzamos).
    """
    rutina = [x for x in cfg.reglas_negocio.entregas_rutina]
    bonif = [x for x in cfg.reglas_negocio.entregas_bonificacion]
    validos = list(set(rutina + bonif))

    df = df.filter(F.col("tipo_entrega").isin(validos))

    return (
        df.withColumn(
            "qty_rutina",
            F.when(F.col("tipo_entrega").isin(rutina), F.col("cantidad_st")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "qty_bonificacion",
            F.when(F.col("tipo_entrega").isin(bonif), F.col("cantidad_st")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "qty_total_st",
            F.col("qty_rutina") + F.col("qty_bonificacion")
        )
    )

@seguimiento
def agregar_columnas_adicionales(df: DataFrame, cfg) -> DataFrame:
    """
    Extras con fundamento:
    - valor_total (cantidad_st * precio)
    - valor_rutina / valor_bonificacion (para análisis)
    - auditoría
    """
    df = df.withColumn("valor_total", F.col("cantidad_st") * F.col("precio"))

    df = (
        df.withColumn("valor_rutina",
                      F.when(F.col("qty_rutina") > 0, F.col("valor_total")).otherwise(F.lit(0.0)))
          .withColumn("valor_bonificacion",
                      F.when(F.col("qty_bonificacion") > 0, F.col("valor_total")).otherwise(F.lit(0.0)))
          .withColumn("etl_entorno", F.lit(cfg.app.entorno))
          .withColumn("etl_timestamp", F.current_timestamp())
    )
    return df

@seguimiento
def seleccionar_columnas_finales(df: DataFrame) -> DataFrame:
    """
    Estándar final de columnas (snake_case, minúsculas) y selección para output.
    """
    columnas = [
        "pais",
        "fecha_proceso",
        "transporte",
        "ruta",
        "material",
        "tipo_entrega",
        "precio",
        "cantidad",
        "unidad",
        "cantidad_st",
        "unidad_final",
        "qty_rutina",
        "qty_bonificacion",
        "qty_total_st",
        "valor_total",
        "valor_rutina",
        "valor_bonificacion",
        "etl_entorno",
        "etl_timestamp",
    ]
    existentes = [c for c in columnas if c in df.columns]
    return df.select(*existentes)
