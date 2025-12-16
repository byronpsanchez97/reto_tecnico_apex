from datetime import datetime
import time
from functools import wraps

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


# =========================
# Decorador de seguimiento
# =========================
def seguimiento(func):
    """
    Decorador para seguimiento de ejecución:
    - tiempo
    - registros entrada
    - registros salida
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        nombre_funcion = func.__name__
        inicio = time.time()

        print(f"[SEGUIMIENTO] Iniciando: {nombre_funcion}")

        # Conteo de entrada
        df_entrada = args[0] if args and isinstance(args[0], DataFrame) else None
        if df_entrada is not None:
            try:
                print(f"[SEGUIMIENTO]   Registros entrada: {df_entrada.count()}")
            except Exception:
                print("[SEGUIMIENTO]   No se pudo contar registros de entrada")

        resultado = func(*args, **kwargs)

        # Conteo de salida
        if isinstance(resultado, DataFrame):
            try:
                print(f"[SEGUIMIENTO]   Registros salida: {resultado.count()}")
            except Exception:
                print("[SEGUIMIENTO]   No se pudo contar registros de salida")

        duracion = round(time.time() - inicio, 2)
        print(f"[SEGUIMIENTO] Finalizó: {nombre_funcion} | Duración: {duracion}s\n")

        return resultado

    return wrapper


# =========================
# Spark
# =========================
@seguimiento
def fun_crear_sesion_spark(nombre_app):
    spark = (
        SparkSession.builder
        .appName(nombre_app)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# =========================
# Utilidades generales
# =========================
@seguimiento
def fun_validar_fecha_iso(fecha, campo):
    """
    Valida fecha en formato YYYY-MM-DD.
    """
    try:
        datetime.strptime(fecha, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(
            f"'{campo}' inválido: {fecha}. Formato esperado: YYYY-MM-DD"
        ) from e


def fun_log(mensaje):
    print(f"[ETL] {mensaje}")


def fun_construir_ruta(*partes):
    """
    Construye rutas de forma portable.
    """
    return "/".join(
        p.strip("/").replace("\\", "/")
        for p in partes
        if p is not None and str(p).strip() != ""
    )

@seguimiento
def fun_normalizar_columna(col):
    """
    Normaliza texto categórico (trim + upper).
    """
    return F.upper(F.trim(col))


# =========================
# Calidad de datos
# =========================
@seguimiento
def fun_normalizar_texto(df):
    return (
        df.withColumn("pais", fun_normalizar_columna(F.col("pais")))
          .withColumn("unidad", fun_normalizar_columna(F.col("unidad")))
          .withColumn("tipo_entrega", fun_normalizar_columna(F.col("tipo_entrega")))
          # material es identificador → solo trim
          .withColumn("material", F.trim(F.col("material")))
    )

@seguimiento
def fun_filtrar_nulls(df):
    return df.filter(
        F.col("fecha_proceso").isNotNull() &
        F.col("pais").isNotNull() &
        F.col("material").isNotNull() &
        F.col("tipo_entrega").isNotNull() &
        F.col("unidad").isNotNull()
    )

@seguimiento
def fun_validar_numericos(df):
    return (
        df.withColumn("cantidad", F.col("cantidad").cast("double"))
          .withColumn("precio", F.col("precio").cast("double"))
          .filter(F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))
          # precio >= 0 → neutral (reglas de negocio se aplican después)
          .filter(F.col("precio").isNotNull() & (F.col("precio") >= 0))
    )

@seguimiento
def fun_validar_dominios(df, cfg):
    unidades_validas = list(cfg.calidad_datos.unidades_validas)
    tipos_validos = list(cfg.calidad_datos.tipos_entrega_validos)

    return (
        df.filter(F.col("unidad").isin(unidades_validas))
          .filter(F.col("tipo_entrega").isin(tipos_validos))
    )

@seguimiento
def fun_aplicar_calidad(df, cfg):
    """
    Aplica reglas técnicas de calidad de datos.
    No contiene reglas de negocio.
    """
    df = fun_normalizar_texto(df)
    df = fun_filtrar_nulls(df)
    df = fun_validar_numericos(df)
    df = fun_validar_dominios(df, cfg)
    return df
