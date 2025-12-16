from __future__ import annotations

from datetime import datetime
from typing import Optional

import time
from functools import wraps
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def seguimiento(func):
    """
    Decorador de seguimiento .

    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        nombre_funcion = func.__name__
        inicio = time.time()

        print(f"[SEGUIMIENTO] Iniciando: {nombre_funcion}")

        # Conteo de entrada si el primer argumento es DataFrame
        df_entrada = args[0] if args and isinstance(args[0], DataFrame) else None
        if df_entrada is not None:
            try:
                registros_entrada = df_entrada.count()
                print(f"[SEGUIMIENTO]   Registros entrada: {registros_entrada}")
            except Exception:
                print("[SEGUIMIENTO]   No se pudo contar registros de entrada")

        resultado = func(*args, **kwargs)

        # Conteo de salida si el resultado es DataFrame
        if isinstance(resultado, DataFrame):
            try:
                registros_salida = resultado.count()
                print(f"[SEGUIMIENTO]   Registros salida: {registros_salida}")
            except Exception:
                print("[SEGUIMIENTO]   No se pudo contar registros de salida")

        duracion = round(time.time() - inicio, 2)
        print(f"[SEGUIMIENTO] Finalizó: {nombre_funcion} | Duración: {duracion}s\n")

        return resultado

    return wrapper


def crear_spark(nombre_app: str) -> SparkSession:
    """
    Crea una SparkSession básica.
    """
    return SparkSession.builder.appName(nombre_app).getOrCreate()


def validar_fecha_iso(fecha: str, campo: str) -> None:
    """
    Valida fecha en formato YYYY-MM-DD.
    """
    try:
        datetime.strptime(fecha, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"'{campo}' inválido: {fecha}. Formato esperado: YYYY-MM-DD") from e


def normalizar_texto(col):
    """
    Normaliza texto
    """
    return F.upper(F.trim(col))


def log(mensaje: str) -> None:
    print(f"[ETL] {mensaje}")


def construir_ruta(*partes: str) -> str:
    """
    Construye rutas de forma portable.
    """
    return "/".join([p.strip("/").replace("\\", "/") for p in partes if p is not None and str(p).strip() != ""])
