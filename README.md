# Proyecto ETL con Spark sobre EC2
Este proyecto implementa un flujo ETL end-to-end utilizando Apache Spark, desplegado sobre una instancia EC2 aprovisionada automáticamente con Terraform.
El objetivo es garantizar un entorno reproducible, automatizado y listo para reprocesos.

## Despliegue de Infraestructura (Terraform)

Desde el directorio terraform se despliega la infraestructura necesaria

``` bash
cd terraform
terraform init
terraform plan
terraform apply
```
Terraform crea:
Una instancia EC2 (Amazon Linux 2)
Security Group con acceso SSH
Instalación automática de Java 11, Spark y utilidades base

## Verificación del Servicio Spark
Una vez finalizado el terraform apply, nos conectamos a la instancia EC2 y 
verificamos que Spark esté correctamente instalado y disponible en el PATH:

``` bash
spark-submit --version
```
Si el comando devuelve la versión de Spark, el entorno está listo.

## Preparación del Entorno de Ejecución
Nos ubicamos en el directorio home del usuario:
``` bash
cd ~
```

Clonamos el repositorio del proyecto:

``` bash
git clone https://github.com/byronpsanchez97/reto_tecnico_apex.git
cd reto_tecnico_apex
```

## Actualización de pip e Instalación de Dependencias

Antes de ejecutar el ETL, en ciertos casos es necesario actualizar pip3 y luego instalar las dependencias del proyecto para ello procedemos de la siguiente manera:

``` bash
python3 -m pip install --user --upgrade pip
pip3 install -r requirements.txt
```

## Ejecución del ETL
Se otorgan permisos de ejecución al script principal:
```bash
chmod +x principal.sh
```
Este script ejecuta internamente spark-submit y lanza el flujo completo de procesamiento.

El proceso ETL soporta dos modalidades de ejecución, permitiendo flexibilidad sin duplicar configuraciones.

### Opción 1: Ejecución usando valores por defecto
Cuando el script principal.sh se ejecuta sin parámetros adicionales, el ETL utiliza automáticamente los valores definidos en configuracion/base.yaml.

El archivo configuracion/base.yaml define los parámetros por defecto del proceso:

```yaml
filtros:
  fecha_inicio: "2025-01-01"
  fecha_fin: "2025-06-30"
  pais: null

```
``` bash
./principal.sh
```
En este caso:
Se cargan los parámetros desde base.yaml
No se sobrescribe ningún valor
Se ejecuta el proceso completo según la configuración base

### Opción 2: Ejecución parametrizada

Si principal.sh se ejecuta pasando parámetros, estos sobrescriben los valores de base.yaml únicamente para esa ejecución.
```
./principal.sh develop 2025-02-01 2025-02-28 EC
```
En este caso:
Se cargan los valores de base.yaml

Luego se aplican los parámetros recibidos por el script:
fecha_inicio = 2025-02-01
fecha_fin = 2025-02-28
pais = EC



## Logs
Al finalizar la ejecución, los logs del proceso pueden revisarse en:
```bash
cd logs
ls -ltra
cat etl_develop_XXX.log
```

## Arquitectura del Flujo de Datos

El flujo ETL procesa información de entregas a partir de archivos CSV
y los transforma progresivamente siguiendo una arquitectura por capas.

### Capas del flujo

1. **Raw**
   - Fuente: archivos CSV
   - Sin transformaciones de negocio
   - Objetivo: preservar el dato original

2. **Curated**
   - Estandarización de nombres de columnas
   - Aplicación de reglas de calidad
   - Normalización de unidades (CS → ST)
   - Datos listos para análisis

3. **Processed (Gold)**
   - Clasificación por tipo de entrega
   - Cálculo de métricas (rutina / bonificación)
   - Particionado por `fecha_proceso`



### Diagrama del Flujo ETL

```text
┌────────────┐
│   CSV Raw  │
│            │
└─────┬──────┘
      │
      ▼
┌────────────────────────┐
│ Estandarización        │
│ + Calidad de Datos     │
│ + Normalización Unid.  │
│        (curated)       │
└─────┬──────────────────┘
      │
      ▼
┌────────────────────────┐
│ Reglas de Negocio      │
│ Clasificación Entregas │
│ Métricas               │
│      (Processed)       │
└─────┬──────────────────┘
      │
      ▼
┌────────────────────────┐
│ data/processed/        │
│ fecha_proceso=YYYYMMDD│
└────────────────────────┘
