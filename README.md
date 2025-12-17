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

