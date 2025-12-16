variable "region" {
  description = "Region AWS"
  default     = "us-east-1"
}

variable "key_name" {
  description = "Key Pair existente en AWS"
  default     = "etl-key"
}

variable "instance_type" {
  description = "Tipo de instancia EC2"
  default     = "t3.micro"
}

variable "instance_name" {
  description = "Nombre de la instancia EC2"
  default     = "spark-ec2-micro"
}
