provider "aws" {
  region = var.region
}

# AMI Amazon Linux 2
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

# Security Group
resource "aws_security_group" "spark_sg" {
  name        = "${var.instance_name}-sg"
  description = "SG para SSH y Spark"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }


  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}



# EC2
resource "aws_instance" "spark_ec2" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.instance_type
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.spark_sg.id]
  associate_public_ip_address = true

user_data = <<-EOF
            #!/bin/bash
            set -e
            exec > /var/log/user-data.log 2>&1

            echo "===> Actualizando sistema"
            yum update -y

            echo "===> Instalando Java (Amazon Corretto 11)"
            yum install -y java-11-amazon-corretto java-11-amazon-corretto-devel wget tar

            echo "===> Configurando JAVA_HOME"
            echo "export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64" > /etc/profile.d/java.sh
            echo "export PATH=\\$JAVA_HOME/bin:\\$PATH" >> /etc/profile.d/java.sh
            chmod +x /etc/profile.d/java.sh

            echo "===> Instalando Spark"
            cd /opt

            SPARK_VERSION=3.5.1
            SPARK_PACKAGE=spark-$${SPARK_VERSION}-bin-hadoop3
            SPARK_TGZ=$${SPARK_PACKAGE}.tgz

            if [ ! -d "/opt/$${SPARK_PACKAGE}" ]; then
              wget -q https://archive.apache.org/dist/spark/spark-$${SPARK_VERSION}/$${SPARK_TGZ}
              tar -xzf $${SPARK_TGZ}
              ln -s $${SPARK_PACKAGE} spark
            fi

            echo "===> Configurando SPARK_HOME y PATH"
            cat <<'EOT' > /etc/profile.d/spark.sh
            export SPARK_HOME=/opt/spark
            export PATH=$SPARK_HOME/bin:$PATH
            EOT
            chmod +x /etc/profile.d/spark.sh

            
            echo "===> Bootstrap completado correctamente"
            EOF



  tags = {
    Name = var.instance_name
  }
}
