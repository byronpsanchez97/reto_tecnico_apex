output "ec2_public_ip" {
  description = "IP publica de la EC2"
  value       = aws_instance.spark_ec2.public_ip
}
