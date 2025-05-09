output "public_ips" {
  value = [for instance in aws_instance.client : instance.public_ip]
}
