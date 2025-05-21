provider "aws" {
  region = "eu-central-1"  # Change to your preferred region
}

resource "aws_key_pair" "deployer" {
  key_name   = "my-ec2-key"
  public_key = file("/home/ila/server.pub")  # You must have a .pub file here
}

resource "aws_instance" "client" {
  count         = 25
  ami           = "ami-03250b0e01c28d196"  # Ubuntu 24.04 LTS
  instance_type = "t3.2xlarge"
  key_name      = aws_key_pair.deployer.key_name

  tags = {
    Name = "client-instance-${count.index}"
  }

  # Optional: open SSH access
  vpc_security_group_ids = [aws_security_group.allow_all.id]
}


resource "aws_security_group" "allow_all" {
  name        = "allow_all_traffic"
  description = "Allow all inbound and outbound traffic"

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]  # Allow ALL inbound IPv4
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]  # Allow ALL outbound IPv4
  }
}


output "public_ips" {
  value = [for instance in aws_instance.client : instance.public_ip]
}
