# main.tf
# DS5220 Data Project 1 - Anomaly Detection Pipeline
# Terraform equivalent of the CloudFormation stack

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# ── Variables ─────────────────────────────────────────────────────────────────

variable "key_name" {
  description = "Name of an existing EC2 Key Pair for SSH access"
  type        = string
  default     = "ds5220-keypair"
}

variable "my_ip" {
  description = "Your public IP in CIDR form for SSH access"
  type        = string
  default     = "206.74.80.226/32"
}

variable "ubuntu_ami_id" {
  description = "Ubuntu 24.04 LTS AMI ID for us-east-1"
  type        = string
  default     = "ami-0c7217cdde317cfec"
}

variable "repo_url" {
  description = "HTTPS URL of your forked anomaly-detection repository"
  type        = string
  default     = "https://github.com/arnavjain321-prog/anomaly-detection.git"
}

# ── S3 Bucket ─────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_bucket" {
  bucket = "ds5220-anomaly-${data.aws_caller_identity.current.account_id}-us-east-1"

  tags = {
    Project = "ds5220-dp1"
  }
}

data "aws_caller_identity" "current" {}

# ── SNS Topic ─────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "ds5220_topic" {
  name = "ds5220-dp1"
}

# Allow S3 to publish to the SNS topic
resource "aws_sns_topic_policy" "s3_publish_policy" {
  arn = aws_sns_topic.ds5220_topic.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3PublishToTopic"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.ds5220_topic.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = aws_s3_bucket.data_bucket.arn
          }
        }
      }
    ]
  })
}

# S3 event notification → SNS (raw/*.csv only)
# In Terraform this is set directly on the bucket resource without
# the circular dependency issue that affects CloudFormation.
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  topic {
    topic_arn     = aws_sns_topic.ds5220_topic.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
    filter_suffix = ".csv"
  }

  depends_on = [aws_sns_topic_policy.s3_publish_policy]
}

# SNS HTTP subscription → EC2 /notify endpoint
resource "aws_sns_topic_subscription" "http_subscription" {
  topic_arn              = aws_sns_topic.ds5220_topic.arn
  protocol               = "http"
  endpoint               = "http://${aws_eip.instance_eip.public_ip}:8000/notify"
  endpoint_auto_confirms = true

  depends_on = [aws_eip_association.eip_assoc]
}

# ── IAM Role for EC2 ──────────────────────────────────────────────────────────

resource "aws_iam_role" "ec2_role" {
  name = "ds5220-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "ec2_s3_policy" {
  name = "AnomalyBucketFullAccess"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.data_bucket.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.data_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "ds5220-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# ── Security Group ────────────────────────────────────────────────────────────

resource "aws_security_group" "instance_sg" {
  name        = "ds5220-sg"
  description = "Allow SSH from my IP and FastAPI port from anywhere"

  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
  }

  ingress {
    description = "FastAPI port open to all"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ds5220-sg"
  }
}

# ── EC2 Instance ──────────────────────────────────────────────────────────────

resource "aws_instance" "anomaly_instance" {
  ami                    = var.ubuntu_ami_id
  instance_type          = "t3.micro"
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.instance_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  root_block_device {
    volume_size           = 16
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e

    # Update and install dependencies
    apt-get update -y
    apt-get install -y python3 python3-pip python3-venv git

    # Set BUCKET_NAME globally for all future logins and reboots
    echo "BUCKET_NAME=${aws_s3_bucket.data_bucket.id}" >> /etc/environment

    # Also export immediately for this bootstrap session
    export BUCKET_NAME=${aws_s3_bucket.data_bucket.id}

    # Clone the forked application
    cd /home/ubuntu
    git clone ${var.repo_url} anomaly-detection

    cd /home/ubuntu/anomaly-detection

    # Set up Python virtual environment and install dependencies
    python3 -m venv venv
    /home/ubuntu/anomaly-detection/venv/bin/pip install --upgrade pip
    /home/ubuntu/anomaly-detection/venv/bin/pip install -r /home/ubuntu/anomaly-detection/requirements.txt

    # Create log directory
    mkdir -p /var/log/anomaly-detection
    chown ubuntu:ubuntu /var/log/anomaly-detection

    # Create systemd service for auto-start on reboot
    cat > /etc/systemd/system/anomaly-api.service <<SYSTEMD
    [Unit]
    Description=Anomaly Detection FastAPI Service
    After=network.target

    [Service]
    User=ubuntu
    WorkingDirectory=/home/ubuntu/anomaly-detection
    Environment=BUCKET_NAME=${aws_s3_bucket.data_bucket.id}
    ExecStart=/home/ubuntu/anomaly-detection/venv/bin/fastapi run /home/ubuntu/anomaly-detection/app.py --host 0.0.0.0 --port 8000
    Restart=always
    RestartSec=5

    [Install]
    WantedBy=multi-user.target
    SYSTEMD

    systemctl daemon-reload
    systemctl enable anomaly-api.service
    systemctl start anomaly-api.service
  EOF

  tags = {
    Name = "ds5220-instance"
  }
}

# ── Elastic IP ────────────────────────────────────────────────────────────────

resource "aws_eip" "instance_eip" {
  domain = "vpc"

  tags = {
    Name = "ds5220-eip"
  }
}

resource "aws_eip_association" "eip_assoc" {
  instance_id   = aws_instance.anomaly_instance.id
  allocation_id = aws_eip.instance_eip.id
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "bucket_name" {
  description = "S3 bucket used by the anomaly pipeline"
  value       = aws_s3_bucket.data_bucket.id
}

output "elastic_ip" {
  description = "Public Elastic IP of the EC2 instance"
  value       = aws_eip.instance_eip.public_ip
}

output "api_endpoint" {
  description = "Base URL for the FastAPI service"
  value       = "http://${aws_eip.instance_eip.public_ip}:8000"
}

output "health_check_url" {
  description = "Health check endpoint"
  value       = "http://${aws_eip.instance_eip.public_ip}:8000/health"
}

output "sns_topic_arn" {
  description = "ARN of the SNS topic ds5220-dp1"
  value       = aws_sns_topic.ds5220_topic.arn
}

output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.anomaly_instance.id
}
