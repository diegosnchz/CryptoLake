terraform {
  required_version = ">= 1.8"
}

output "info" {
  value = "Local environment uses Docker Compose + MinIO. No Terraform resources needed."
}
