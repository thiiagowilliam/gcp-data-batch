variable "composer_name" {}
variable "composer_image_version" {}
variable "region" {}
variable "project_id" {}
variable "composer_sa" {}
variable "env_variables" {}

variable "pypi_packages" {
  type        = map(string)
  description = "Lista de dependências Python para o processamento Medallion"
  default     = {
    "pandera" = "==0.17.2"
    "pyarrow" = "==14.0.1"
    "gcsfs"   = "==2023.10.0"
    "fsspec"  = "==2023.10.0"
    "pandas"  = "==2.1.1"
  }
}