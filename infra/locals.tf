locals {
  schemas = jsondecode(file("${path.root}/../contracts/schemas.json"))
}