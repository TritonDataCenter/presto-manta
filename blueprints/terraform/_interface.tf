terraform {
  required_version = "~> 0.10.6"
}

#
# Providers.
#
provider "triton" {
  version = "~> 0.2.0"
}

#
# Required variables.
#
variable "project_name" {
  description = "The name of this project. This value may be used for naming resources."
}

variable "triton_account_uuid" {
  description = "The Triton account UUID."
}

variable "manta_url" {
  default     = "https://us-east.manta.joyent.com/"
  description = "The URL of the Manta service endpoint."
}

variable "manta_user" {
  description = "The account name used to access the Manta service.."
}

variable "manta_key_id" {
  description = "The fingerprint for the public key used to access the Manta service."
}

variable "manta_key" {
  description = "The private key data for the Manta service credentials."
}

#
# Default Variables.
#
variable "triton_region" {
  default     = "us-east-1"
  description = "The region to provision resources within."
}

variable "network_name_private" {
  default     = "My-Fabric-Network"
  description = "The network name for private network access.."
}

variable "count_presto_workers" {
  default     = "2"
  description = "The number of Presto workers to provision."
}

variable "key_path_public" {
  default     = "~/.ssh/id_rsa.pub"
  description = "Path to the public key to use for connecting to machines."
}

variable "key_path_private" {
  default     = "~/.ssh/id_rsa"
  description = "Path to the private key to use for connecting to machines."
}

variable "machine_package_zone" {
  default     = "g4-highcpu-16G"
  description = "Machine package size to use."
}

variable "version_presto" {
  default     = "0.185"
  description = "The version of Presto to install. See https://repo1.maven.org/maven2/com/facebook/presto/presto-server/."
}

variable "version_presto_manta" {
  default     = "1.0.0-SNAPSHOT"
  description = "The version of the Presto Manta connector to install. See https://github.com/joyent/presto-manta"
}

#
# Data Sources
#
data "triton_image" "ubuntu" {
  name        = "ubuntu-16.04"
  type        = "lx-dataset"
  most_recent = true
}

# will be provided as an input variable so that only private networks
# can be used.
data "triton_network" "public" {
  name = "Joyent-SDC-Public"
}

data "triton_network" "private" {
  name = "${var.network_name_private}"
}

#
# Locals
#
locals {
  cns_service_presto_coordinator = "presto-coordinator"
  address_presto_coordinator     = "${local.cns_service_presto_coordinator}.svc.${var.triton_account_uuid}.${var.triton_region}.cns.joyent.com"

  cns_service_presto_worker = "presto-worker"
}

#
# Outputs
#
output "presto_coordinator_ip_public" {
  value = "${triton_machine.presto_coordinator.primaryip}"
}

output "presto_worker_ip_public" {
  value = "${triton_machine.presto_worker.*.primaryip}"
}
