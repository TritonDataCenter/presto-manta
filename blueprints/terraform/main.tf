#
# Data Sources
#
data "triton_image" "ubuntu" {
  name        = "ubuntu-16.04"
  type        = "lx-dataset"
  most_recent = true
}

data "triton_network" "public" {
  name = "Joyent-SDC-Public"
}

data "triton_network" "private" {
  name = "My-Fabric-Network"
}

#
# Modules
#
module "bastion" {
  source = "github.com/joyent/terraform-triton-bastion?ref=1.0.0-rc2"

  name    = "${var.environment_name}"
  image   = "${data.triton_image.ubuntu.id}"
  package = "g4-general-4G"

  # Public and Private
  networks = [
    "${data.triton_network.public.id}",
    "${data.triton_network.private.id}",
  ]
}

module "presto" {
  source = "github.com/joyent/terraform-triton-presto?ref=1.0.0-rc2"

  name                = "${var.environment_name}"
  image               = "${data.triton_image.ubuntu.id}"
  coordinator_package = "g4-general-4G"
  worker_package      = "g4-general-16G"

  # Private only
  networks = [
    "${data.triton_network.private.id}",
  ]

  provision        = "true"
  private_key_path = "${var.private_key_path}"

  count_workers = "${var.count_workers}"

  client_access = ["any"]

  manta_url    = "${var.manta_url}"
  manta_user   = "${var.manta_user}"
  manta_key_id = "${var.manta_key_id}"
  manta_key    = "${var.manta_key}"

  bastion_host             = "${module.bastion.bastion_address}"
  bastion_user             = "${module.bastion.bastion_user}"
  bastion_cns_service_name = "${module.bastion.bastion_cns_service_name}"
}
