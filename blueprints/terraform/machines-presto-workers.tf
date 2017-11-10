resource "triton_machine" "presto_worker" {
  count = "${var.count_presto_workers}"

  name    = "${var.project_name}-presto-worker-${count.index}"
  package = "${var.machine_package_zone}"
  image   = "${data.triton_image.ubuntu.id}"

  firewall_enabled = true

  networks = [
    "${data.triton_network.private.id}",
    "${data.triton_network.public.id}",
  ]

  cns {
    services = ["${local.cns_service_presto_worker}"]
  }

  tags {
    role = "presto"
  }

  metadata {
    version_presto             = "${var.version_presto}"
    version_presto_manta       = "${var.version_presto_manta}"
    mode_presto                = "worker"
    address_presto_coordinator = "${local.address_presto_coordinator}"

    manta_url    = "${var.manta_url}"
    manta_user   = "${var.manta_user}"
    manta_key_id = "${var.manta_key_id}"
    manta_key    = "${var.manta_key}"
  }
}

# This is separate from the triton_machine resource, because the firewall ports
# need to be open first.
resource "null_resource" "presto_worker_install" {
  count = "${var.count_presto_workers}"

  depends_on = [
    "triton_machine.presto_worker",
    "triton_firewall_rule.ssh",
  ]

  connection {
    host        = "${triton_machine.presto_worker.*.primaryip[count.index]}"
    user        = "root"
    private_key = "${file(var.key_path_private)}"
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir -p /tmp/presto_installer/",
    ]
  }

  provisioner "file" {
    source      = "../scripts/install_presto.sh"
    destination = "/tmp/presto_installer/install_presto.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod 0755 /tmp/presto_installer/install_presto.sh",
      "/tmp/presto_installer/install_presto.sh",
    ]
  }
}
