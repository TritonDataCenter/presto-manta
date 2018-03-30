#
# Outputs
#
output "bastion_address" {
  value = ["${module.bastion.bastion_address}"]
}

output "presto_coordinator_address" {
  value = ["${module.presto.presto_coordinator_address}"]
}
