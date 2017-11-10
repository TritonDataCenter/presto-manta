resource "triton_firewall_rule" "ssh" {
  rule        = "FROM any TO tag \"role\" = \"presto\" ALLOW tcp PORT 22"
  enabled     = true
  description = "SSH connection to Presto instances"
}

resource "triton_firewall_rule" "http_presto_worker_to_coordinator" {
  rule        = "FROM tag \"triton.cns.services\" = \"presto-worker\" TO tag \"triton.cns.services\" = \"presto-coordinator\" ALLOW tcp PORT 8080"
  enabled     = true
  description = "HTTP connection between Presto worker instances and coordinator"
}

resource "triton_firewall_rule" "http_presto_coordinator_to_worker" {
  rule        = "FROM tag \"triton.cns.services\" = \"presto-coordinator\" TO tag \"triton.cns.services\" = \"presto-worker\" ALLOW tcp PORT 8080"
  enabled     = true
  description = "HTTP connection between Presto worker instances and coordinator"
}

resource "triton_firewall_rule" "http_worker_to_worker" {
  rule        = "FROM tag \"triton.cns.services\" = \"presto-worker\" TO tag \"triton.cns.services\" = \"presto-worker\" ALLOW tcp PORT 8080"
  enabled     = true
  description = "HTTP connection between Presto worker instances"
}
