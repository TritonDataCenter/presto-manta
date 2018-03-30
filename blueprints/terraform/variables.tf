#
# Variables
#
variable "environment_name" {
  description = "The name of the environment."
  default     = "presto"
}

variable "private_key_path" {
  description = "The path to the private key to use for provisioning machines."
  type        = "string"
}

variable "manta_url" {
  default     = "https://us-east.manta.joyent.com/"
  description = "See module documentation."
  type        = "string"
}

variable "manta_user" {
  description = "See module documentation."
  type        = "string"
}

variable "manta_key_id" {
  description = "See module documentation."
  type        = "string"
}

variable "manta_key" {
  description = "See module documentation."
  type        = "string"
}

variable "count_workers" {
  default     = "1"
  description = "See module documentation."
  type        = "string"
}
