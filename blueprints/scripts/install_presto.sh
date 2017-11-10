#!/bin/bash
#
# Installs Presto with some customizations specific to the overall project.
#
# Note: Generally follows guidelines at https://web.archive.org/web/20170701145736/https://google.github.io/styleguide/shell.xml.
#

# Close STDOUT file descriptor
exec 1<&-
# Close STDERR FD
exec 2<&-

# Open STDOUT as $LOG_FILE file for read and write.
exec 1<>/var/log/install-`date +%s`.log

# Redirect STDERR to STDOUT
exec 2>&1

set -o errexit
set -o pipefail
set -o nounset

export DEBIAN_FRONTEND=noninteractive

SUPPORTED_PLATFORMS=("Ubuntu")

# check_prerequisites - exits if distro is not supported.
#
# Parameters:
#     None.
function check_prerequisites() {
  local distro
  if [[ -f "/etc/lsb-release" ]]; then
    distro="Ubuntu"
  fi

  if [[ -z "${distro}" ]]; then
    log "Unsupported platform. Exiting..."
    exit 1
  fi
}

# install_dependencies - installs dependencies
#
# Parameters:
#     $1: the name of the distribution.
function install_dependencies() {
  log "Updating package index..."
  apt-get -qq -y update
  log "Upgrading existing packages"
  sudo apt-get -qq -y upgrade
  log "Installing prerequisites..."
  sudo apt-get -qq -y install --no-install-recommends \
    wget uuid openjdk-8-jdk-headless openjdk-8-dbg htop libnss3 dc netcat \
    unattended-upgrades
}

# configure_jvm - configures cryptographic extensions for Manta and installs
#                 helpers that allow for Java to run efficiently on LX.
# Parameters:
#     None.
function configure_jvm() {
  echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre" >> /etc/environment

  log "Adding libnss PKCS11 extensions to the JVM"

  # Add libnss PKCS11 configuration
  cat << 'EOF' > /etc/nss.cfg
name = NSS
nssLibraryDirectory = /usr/lib/x86_64-linux-gnu
nssDbMode = noDb
attributes = compatibility
EOF

  perl -0777 -i.original -pe \
    's/security.provider.1=sun.security.provider.Sun\nsecurity.provider.2=sun.security.rsa.SunRsaSign\nsecurity.provider.3=sun.security.ec.SunEC\nsecurity.provider.4=com.sun.net.ssl.internal.ssl.Provider\nsecurity.provider.5=com.sun.crypto.provider.SunJCE\nsecurity.provider.6=sun.security.jgss.SunProvider\nsecurity.provider.7=com.sun.security.sasl.Provider\nsecurity.provider.8=org.jcp.xml.dsig.internal.dom.XMLDSigRI\nsecurity.provider.9=sun.security.smartcardio.SunPCSC/security.provider.1=sun.security.pkcs11.SunPKCS11 \/etc\/nss.cfg\nsecurity.provider.2=sun.security.provider.Sun\nsecurity.provider.3=sun.security.rsa.SunRsaSign\nsecurity.provider.4=sun.security.ec.SunEC\nsecurity.provider.5=com.sun.net.ssl.internal.ssl.Provider\nsecurity.provider.6=com.sun.crypto.provider.SunJCE\nsecurity.provider.7=sun.security.jgss.SunProvider\nsecurity.provider.8=com.sun.security.sasl.Provider\nsecurity.provider.9=org.jcp.xml.dsig.internal.dom.XMLDSigRI\nsecurity.provider.10=sun.security.smartcardio.SunPCSC/igs' \
    /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/java.security

  log "Add CPU count spoofer library"
  mkdir -p /usr/local/numcpus
  wget -q -O /usr/local/numcpus/libnumcpus.so https://github.com/dekobon/libnumcpus/releases/download/1.0/libnumcpus-linux-x86_64.so
  echo '6bc838db493d70a83dba9bd3d34c013d368339f288efc4629830fdcb25fa08f7  /usr/local/numcpus/libnumcpus.so' | sha256sum -c

  log "Adding LX process tuning script"
  # Adds thread calculator script that allows you to tune JVM threadpools to
  # non-pathological values when running in a zone.
  cat <<'EOF' > /usr/local/bin/proclimit
#!/usr/bin/env sh

##
# When this script is invoked inside of a zone:
#
# This script returns a number representing a very conservative estimate of the
# maximum number of processes or threads that you want to run within the zone
# that invoked this script. Typically, you would take this value and define a
# multiplier that works well for your application.
#
# Otherwise:
# This script returns the number of cores reported by the OS.

# If we are on a LX Brand Zone calculation value using utilities only available in the /native
# directory

if [ -d /native ]; then
  PATH=/native/sbin:/native/usr/bin:/native/sbin:$PATH
fi

KSH="$(which ksh93)"
PRCTL="$(which prctl)"

if [ -n "${KSH}" ] && [ -n "${PRCTL}" ]; then
  CAP=$(${KSH} -c "echo \$((\$(${PRCTL} -n zone.cpu-cap -P \$\$ | grep privileged | awk '{ print \$3; }') / 100))")

  # If there is no cap set, then we will fall through and use the other functions
  # to determine the maximum processes.
  if [ -n "${CAP}" ]; then
    $KSH -c "echo \$((ceil(${CAP})))"
    exit 0
  fi
fi

# Linux calculation if you have nproc
if [ -n "$(which nproc)" ]; then
  nproc
  exit 0
fi

# Linux more widely supported implementation
if [ -f /proc/cpuinfo ] && [ -n $(which wc) ]; then
  grep processor /proc/cpuinfo | wc -l
  exit 0
fi

# OS X calculation
if [ "$(uname)" == "Darwin" ]; then
  sysctl -n hw.ncpu
  exit 0
fi

# Fallback value if we can't calculate
echo 1
EOF
  chmod +x /usr/local/bin/proclimit
}

# check_arguments - exits if prerequisites are NOT satisfied
#
# Parameters:
#     $1: the version of presto
#     $2: the version of the presto manta connector
#     $3: the mode for the machine to operate in
#     $4: the address of the coordinator
#     $5: the address of the thrift metastore
function check_arguments() {
  local -r version_presto=${1}
  local -r version_presto_manta=${2}
  local -r mode_presto=${3}
  local -r address_presto_coordinator=${4}
  local -r manta_url=${5}
  local -r manta_user=${6}
  local -r manta_key_id=${7}
  local -r manta_key=${8}

  if [[ -z "${version_presto}" ]]; then
    log "No presto version provided. Exiting..."
    exit 1
  fi

  if [[ -z "${version_presto_manta}" ]]; then
    log "No presto manta connector version provided. Exiting..."
    exit 1
  fi

  if [[ -z "${mode_presto}" ]]; then
    log "No mode provided. Exiting..."
    exit 1
  fi

  if [[ "${mode_presto}" != "coordinator" && "${mode_presto}" != "worker" && "${mode_presto}" != "coordinator-worker" ]]; then
    log "Unknown mode [${mode_presto}]. Exiting..."
    exit 1
  fi

  if [[ "${mode_presto}" == "worker" && -z "${address_presto_coordinator}" ]]; then
    log "Coordinator's IP address must be provided. Exiting..."
    exit 1
  fi

  if [[ -z "${manta_url}" ]]; then
    log "No Manta URL provided. Exiting..."
    exit 1
  fi

  if [[ -z "${manta_user}" ]]; then
    log "No Manta user provided. Exiting..."
    exit 1
  fi

  if [[ -z "${manta_key_id}" ]]; then
    log "No Manta key id provided. Exiting..."
    exit 1
  fi

  if [[ -z "${manta_key}" ]]; then
    log "No Manta key path provided. Exiting..."
    exit 1
  fi

}

# install - downloads and installs the specified tool and version
#
# Parameters:
#     $1: the version of presto
#     $2: the version of the presto manta connector
#     $3: the mode for the machine to operate in
#     $4: the address of the coordinator
function install_presto() {
  local -r version_presto=${1}
  local -r version_presto_manta=${2}
  local -r mode_presto=${3}
  local -r address_presto_coordinator=${4}
  local -r manta_url=${5}
  local -r manta_user=${6}
  local -r manta_key_id=${7}
  local -r manta_key=${8}

  local -r user_presto='presto'

  local -r path_file="presto-server-${version_presto}.tar.gz"
  local -r path_install="/usr/local/presto-server-${version_presto}"
  local -r manta_presto_library_path="${path_install}/plugin/manta/presto-manta-jar-with-dependencies.jar"
  local -r pid_file="/var/run/presto/presto.pid"
  local -r http_port="8080"

  log "Downloading Presto ${version_presto}..."
  wget -q -O ${path_file} "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${version_presto}/presto-server-${version_presto}.tar.gz"

  log "Installing Presto ${version_presto}..."

  useradd presto || log "User [presto] already exists. Continuing..."

  install -d -o ${user_presto} -g ${user_presto} ${path_install}
  tar -xzf ${path_file} -C /usr/local/

  install -d -o ${user_presto} -g ${user_presto} /etc/presto/
  install -d -o ${user_presto} -g ${user_presto} /etc/presto/catalog
  install -d -o ${user_presto} -g ${user_presto} /var/lib/presto/
  install -d -o ${user_presto} -g ${user_presto} /var/log/presto/
  install -d -o ${user_presto} -g ${user_presto} /etc/manta/
  install -d -o ${user_presto} -g ${user_presto} ${path_install}/plugin/manta

  log "Installing Presto Manta Connector ${version_presto_manta}..."
  wget -q -O ${manta_presto_library_path} "https://github.com/joyent/presto-manta/releases/download/${version_presto_manta}/presto-manta-${version_presto_manta}-jar-with-dependencies.jar"

  /usr/bin/printf "
node.environment=production
node.id=$(hostname)
node.data-dir=/var/lib/presto/
" > /etc/presto/node.properties

  /usr/bin/printf "
-server
-XX:MaxRAM=15500m
-Xmx14000m
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+UseNUMA
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
" > /etc/presto/jvm.config

  #
  # Configure as COORDINATOR
  #
  if [[ "${mode_presto}" == "coordinator" ]]; then
    log "Configuring node as a [${mode_presto}]..."

    /usr/bin/printf "
#
# coordinator
#
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=${http_port}
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://localhost:${http_port}
" > /etc/presto/config.properties
  fi

  #
  # Configure as WORKER
  #
  if [[ "${mode_presto}" == "worker" ]]; then
    log "Configuring node as a [${mode_presto}]..."

    /usr/bin/printf "
#
# worker
#
coordinator=false
http-server.http.port=${http_port}
query.max-memory=50GB
query.max-memory-per-node=5GB
discovery.uri=http://${address_presto_coordinator}:${http_port}
" > /etc/presto/config.properties
  fi

  #
  # Configure as BOTH coordinator and worker
  #
  if [[ "${mode_presto}" == "coordinator-worker" ]]; then
    log "Configuring node as a [${mode_presto}]..."

    /usr/bin/printf "
#
# coordinator-worker
#
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=${http_port}
query.max-memory=5GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://localhost:${http_port}
" > /etc/presto/config.properties
  fi

  /usr/bin/printf "${manta_key}" > /etc/manta/manta_key
  chown ${user_presto}:${user_presto} /etc/manta/manta_key
  chmod 600 /etc/manta/manta_key

  /usr/bin/printf "connector.name=manta
manta.max_connections=48
manta.url=${manta_url}
manta.user=${manta_user}
manta.key_path=/etc/manta/manta_key
manta.key_id=$(ssh-keygen -E md5 -l -q -f /etc/manta/manta_key | awk -F'(MD5:)|( )' '{print $3}')
manta.schema.default=~~/stor/presto
" > /etc/presto/catalog/manta.properties

  chown ${user_presto}:${user_presto} /etc/presto/catalog/manta.properties

  /usr/bin/printf "
PRESTO_OPTS= \
    --pid-file=${pid_file} \
    --node-config=/etc/presto/node.properties \
    --jvm-config=/etc/presto/jvm.config \
    --config=/etc/presto/config.properties \
    --launcher-log-file=/var/log/presto/launcher.log \
    --server-log-file=/var/log/presto/server.log \
    -Dhttp-server.log.path=/var/log/presto/http-request.log \
    -Dcatalog.config-dir=/etc/presto/catalog

[Install]
WantedBy=default.target
" > /etc/default/presto

  /usr/bin/printf "
[Unit]
Description=Presto Server
Documentation=https://prestodb.io/docs/current/index.html
After=network-online.target

[Service]
User=${user_presto}
Restart=on-failure
Type=forking
PIDFile=${pid_file}
RuntimeDirectory=presto
EnvironmentFile=/etc/default/presto
Environment=MANTA_URL=${manta_url}
Environment=MANTA_USER=${manta_user}
Environment=MANTA_KEY_ID=${manta_key_id}
Environment=MANTA_KEY_PATH=/etc/manta/manta_key
ExecStart=${path_install}/bin/launcher start \$PRESTO_OPTS
ExecStop=${path_install}/bin/launcher stop \$PRESTO_OPTS

[Install]
WantedBy=default.target
" > /etc/systemd/system/presto.service

  if [[ "${mode_presto}" == "worker" ]]; then
    log "Waiting for Presto Coordinator to come online at: http://${address_presto_coordinator}:${http_port}"
        while ! nc -z ${address_presto_coordinator} ${http_port}; do
      sleep 5
    done
  fi

  log "Starting presto..."
  systemctl daemon-reload

  systemctl enable presto.service
  systemctl start presto.service

  if [[ "${mode_presto}" == "coordinator" ]]; then
    log "Waiting for Presto Coordinator to start"
    while ! nc -z localhost ${http_port}; do
      sleep 5
    done
    log "Presto Coordinator is now online"
  fi
}

# install - downloads and installs the specified tool and version
#
# Parameters:
#     $1: the version of the tool
function install_presto_cli() {
  local -r version_presto=${1}

  local -r path_file="presto-cli-${version_presto}-executable.jar"
  local -r path_install="/usr/local/presto-cli-${version_presto}"

  log "Downloading Presto CLI ${version_presto}..."
  wget -q -O ${path_file} "https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${version_presto}/presto-cli-${version_presto}-executable.jar"

  log "Installing Presto CLI ${version_presto}..."

  useradd presto || log "User [presto] already exists. Continuing..."

  install -d -o presto -g presto ${path_install}
  mv ${path_file} ${path_install}/presto
  chmod +x ${path_install}/presto
}

# log - prints an informational message
#
# Parameters:
#     $1: the message
function log() {
  local -r message=${1}
  local -r script_name=$(basename ${0})
  echo -e "==> ${script_name}: ${message}"
}

# main
function main() {
  check_prerequisites

  # TODO(clstokes): switch for kvm vs lx for mdata-get
  local -r arg_version_presto=$(/native/usr/sbin/mdata-get 'version_presto')
  local -r arg_version_presto_manta=$(/native/usr/sbin/mdata-get 'version_presto_manta')
  local -r arg_mode_presto=$(/native/usr/sbin/mdata-get 'mode_presto')
  local -r arg_address_presto_coordinator=$(/native/usr/sbin/mdata-get 'address_presto_coordinator')
  local -r arg_manta_url=$(/native/usr/sbin/mdata-get 'manta_url')
  local -r arg_manta_user=$(/native/usr/sbin/mdata-get 'manta_user')
  local -r arg_manta_key_id=$(/native/usr/sbin/mdata-get 'manta_key_id')
  local -r arg_manta_key=$(/native/usr/sbin/mdata-get 'manta_key')
  check_arguments \
    ${arg_version_presto} ${arg_version_presto_manta} ${arg_mode_presto} ${arg_address_presto_coordinator} \
    ${arg_manta_url} ${arg_manta_user} ${arg_manta_key_id} "${arg_manta_key}"

  install_dependencies
  configure_jvm
  install_presto \
    ${arg_version_presto} ${arg_version_presto_manta} ${arg_mode_presto} ${arg_address_presto_coordinator} \
    ${arg_manta_url} ${arg_manta_user} ${arg_manta_key_id} "${arg_manta_key}"
  install_presto_cli ${arg_version_presto}

  log "Done."
}

main
