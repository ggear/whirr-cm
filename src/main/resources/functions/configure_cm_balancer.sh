#
# Licensed to Cloudera, Inc. under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# Cloudera, Inc. licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -x

function configure_cm_balancer() {
  local OPTIND
  local OPTARG
  CM_BALANCER_BIND_PORT=9090
  CM_BALANCER_HOSTS=
  while getopts "b:h:" OPTION; do
    case $OPTION in
      b)
        CM_BALANCER_BIND_PORT="$OPTARG"
        ;;
      h)
        CM_BALANCER_HOSTS="$OPTARG"
        ;;
    esac
  done
  export IFS=, 
  CM_BALANCER_HOSTS_ARRAY=($CM_BALANCER_HOSTS)
  if which dpkg &> /dev/null; then
    retry_apt_get update
    retry_apt_get -y install haproxy 
  elif which rpm &> /dev/null; then
    retry_yum install -y haproxy
  fi
  CM_BALANCER_CONFIG='

  listen '$(hostname)' :'$CM_BALANCER_BIND_PORT'
    mode tcp
    option tcplog
    balance leastconn
'
  for i in "${!CM_BALANCER_HOSTS_ARRAY[@]}"; do 
    CM_BALANCER_CONFIG=$CM_BALANCER_CONFIG'
    server host'$i' '${CM_BALANCER_HOSTS_ARRAY[$i]}
  done
  echo $CM_BALANCER_CONFIG >> /etc/haproxy/haproxy.cfg
  service haproxy start
}
