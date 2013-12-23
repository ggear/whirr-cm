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

function install_cm_server() {
  REPOCM=${REPOCM:-cm5}
  CM_REPO_HOST=${CM_REPO_HOST:-archive.cloudera.com}
  CM_MAJOR_VERSION=$(echo $REPOCM | sed -e 's/cm\([0-9]\).*/\1/')
  CM_VERSION=$(echo $REPOCM | sed -e 's/cm\([0-9][0-9]*\)/\1/')
  if [ "$CM_MAJOR_VERSION" != "$CM_VERSION" ]; then
    if which dpkg &> /dev/null; then
      CM_VERSION_PACKAGE="="$CM_VERSION"*"
    elif which rpm &> /dev/null; then
      CM_VERSION_PACKAGE="-"$CM_VERSION
    fi    
  fi
  CM_REPO_ROOT=${CM_REPO_ROOT:-http://$CM_REPO_HOST/cm$CM_MAJOR_VERSION}
  if which dpkg &> /dev/null; then
    export DEBIAN_FRONTEND=noninteractive
    retry_apt_get update
    retry_apt_get -q -y install "cloudera-manager-server$CM_VERSION_PACKAGE" "cloudera-manager-daemons$CM_VERSION_PACKAGE"
  elif which rpm &> /dev/null; then
    retry_yum install --exclude jdk -y "cloudera-manager-server$CM_VERSION_PACKAGE" "cloudera-manager-daemons$CM_VERSION_PACKAGE"
  fi
  rm -rvf /etc/cloudera-scm-server/db.mgmt.properties
  mkdir -p /opt/cloudera/csd
  if [ $(curl -sI "$CM_REPO_ROOT/plugins/" | grep "HTTP/1.1 200 OK" | wc -l) -gt 0 ]; then
    wget -nd -N -np -nH -r -l 2 -A jar -erobots=off -P /opt/cloudera/csd $CM_REPO_ROOT/plugins
  fi
  chown -R cloudera-scm:cloudera-scm /opt/cloudera/csd
}
