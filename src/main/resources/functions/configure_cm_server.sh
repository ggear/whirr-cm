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

function wait_cm_server() {
  for ID in {60..1}; do
    if [ $(curl -sI -u admin:admin "http://localhost:7180" | grep "HTTP/1.1 200 OK" | wc -l) -gt 0 ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

function configure_cm_server() {
  if [ -f /tmp/cm-license.txt ]; then
    service cloudera-scm-server start
	  if [ wait_cm_server ]; then
	    curl -u admin:admin -F license=@/tmp/cm-license.txt http://localhost:7180/api/v1/cm/license
	    rm -rf /tmp/cm-license.txt
	    service cloudera-scm-server restart
	    wait_cm_server
    fi
  fi
}
