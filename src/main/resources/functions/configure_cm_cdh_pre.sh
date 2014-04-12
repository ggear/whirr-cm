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

function configure_cm_cdh_pre() {
  local OPTIND
  local OPTARG
  CM_CDH_ROLE=
  CM_CDH_DIRS="/data"
  while getopts "r:d:" OPTION; do
    case $OPTION in
      r)
        CM_CDH_ROLE="$OPTARG"
        ;;
      d)
        CM_CDH_DIRS="$OPTARG"
        ;;
    esac
  done
  export IFS=, 
  CM_CDH_DIRS_ARRAY=($CM_CDH_DIRS)
  for i in "${CM_CDH_DIRS_ARRAY[@]}"; do
    mkdir -p $i
    chmod 777 $i
  done
  if [ "$CM_CDH_ROLE" = "cm-cdh-jobtracker" ]; then
    mkdir -p "${CM_CDH_DIRS_ARRAY[0]}/mapreduce/jobtracker/history"
    chmod 777 "${CM_CDH_DIRS_ARRAY[0]}/mapreduce/jobtracker/history"
elif [ "$CM_CDH_ROLE" = "cm-cdh-impala" -o "$CM_CDH_ROLE" = "cm-cdh-impalastatestore" -o "$CM_CDH_ROLE" = "cm-cdh-impalacatalog" ]; then
    JAVA_HOME=$(grep "export JAVA_HOME" /etc/profile | cut -d= -f 2)
    REPOCDH=${REPOCDH:-cdh5}
    CDH_MAJOR_VERSION=$(echo $REPOCDH | sed -e 's/cdh\([0-9]\).*/\1/')
    if [ $CDH_MAJOR_VERSION -le 4 ]; then
      if which dpkg &> /dev/null; then
        retry_apt_get update
        retry_apt_get -y install oracle-j2sdk1.6
        rm -rf /usr/lib/jvm/default
        ln -s $JAVA_HOME /usr/lib/jvm/default 
        update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
        update-alternatives --set java $JAVA_HOME/bin/java
      elif which rpm &> /dev/null; then
        retry_yum install -y jdk
        rm -rf /usr/java/default
        ln -s $JAVA_HOME /usr/java/default  
        alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
        alternatives --set java $JAVA_HOME/bin/java
      fi        
    fi
  elif [ "$CM_CDH_ROLE" = "cm-cdh-oozie" ]; then
    if which dpkg &> /dev/null; then
      export DEBIAN_FRONTEND=noninteractive
      retry_apt_get update
      retry_apt_get -q -y install unzip
    elif which rpm &> /dev/null; then
      retry_yum install -y unzip
    fi
    mkdir -p /var/lib/oozie/work
    if [ -f "/usr/share/java/mysql-connector-java.jar" ]; then
      chmod 777 /var/lib/oozie
      ln -s /usr/share/java/mysql-connector-java.jar /var/lib/oozie/mysql-connector-java.jar
    fi
    wget -nv http://extjs.com/deploy/ext-2.2.zip
    unzip -q ext-2.2.zip -d /var/lib/oozie
  fi
}
