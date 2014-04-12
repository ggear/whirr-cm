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

function install_cm_java() {

  if [ "$INSTALL_JAVA_DONE" == "1" ]; then
    return;
  fi

  if which dpkg &> /dev/null; then
    JDK_INSTALL_PATH=/usr/lib/jvm
  elif which rpm &> /dev/null; then
    JDK_INSTALL_PATH=/usr/java
  fi
  
  WORKING_DIR=$(pwd)
  mkdir -p $JDK_INSTALL_PATH
  cd $JDK_INSTALL_PATH

  if [ ! -z "${JDK_INSTALL_URL+xxx}" ]; then 
    JDK_PACKAGE=$(basename $JDK_INSTALL_URL)
    wget -nv $JDK_INSTALL_URL
    if [ $(echo $JDK_PACKAGE | cut -d'.' -f2) == "bin" ]; then
      chmod +x $JDK_PACKAGE  
      mv /bin/more /bin/more.no
      yes | ./$JDK_PACKAGE -noregister
      mv /bin/more.no /bin/more
      rm -f *.rpm $JDK_PACKAGE
    elif [ $(echo $JDK_PACKAGE | cut -d'.' -f2) == "tar" ]; then
      tar xf $JDK_PACKAGE
      rm -rf $JDK_PACKAGE  
    fi 
  else 
    REPOCM=${REPOCM:-cm5}
    CM_MAJOR_VERSION=$(echo $REPOCM | sed -e 's/cm\([0-9]\).*/\1/')
    if [ $CM_MAJOR_VERSION -ge 5 ]; then
      if which dpkg &> /dev/null; then
        retry_apt_get update
        retry_apt_get -y install oracle-j2sdk1.7
      elif which rpm &> /dev/null; then
        retry_yum install -y oracle-j2sdk1.7
      fi
    else
      if which dpkg &> /dev/null; then
        retry_apt_get update
        retry_apt_get -y install oracle-j2sdk1.6
      elif which rpm &> /dev/null; then
        retry_yum install -y jdk
      fi
    fi
  fi

  export JAVA_HOME=$(ls -dt $JDK_INSTALL_PATH/j* | head -n 1)
  rm -rf $JDK_INSTALL_PATH/default
  ln -s $JAVA_HOME $JDK_INSTALL_PATH/default

  echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile
  echo "export CMF_AGENT_JAVA_HOME=$JAVA_HOME" >> /etc/profile
  
  if which dpkg &> /dev/null; then
    update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
    update-alternatives --set java $JAVA_HOME/bin/java
  elif which rpm &> /dev/null; then
    alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
    alternatives --set java $JAVA_HOME/bin/java
  fi
  
  cd $WORKING_DIR

  INSTALL_JAVA_DONE=1
}
