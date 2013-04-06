/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.whirr.cm;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;

import com.cloudera.whirr.cm.handler.CmAgentHandler;
import com.cloudera.whirr.cm.handler.CmNodeHandler;
import com.cloudera.whirr.cm.handler.CmServerHandler;
import com.cloudera.whirr.cm.handler.cdh.BaseHandlerCmCdh;
import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerException;
import com.cloudera.whirr.cm.server.CmServerService;
import com.cloudera.whirr.cm.server.CmServerServiceType;

public class CmServerClusterInstance implements CmConstants {

  private static CmServerCluster cluster;

  private CmServerClusterInstance() {
  }

  public static synchronized CmServerCluster getInstance() {
    return getInstance(false);
  }

  public static synchronized CmServerCluster getInstance(boolean clear) {
    return clear ? (cluster = new CmServerCluster()) : (cluster == null ? (cluster = new CmServerCluster()) : cluster);
  }

  public static CmServerCluster getInstance(ClusterSpec clusterSpec, Set<Instance> instances) throws CmServerException,
      IOException {
    return getInstance(clusterSpec, instances, Collections.<String> emptySet());
  }

  public static CmServerCluster getInstance(ClusterSpec clusterSpec, Set<Instance> instances, Set<String> roles)
      throws CmServerException, IOException {
    cluster = new CmServerCluster();
    for (Instance instance : instances) {
      for (String role : instance.getRoles()) {
        if (role.equals(CmServerHandler.ROLE)) {
          cluster.addServer(instance.getPublicHostName());
        } else if (role.equals(CmAgentHandler.ROLE)) {
          cluster.addAgent(instance.getPublicHostName());
        } else if (role.equals(CmNodeHandler.ROLE)) {
          cluster.addNode(instance.getPublicHostName());
        } else {
          CmServerServiceType type = BaseHandlerCmCdh.getRoleToTypeGlobal().get(role);
          if (type != null && (roles == null || roles.isEmpty() || roles.contains(role))) {
            CmServerService service = new CmServerService(type, clusterSpec.getConfiguration().getString(
                CONFIG_WHIRR_NAME, CONFIG_WHIRR_NAME_DEFAULT), "" + (cluster.getServices(type).size() + 1),
                instance.getPublicHostName(), instance.getPublicIp(), instance.getPrivateIp());
            cluster.addService(service);
          }
        }
      }
    }
    return cluster;
  }

}