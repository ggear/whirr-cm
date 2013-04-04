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
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;

import com.cloudera.whirr.cm.handler.CmServerHandler;
import com.cloudera.whirr.cm.handler.cdh.BaseHandlerCmCdh;
import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerException;
import com.cloudera.whirr.cm.server.CmServerService;
import com.cloudera.whirr.cm.server.CmServerServiceType;

public class CmServerUtil implements CmConstants {

  public static CmServerCluster getCluster(ClusterSpec clusterSpec, Set<Cluster.Instance> instances,
      CmServerCluster cluster) throws IOException, CmServerException {
    return getCluster(clusterSpec, instances, cluster, Collections.<String> emptySet());
  }

  public static CmServerCluster getCluster(ClusterSpec clusterSpec, Set<Cluster.Instance> instances,
      CmServerCluster cluster, Set<String> roles) throws IOException, CmServerException {
    cluster = (cluster == null ? new CmServerCluster() : cluster);
    for (Instance instance : instances) {
      for (String role : instance.getRoles()) {
        CmServerServiceType type = BaseHandlerCmCdh.getRoleToTypeGlobal().get(role);
        if (type != null && (roles == null || roles.isEmpty() || roles.contains(role))) {
          CmServerService service = new CmServerService(type, clusterSpec.getConfiguration().getString(
              CONFIG_WHIRR_NAME, CONFIG_WHIRR_NAME_DEFAULT), "" + (cluster.getServices(type).size() + 1),
              instance.getPublicHostName(), instance.getPublicIp(), instance.getPrivateIp());
          cluster.add(service);
        }
      }
    }
    return cluster;
  }

  public static String getServerHost(Set<Cluster.Instance> instances) throws UnknownHostException {
    for (Instance instance : instances) {
      for (String role : instance.getRoles()) {
        if (role.equals(CmServerHandler.ROLE)) {
          return instance.getPublicIp();
        }
      }
    }
    throw new UnknownHostException("Could not find server host");
  }

}
