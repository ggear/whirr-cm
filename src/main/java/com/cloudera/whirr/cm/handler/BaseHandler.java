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
package com.cloudera.whirr.cm.handler;

import static org.apache.whirr.RolePredicates.role;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager.Rule;
import org.apache.whirr.service.hadoop.VolumeManager;

import com.cloudera.whirr.cm.CmConstants;

public abstract class BaseHandler extends ClusterActionHandlerSupport implements CmConstants {

  // jclouds allows '-', CM does not, CM allows '_', jclouds does not, so lets restrict to alphanumeric
  private static final Pattern CM_CLUSTER_NAME_REGEX = Pattern.compile("[A-Za-z0-9]+");

  protected static final String CONFIG_IMPORT_PATH = "functions/cmf/";

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeBootstrap(event);
    if (!CM_CLUSTER_NAME_REGEX.matcher(
        event.getClusterSpec().getConfiguration()
            .getString(ClusterSpec.Property.CLUSTER_NAME.getConfigName(), CONFIG_WHIRR_NAME_DEFAULT)).matches()) {
      throw new IOException("Illegal cluster name ["
          + event.getClusterSpec().getConfiguration()
              .getString(ClusterSpec.Property.CLUSTER_NAME.getConfigName(), CONFIG_WHIRR_NAME_DEFAULT)
          + "] passed in variable [" + ClusterSpec.Property.CLUSTER_NAME.getConfigName() + "] with default ["
          + CONFIG_WHIRR_NAME_DEFAULT + "]. Please use only alphanumeric characters.");
    }
  }

  protected void handleFirewallRules(ClusterActionEvent event, List<String> anySourcePorts,
      List<String> clusterSourcePorts) throws IOException {
    List<String> clientCirds = event.getClusterSpec().getClientCidrs();
    if (anySourcePorts != null && !anySourcePorts.isEmpty()) {
      event.getClusterSpec().setClientCidrs(Arrays.asList(new String[] { "0.0.0.0/0" }));
      for (String port : anySourcePorts) {
        if (port != null && !"".equals(port)) {
          event.getFirewallManager().addRule(Rule.create().destination(role(getRole())).port(Integer.parseInt(port)));
        }
      }
    }
    if (clusterSourcePorts != null && !clusterSourcePorts.isEmpty()) {
      List<String> cirds = new ArrayList<String>();
      cirds.add(getOriginatingIp(event));
      for (Instance instance : event.getCluster().getInstances()) {
        cirds.add(instance.getPrivateIp() + "/32");
        cirds.add(instance.getPublicIp() + "/32");
      }
      event.getClusterSpec().setClientCidrs(cirds);
      for (String port : clusterSourcePorts) {
        if (port != null && !"".equals(port))
          event.getFirewallManager().addRule(Rule.create().destination(role(getRole())).port(Integer.parseInt(port)));
      }
    }
    event.getClusterSpec().setClientCidrs(clientCirds);
    handleFirewallRules(event);
  }

  private String getOriginatingIp(ClusterActionEvent event) throws IOException {
    if ("stub".equals(event.getClusterSpec().getProvider())) {
      return "62.217.232.123";
    }
    HttpURLConnection connection = (HttpURLConnection) new URL("http://checkip.amazonaws.com/").openConnection();
    connection.connect();
    return IOUtils.toString(connection.getInputStream()).trim() + "/32";
  }

  public Map<String, String> getDeviceMappings(ClusterActionEvent event) {
    Map<String, String> deviceMappings = new HashMap<String, String>();
    if (event.getCluster() != null && event.getCluster().getInstances() != null
        && !event.getCluster().getInstances().isEmpty()) {
      deviceMappings.putAll(new VolumeManager().getDeviceMappings(event.getClusterSpec(), event.getCluster()
          .getInstances().iterator().next()));
    }
    return deviceMappings;
  }

}
