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
import static org.jclouds.scriptbuilder.domain.Statements.call;
import static org.jclouds.scriptbuilder.domain.Statements.createOrOverwriteFile;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;

import com.cloudera.whirr.cm.CmServerClusterInstance;
import com.cloudera.whirr.cm.handler.cdh.BaseHandlerCmCdh;
import com.cloudera.whirr.cm.server.CmServer;
import com.cloudera.whirr.cm.server.CmServerException;
import com.cloudera.whirr.cm.server.CmServerService;
import com.cloudera.whirr.cm.server.CmServerServiceType;
import com.cloudera.whirr.cm.server.impl.CmServerLog;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

public class CmServerHandler extends BaseHandlerCm {

  public static final String ROLE = "cm-server";

  public static final String LICENSE_FILE = "cm-license.txt";

  public static final String PROPERTY_PARCEL_PRODUCT = "cm-server.parcel.product";
  public static final String PROPERTY_PARCEL_VERSION = "cm-server.parcel.version";
  public static final String PROPERTY_PORTS = "cm-server.ports";
  public static final String PROPERTY_PORT_WEB = "cm-server.port.web";
  public static final String PROPERTY_PORT_COMMS = "cm-server.port.comms";

  public static final String CM_USER = "admin";
  public static final String CM_PASSWORD = "admin";

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeBootstrap(event);
    try {
      CmServerClusterInstance.getCluster().addServer(getInstanceId());
    } catch (CmServerException e) {
      throw new IOException("Unexpected error building cluster", e);
    }
    addStatement(event, call("install_cm"));
    addStatement(event, call("install_cm_server"));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeConfigure(event);
    URL licenceConfigUri = null;
    if ((licenceConfigUri = CmServerHandler.class.getClassLoader().getResource(LICENSE_FILE)) != null) {
      addStatement(
          event,
          createOrOverwriteFile(
              "/tmp/" + LICENSE_FILE,
              Splitter.on('\n').split(
                  CharStreams.toString(Resources.newReaderSupplier(licenceConfigUri, Charsets.UTF_8)))));
    }
    addStatement(event, call("configure_cm_server"));
    @SuppressWarnings("unchecked")
    List<String> ports = getConfiguration(event.getClusterSpec()).getList(PROPERTY_PORTS);
    ports.add(getConfiguration(event.getClusterSpec()).getString(PROPERTY_PORT_WEB));
    ports.add(getConfiguration(event.getClusterSpec()).getString(PROPERTY_PORT_COMMS));
    for (String port : ports) {
      if (port != null && !"".equals(port))
        event.getFirewallManager().addRule(Rule.create().destination(role(ROLE)).port(Integer.parseInt(port)));
    }
    handleFirewallRules(event);
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    super.afterConfigure(event);

    logHeader("ClouderaManagerServer");
    logLineItem("ClouderaManagerServer", "Web Console:");
    logLineItemDetail(
        "ClouderaManagerServer",
        "http://" + event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp() + ":"
            + getConfiguration(event.getClusterSpec()).getString(PROPERTY_PORT_WEB));
    logLineItem("ClouderaManagerServer", "Web Console User/Password (change these!):");
    logLineItemDetail("ClouderaManagerServer", CM_USER + "/" + CM_PASSWORD);
    logLineItem("ClouderaManagerServer", "Auto provision and start services:");
    logLineItemDetail("ClouderaManagerServer",
        "" + event.getClusterSpec().getConfiguration().getBoolean(CONFIG_WHIRR_AUTO_VARIABLE, true));

    logLineItem("ClouderaManagerServer", "User:");
    logLineItemDetail("ClouderaManagerServer", event.getClusterSpec().getClusterUser());
    logLineItem("ClouderaManagerServer", "Private Key Path:");
    logLineItemDetail("ClouderaManagerServer", event.getClusterSpec().getPrivateKeyFile() == null ? "<not-defined>"
        : event.getClusterSpec().getPrivateKeyFile().getCanonicalPath());
    logLineItem("ClouderaManagerServer", "Console:");
    logLineItemDetail("ClouderaManagerServer", "ssh -o StrictHostKeyChecking=no "
        + event.getClusterSpec().getClusterUser() + "@"
        + event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp());

    Set<Instance> nodes = event.getCluster().getInstancesMatching(role(CmNodeHandler.ROLE));
    if (!nodes.isEmpty()) {
      logHeader("ClouderaManagerNodes");
      logLineItem("ClouderaManagerNodes", "Consoles:");
      for (Instance instance : nodes) {
        logLineItemDetail("ClouderaManagerNodes", "ssh -o StrictHostKeyChecking=no "
            + event.getClusterSpec().getClusterUser() + "@" + instance.getPublicIp());
      }
    }

    Set<Instance> agents = event.getCluster().getInstancesMatching(role(CmAgentHandler.ROLE));
    if (!agents.isEmpty()) {
      logHeader("ClouderaManagerAgents");
      logLineItem("ClouderaManagerAgents", "Consoles:");
      for (Instance instance : agents) {
        logLineItemDetail("ClouderaManagerAgents", "ssh -o StrictHostKeyChecking=no "
            + event.getClusterSpec().getClusterUser() + "@" + instance.getPublicIp());
      }
    }

    if (!CmServerClusterInstance.getCluster().isEmpty()) {

      logHeader("ClouderaManagerClusterProvision");

      if (!event.getClusterSpec().getConfiguration().getBoolean(CONFIG_WHIRR_AUTO_VARIABLE, true)) {

        logLineItem("ClouderaManagerClusterProvision", "Warning, services found, but whirr");
        logLineItemDetail("ClouderaManagerClusterProvision", "property [" + CONFIG_WHIRR_AUTO_VARIABLE + "]");
        logLineItemDetail("ClouderaManagerClusterProvision", "set to false so not provsioning.");
        logLineItem("ClouderaManagerClusterProvision", "Roles:");
        for (String role : BaseHandlerCmCdh.getRoles()) {
          logLineItemDetail("ClouderaManagerClusterProvision", role);
        }

      } else if (event.getClusterSpec().getConfiguration().getBoolean(CONFIG_WHIRR_AUTO_VARIABLE, true)) {

        try {

          logLineItem("ClouderaManagerClusterProvision", "Roles:");

          CmServerClusterInstance.getCluster(event.getClusterSpec(), event.getCluster().getInstances());

          if (event.getClusterSpec().getConfiguration().getBoolean(CONFIG_WHIRR_USE_PACKAGES, false)) {
            CmServerClusterInstance.getCluster().setIsParcel(false);
          } else {
            CmServerClusterInstance.getCluster().setIsParcel(true);
          }

          CmServerClusterInstance.getCluster().setMounts(getDataMounts(event));

          for (CmServerService service : CmServerClusterInstance.getCluster().getServices(CmServerServiceType.CLUSTER)) {
            logLineItemDetail("ClouderaManagerClusterProvision", service.getName() + "@" + service.getIp());
          }

          logLineItem("ClouderaManagerClusterProvision", "Provision:");
          Map<String, String> config = new HashMap<String, String>();
          @SuppressWarnings("unchecked")
          Iterator<String> keys = event.getClusterSpec().getConfiguration().getKeys();
          while (keys.hasNext()) {
            String key = keys.next();
            if (key.startsWith(CONFIG_WHIRR_CM_PREFIX)) {
              config.put(key.replaceFirst(CONFIG_WHIRR_CM_PREFIX, ""), event.getClusterSpec().getConfiguration()
                  .getString(key));
            }
          }

          CmServer server = CmServerClusterInstance.getFactory().getCmServer(
              event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp(), 7180, CM_USER, CM_PASSWORD,
              new CmServerLog.CmServerLogSysOut(LOG_TAG_CM_SERVER_API, false));
          server.initialise(config);
          if (server.configure(CmServerClusterInstance.getCluster())) {
            if (!server.getServiceConfigs(CmServerClusterInstance.getCluster(), event.getClusterSpec()
                .getClusterDirectory())) {
              throw new CmServerException("Unexepcted error attempting to download cluster config");
            }
          } else {
            throw new CmServerException("Unexepcted error attempting to configure cluster");
          }

        } catch (Exception e) {

          logException(
              "ClouderaManagerClusterProvision",
              "Failed to execute Cloudera Manager Cluster Provision, please review the proceeding exception and log into the web console [http://"
                  + event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp()
                  + ":"
                  + getConfiguration(event.getClusterSpec()).getString(PROPERTY_PORT_WEB) + "] to resolve", e);

        }

      }

    }

  }

  @Override
  protected void afterStart(ClusterActionEvent event) throws IOException, InterruptedException {
    super.afterStart(event);

    if (!CmServerClusterInstance.getCluster().isEmpty()) {

      logHeader("ClouderaManagerClusterStart");

      if (!event.getClusterSpec().getConfiguration().getBoolean(CONFIG_WHIRR_AUTO_VARIABLE, true)) {

        logLineItem("ClouderaManagerClusterStart", "Warning, services found, but whirr");
        logLineItemDetail("ClouderaManagerClusterStart", "property [" + CONFIG_WHIRR_AUTO_VARIABLE + "]");
        logLineItemDetail("ClouderaManagerClusterStart", "set to false so not starting.");

        logLineItem("ClouderaManagerClusterStart", "Roles:");
        for (String role : BaseHandlerCmCdh.getRoles()) {
          logLineItemDetail("ClouderaManagerClusterStart", role);
        }

      } else if (event.getClusterSpec().getConfiguration().getBoolean(CONFIG_WHIRR_AUTO_VARIABLE, true)) {

        logLineItem("ClouderaManagerClusterStart", "Services:");
        for (CmServerServiceType type : CmServerClusterInstance.getCluster().getServiceTypes()) {
          logLineItemDetail("ClouderaManagerClusterStart", type.toString());
        }

        try {

          logLineItem("ClouderaManagerClusterStart", "Start:");
          CmServer server = CmServerClusterInstance.getFactory().getCmServer(
              event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp(), 7180, CM_USER, CM_PASSWORD,
              new CmServerLog.CmServerLogSysOut(LOG_TAG_CM_SERVER_API, false));
          if (server.isConfigured(CmServerClusterInstance.getCluster())) {
            server.start(CmServerClusterInstance.getCluster());
          } else {
            throw new CmServerException("Unexpected error starting cluster, not correctly provisioned");
          }

        } catch (Exception e) {

          logException(
              "ClouderaManagerClusterStart",
              "Failed to execute Cloudera Manager Cluster Start, please review the proceeding exception and log into the web console [http://"
                  + event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp()
                  + ":"
                  + getConfiguration(event.getClusterSpec()).getString(PROPERTY_PORT_WEB) + "] to resolve", e);

        }

      }

      logHeader("ClouderaManagerServer");
      logLineItem("ClouderaManagerServer", "Web Console:");
      logLineItemDetail(
          "ClouderaManagerServer",
          "http://" + event.getCluster().getInstanceMatching(role(ROLE)).getPublicIp() + ":"
              + getConfiguration(event.getClusterSpec()).getString(PROPERTY_PORT_WEB));
      logLineItem("ClouderaManagerServer", "Web Console User/Password (change these!):");
      logLineItemDetail("ClouderaManagerServer", CM_USER + "/" + CM_PASSWORD);

      logFooter();

    }

  }

  private Set<String> getDataMounts(ClusterActionEvent event) throws IOException {
    Set<String> mnts = new HashSet<String>();

    String overrideMnt = getConfiguration(event.getClusterSpec()).getString(DATA_DIRS_ROOT);

    if (overrideMnt != null) {
      mnts.add(overrideMnt);
    } else if (!getDeviceMappings(event).isEmpty()) {
      mnts.addAll(getDeviceMappings(event).keySet());
    } else {
      mnts.add(getConfiguration(event.getClusterSpec()).getString(DATA_DIRS_DEFAULT));
    }

    return mnts;
  }

}
