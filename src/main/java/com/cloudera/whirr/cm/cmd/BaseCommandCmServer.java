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
package com.cloudera.whirr.cm.cmd;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;

import com.cloudera.whirr.cm.handler.CmAgentHandler;
import com.cloudera.whirr.cm.handler.CmNodeHandler;
import com.cloudera.whirr.cm.handler.CmServerHandler;
import com.cloudera.whirr.cm.handler.cdh.BaseHandlerCmCdh;
import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerCommand;
import com.cloudera.whirr.cm.server.CmServerException;
import com.cloudera.whirr.cm.server.CmServerService;
import com.cloudera.whirr.cm.server.CmServerServiceType;
import com.google.common.base.Splitter;

public abstract class BaseCommandCmServer extends BaseCommand {

  public BaseCommandCmServer(String name, String description, ClusterControllerFactory factory,
      ClusterStateStoreFactory stateStoreFactory) {
    super(name, description, factory, stateStoreFactory);
  }

  public BaseCommandCmServer(String name, String description, ClusterControllerFactory factory) {
    super(name, description, factory);
  }

  public boolean isRoleFilterable() {
    return false;
  }

  public abstract int run(String user, String cmServer, List<String> cmAgents, List<String> cmNodes,
      CmServerCluster cluster, CmServerCommand serverCommand) throws Exception;

  private OptionSpec<String> rolesOption = isRoleFilterable() ? parser.accepts("roles", "Cluster roles to target")
      .withRequiredArg().ofType(String.class) : null;

  @Override
  public int run(OptionSet optionSet, ClusterSpec clusterSpec, ClusterStateStore clusterStateStore,
      ClusterController clusterController) throws Exception {

    logger.logOperationStartedSync(getLabel());

    Set<String> roles = new HashSet<String>();
    if (isRoleFilterable() && optionSet.hasArgument(rolesOption)) {
      for (String role : Splitter.on(",").split(optionSet.valueOf(rolesOption))) {
        roles.add(role);
      }
    }

    String server = null;
    List<String> agents = new ArrayList<String>();
    List<String> nodes = new ArrayList<String>();
    CmServerCluster cluster = new CmServerCluster();
    for (Instance instance : clusterController.getInstances(clusterSpec, clusterStateStore)) {
      for (String role : instance.getRoles()) {
        if (role.equals(CmServerHandler.ROLE)) {
          server = instance.getPublicHostName();
        } else if (role.equals(CmAgentHandler.ROLE)) {
          agents.add(instance.getPublicHostName());
        } else if (role.equals(CmNodeHandler.ROLE)) {
          agents.add(instance.getPublicHostName());
        } else {
          CmServerServiceType type = BaseHandlerCmCdh.getRoleToTypeGlobal().get(role);
          if (type != null && (roles == null || roles.isEmpty() || roles.contains(role))) {
            CmServerService service = new CmServerService(type, clusterSpec.getConfiguration().getString(
                CONFIG_WHIRR_NAME, CONFIG_WHIRR_NAME_DEFAULT), "" + (cluster.getServices(type).size() + 1),
                instance.getPublicHostName(), instance.getPublicIp(), instance.getPrivateIp());
            cluster.add(service);
          }
        }
      }
    }

    if (server == null) {
      throw new CmServerException("Could not find " + CmServerHandler.ROLE + ".");
    }
    if (agents.isEmpty() && nodes.isEmpty()) {
      throw new CmServerException("Could not find any " + CmAgentHandler.ROLE + "'s or " + CmNodeHandler.ROLE + "'s.");
    }
    if (cluster.isEmpty()) {
      throw new CmServerException("No appropriate roles found to target.");
    }

    CmServerCommand command = CmServerCommand.get().host(server).cluster(cluster)
        .client(clusterSpec.getClusterDirectory().getAbsolutePath());

    int returnInt = run(clusterSpec.getClusterUser(), server, agents, nodes, cluster, command);

    logger.logOperationFinishedSync(getLabel());

    return returnInt;
  }

  @Override
  public void printUsage(PrintStream stream) throws IOException {
    if (isRoleFilterable()) {
      stream.println("Usage: whirr " + getName() + " [OPTIONS] [--roles role1,role2]");
      stream.println();
      parser.printHelpOn(stream);
    } else {
      super.printUsage(stream);
    }
  }

}