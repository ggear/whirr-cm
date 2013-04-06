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
import java.util.HashSet;
import java.util.Set;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;

import com.cloudera.whirr.cm.CmServerClusterInstance;
import com.cloudera.whirr.cm.handler.CmAgentHandler;
import com.cloudera.whirr.cm.handler.CmNodeHandler;
import com.cloudera.whirr.cm.handler.CmServerHandler;
import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerCommand;
import com.cloudera.whirr.cm.server.CmServerException;
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

  public abstract int run(ClusterSpec specification, CmServerCluster cluster, CmServerCommand serverCommand) throws Exception;

  private OptionSpec<String> rolesOption = isRoleFilterable() ? parser.accepts("roles", "Cluster roles to target")
      .withRequiredArg().ofType(String.class) : null;

  @Override
  public int run(OptionSet optionSet, ClusterSpec specification, ClusterStateStore clusterStateStore,
      ClusterController clusterController) throws Exception {

    logger.logOperationStartedSync(getLabel());

    Set<String> roles = new HashSet<String>();
    if (isRoleFilterable() && optionSet.hasArgument(rolesOption)) {
      for (String role : Splitter.on(",").split(optionSet.valueOf(rolesOption))) {
        roles.add(role);
      }
    }

    CmServerCluster cluster = CmServerClusterInstance.getInstance(specification,
        clusterController.getInstances(specification, clusterStateStore), roles);

    if (cluster.getServer() == null) {
      throw new CmServerException("Could not find " + CmServerHandler.ROLE + ".");
    }
    if (cluster.getAgents().isEmpty() && cluster.getNodes().isEmpty()) {
      throw new CmServerException("Could not find any " + CmAgentHandler.ROLE + "'s or " + CmNodeHandler.ROLE + "'s.");
    }
    if (cluster.isEmpty()) {
      throw new CmServerException("No appropriate roles found to target.");
    }

    CmServerCommand command = CmServerCommand.get().host(cluster.getServer()).cluster(cluster)
        .client(specification.getClusterDirectory().getAbsolutePath());

    int returnInt = run(specification, cluster, command);

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