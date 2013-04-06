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

import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.state.ClusterStateStoreFactory;

import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerCommand;
import com.cloudera.whirr.cm.server.CmServerService;
import com.cloudera.whirr.cm.server.CmServerServiceType;

public class CmServerListServicesCommand extends BaseCommandCmServer {

  public static final String NAME = "list-services";
  public static final String DESCRIPTION = "List services in a cluster.";

  public CmServerListServicesCommand() throws IOException {
    this(new ClusterControllerFactory());
  }

  public CmServerListServicesCommand(ClusterControllerFactory factory) {
    this(factory, new ClusterStateStoreFactory());
  }

  public CmServerListServicesCommand(ClusterControllerFactory factory, ClusterStateStoreFactory stateStoreFactory) {
    super(NAME, DESCRIPTION, factory, stateStoreFactory);
  }

  @Override
  public int run(ClusterSpec specification, CmServerCluster cluster, CmServerCommand serverCommand) throws Exception {

    CmServerCluster commandReturnCluster = serverCommand.command("services").executeCluster();

    logger.logOperationInProgressSync(getLabel(), "CM SERVER");
    logger.logOperationInProgressSync(getLabel(), "  http://" + cluster.getServer() + ":7180");
    logger.logOperationInProgressSync(getLabel(), "  ssh -o StrictHostKeyChecking=no " + specification.getClusterUser()
        + "@" + cluster.getServer());

    if (!cluster.getAgents().isEmpty()) {
      logger.logOperationInProgressSync(getLabel(), "CM AGENTS");
    }
    for (String cmAgent : cluster.getAgents()) {
      logger.logOperationInProgressSync(getLabel(),
          "  ssh -o StrictHostKeyChecking=no " + specification.getClusterUser() + "@" + cmAgent);
    }

    if (!cluster.getNodes().isEmpty()) {
      logger.logOperationInProgressSync(getLabel(), "CM NODES");
    }
    for (String cmNode : cluster.getNodes()) {
      logger.logOperationInProgressSync(getLabel(),
          "  ssh -o StrictHostKeyChecking=no " + specification.getClusterUser() + "@" + cmNode);
    }

    if (commandReturnCluster.isEmpty()) {
      logger.logOperationInProgressSync(getLabel(), "NO CDH SERVICES");
    } else {
      for (CmServerServiceType type : commandReturnCluster.getServiceTypes()) {
        logger.logOperationInProgressSync(getLabel(), type.toString());
        for (CmServerService service : commandReturnCluster.getServices(type)) {
          logger.logOperationInProgressSync(getLabel(), "  " + service.getName() + "@" + service.getHost() + "="
              + service.getStatus());
        }
      }
    }

    return 0;
  }

}