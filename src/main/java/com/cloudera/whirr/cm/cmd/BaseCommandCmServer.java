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

import java.net.UnknownHostException;

import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;

import com.cloudera.whirr.cm.CmServerUtil;
import com.cloudera.whirr.cm.handler.CmServerHandler;
import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerCommand;
import com.cloudera.whirr.cm.server.CmServerException;

public abstract class BaseCommandCmServer extends BaseCommand {

  public BaseCommandCmServer(String name, String description, ClusterControllerFactory factory,
      ClusterStateStoreFactory stateStoreFactory) {
    super(name, description, factory, stateStoreFactory);
  }

  public BaseCommandCmServer(String name, String description, ClusterControllerFactory factory) {
    super(name, description, factory);
  }

  public abstract int run(CmServerCluster cluster, CmServerCommand serverCommand) throws Exception;

  @Override
  public int run(ClusterSpec clusterSpec, ClusterStateStore clusterStateStore, ClusterController clusterController)
      throws Exception {

    String serverHost = null;
    try {
      serverHost = CmServerUtil.getServerHost(clusterController.getInstances(clusterSpec, clusterStateStore));
    } catch (UnknownHostException exception) {
      throw new CmServerException("Could not find " + CmServerHandler.ROLE + " in template.");
    }

    CmServerCluster cluster = CmServerUtil.getCluster(clusterSpec,
        clusterController.getInstances(clusterSpec, clusterStateStore), new CmServerCluster());

    CmServerCommand command = CmServerCommand.get().host(serverHost).cluster(cluster)
        .client(clusterSpec.getClusterDirectory().getAbsolutePath());

    logger.logOperationStartedSync(getLabel());

    int returnInt = run(cluster, command);

    logger.logOperationFinishedSync(getLabel());

    return returnInt;
  }

}