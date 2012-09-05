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

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;

public class CmAgentHandler extends BaseHandler {

	private static final String CM_SERVER_PORT = "7182";
	
  public static final String ROLE = "cmagent";
  private static final String PORTS = "cmagent.ports";
  
  @Override public String getRole() { return ROLE; }
  
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
  	super.beforeBootstrap(event);
  	addStatement(event, call("install_cm"));
  	addStatement(event, call("install_cm_agent"));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
		try {
			addStatement(
			  event,
			  call("configure_cm_agent", "-h", event.getCluster()
			    .getInstanceMatching(role(CmServerHandler.ROLE)).getPublicHostName(),
			    "-p", CM_SERVER_PORT));
		} catch (NoSuchElementException e) {
			addStatement(event,
			  call("configure_cm_agent", "-h", "localhost", "-p", CM_SERVER_PORT));
		}
    List<?> ports = getConfiguration(event.getClusterSpec()).getList(PORTS);
    if (ports != null) {
      for (Object port : ports) {
        event.getFirewallManager().addRule(
            Rule.create().destination(role(ROLE)).port(Integer.parseInt(port.toString()))
        );
      }
    }
  }

}
