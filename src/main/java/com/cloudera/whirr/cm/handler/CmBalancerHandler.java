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

import java.io.IOException;

import org.apache.whirr.Cluster;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager.Rule;
import org.jclouds.scriptbuilder.domain.Statement;

import com.cloudera.whirr.cm.CmConstants;
import com.cloudera.whirr.cm.CmServerClusterInstance;
import com.cloudera.whirr.cm.handler.cdh.CmCdhImpalaDaemonHandler;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

public class CmBalancerHandler extends ClusterActionHandlerSupport implements CmConstants {

  public static final String ROLE = "cm-balancer";

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeStart(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeStart(event);
    final String portBalancerImpala = CmServerClusterInstance.getConfiguration(event.getClusterSpec()).getString(
        CmConstants.CONFIG_WHIRR_INTERNAL_PORT_BALANCER_IMPALA);
    addStatement(event, call("retry_helpers"));
    addStatement(
        event,
        call(
            "configure_cm_balancer",
            "-b",
            portBalancerImpala,
            "-h",
            Joiner.on(',').join(
                Lists.transform(
                    Lists.newArrayList(event.getCluster().getInstancesMatching(
                        RolePredicates.role(CmCdhImpalaDaemonHandler.ROLE))), new Function<Cluster.Instance, String>() {
                      @Override
                      public String apply(Cluster.Instance input) {
                        return input.getPrivateIp() + ":" + portBalancerImpala;
                      }
                    }))));
    if (CmServerClusterInstance.getConfiguration(event.getClusterSpec()).getBoolean(CONFIG_WHIRR_FIREWALL_ENABLE, true)) {
      event.getFirewallManager().addRules(Rule.create().destination(role(getRole())).ports(Integer.parseInt(portBalancerImpala)));
      for (Statement portIngressStatement : event.getFirewallManager().getRulesAsStatements()) {
        addStatement(event, portIngressStatement);
      }
    }
  }

}
