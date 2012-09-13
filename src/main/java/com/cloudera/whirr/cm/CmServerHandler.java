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
import static org.jclouds.scriptbuilder.domain.Statements.createOrOverwriteFile;

import java.io.IOException;
import java.util.Set;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

public class CmServerHandler extends BaseHandler {

	public static final String ROLE = "cmserver";

	private static final String LICENSE_FILE = "cm-license.txt";
	private static final String CONFIG_FILE = "cm-config.json";

	private static final int CLIENT_PORT = 7180;

	@Override
	public String getRole() {
		return ROLE;
	}

	@Override
	protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
		super.beforeBootstrap(event);
		addStatement(event, call("install_cm"));
		addStatement(event, call("install_cm_server"));
	}

	@Override
	protected void beforeConfigure(ClusterActionEvent event) throws IOException,
	  InterruptedException {
		super.beforeConfigure(event);
		try {
			addStatement(
			  event,
			  createOrOverwriteFile(
			    "/tmp/" + LICENSE_FILE,
			    Splitter.on('\n').split(
			      CharStreams.toString(Resources.newReaderSupplier(
			        Resources.getResource(LICENSE_FILE), Charsets.UTF_8)))));
		} catch (IllegalArgumentException e) {

		}
		try {
			addStatement(
			  event,
			  createOrOverwriteFile(
			    "/tmp/" + CONFIG_FILE,
			    Splitter.on('\n').split(
			      CharStreams.toString(Resources.newReaderSupplier(
			        Resources.getResource(CONFIG_FILE), Charsets.UTF_8)))));
		} catch (IllegalArgumentException e) {

		}
		addStatement(event, call("configure_cm_server"));
		event.getFirewallManager().addRule(
		  Rule.create().destination(role(ROLE)).port(CLIENT_PORT));
	}

	@Override
	protected void afterConfigure(ClusterActionEvent event) throws IOException,
	  InterruptedException {
		Cluster cluster = event.getCluster();
		Instance master = cluster.getInstanceMatching(role(ROLE));
		String masterAddress = master.getPublicAddress().getHostName();
		System.out.printf(
		  "Cloudera Manager Admin Console available at http://%s:%s\n",
		  masterAddress, CLIENT_PORT);

		System.out
		  .println("Nodes in cluster (copy into text area when setting up the cluster):");
		Set<Instance> nodes = cluster
		  .getInstancesMatching(role(CmNodeHandler.ROLE));
		for (Instance i : nodes) {
			System.out.println(i.getPrivateIp());
		}
		System.out.println();

		System.out
		  .println("Authentication Method (choose \"All hosts accept same public key\"):");
		System.out.printf("User name (choose \"another user\"): %s\n",
		  System.getProperty("user.name"));
		String privateKey = event.getClusterSpec().getPrivateKeyFile()
		  .getCanonicalPath();
		System.out.printf("Public key file: %s\n", privateKey + ".pub");
		System.out.printf("Private key file: %s (empty passphrase)\n", privateKey);
		System.out.println();

		if (nodes.size() > 0) {
			// assumes set has stable ordering
			System.out.printf("Hue UI will be available at http://%s:8888\n",
			  Iterables.get(nodes, 0).getPublicHostName());
		}
	}
}
