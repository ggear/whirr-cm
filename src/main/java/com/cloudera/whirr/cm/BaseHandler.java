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

import static org.jclouds.scriptbuilder.domain.Statements.call;
import static org.jclouds.scriptbuilder.domain.Statements.createOrOverwriteFile;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

public abstract class BaseHandler extends ClusterActionHandlerSupport {

	protected final static String CM_CONFIG_IMPORT_FILE = "collect_existing_service_data.py";
	protected final static String CM_CONFIG_IMPORT_PATH = "functions/cmf/";

	protected Configuration getConfiguration(ClusterSpec spec) throws IOException {
		return getConfiguration(spec, "whirr-cm-default.properties");
	}

	@Override
	protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
		addStatement(event, call("configure_hostnames"));
		addStatement(event, call("retry_helpers"));
		addStatement(
		  event,
		  call(getInstallFunction(getConfiguration(event.getClusterSpec()), "java",
		    "install_openjdk")));
		addStatement(event, call("install_cdh_hadoop"));
		addStatement(event, call("install_cm_config_import"));
	}

	@Override
	protected void beforeConfigure(ClusterActionEvent event) throws IOException,
	  InterruptedException {
		addStatement(
		  event,
		  createOrOverwriteFile(
		    "/tmp/" + CM_CONFIG_IMPORT_FILE,
		    Splitter.on('\n').split(
		      CharStreams.toString(Resources.newReaderSupplier(Resources
		        .getResource(CM_CONFIG_IMPORT_PATH + CM_CONFIG_IMPORT_FILE),
		        Charsets.UTF_8)))));
	}

}
