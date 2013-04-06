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

import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.hadoop.VolumeManager;

import com.cloudera.whirr.cm.server.impl.CmServerLog;
import com.google.common.collect.Iterables;

public abstract class BaseHandlerCm extends BaseHandler {

  public static final String DATA_DIRS_ROOT = "cm.data.dirs.root";
  public static final String DATA_DIRS_DEFAULT = "cm.data.dirs.default";

  private static final CmServerLog logger = new CmServerLog.CmServerLogSysOut(LOG_TAG_WHIRR_HANDLER, false);

  protected Map<String, String> deviceMappings = new HashMap<String, String>();

  protected String getInstanceId() {
    return getRole() + "-instance-id";
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    logHeaderHandlerLifecycle("HostBeforeBootstrap");
    super.beforeBootstrap(event);
    addStatement(event, call("configure_hostnames"));
    addStatement(event, call("retry_helpers"));
  }

  @Override
  protected void afterBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    logHeaderHandlerLifecycle("HostAfterBootstrap");
    super.afterBootstrap(event);
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    logHeaderHandlerLifecycle("HostBeforeConfigure");
    super.beforeConfigure(event);
    addStatement(event, call("retry_helpers"));
    if (getConfiguration(event.getClusterSpec()).getString(DATA_DIRS_ROOT) == null) {
      getDeviceMappings(event);
      String devMappings = VolumeManager.asString(deviceMappings);
      addStatement(event, call("prepare_all_disks", "'" + devMappings + "'"));
    }
    logHeaderHandlerLifecycle("HostPostConfigure");
  }

  public Map<String, String> getDeviceMappings(ClusterActionEvent event) {
    if (deviceMappings.isEmpty()) {
      Instance prototype = Iterables.getFirst(event.getCluster().getInstances(), null);
      if (prototype == null) {
        throw new IllegalStateException("No instances found.");
      }
      VolumeManager volumeManager = new VolumeManager();
      deviceMappings.putAll(volumeManager.getDeviceMappings(event.getClusterSpec(), prototype));
    }

    return deviceMappings;
  }

  private void logHeaderHandlerLifecycle(String operation) {
    logHeader(operation);
    logLineItem(operation, "Role:");
    logLineItemDetail(operation, getRole());
  }

  public void logHeader(String operation) {
    logger.logSpacer();
    logger.logSpacerDashed();
    logger.logOperation(operation, "");
    logger.logSpacerDashed();
  }

  public void logFooter() {
    logger.logSpacer();
    logger.logSpacerDashed();
  }

  public void logLineItem(String operation, String detail) {
    logger.logSpacer();
    logger.logOperationInProgressSync(operation, detail);
  }

  public void logLineItemDetail(String operation, String detail) {
    logger.logOperationInProgressSync(operation, detail);
  }

  public void logException(String message, Throwable throwable) {
    logger.logSpacer();
    logger.logOperationStackTrace(throwable);
    logger.logSpacer();
    logger.logOperation("", message);
  }
}
