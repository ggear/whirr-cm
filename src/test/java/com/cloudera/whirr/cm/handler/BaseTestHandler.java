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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.BaseServiceDryRunTest;
import org.junit.Before;
import org.junit.BeforeClass;

import com.cloudera.whirr.cm.BaseTest;
import com.cloudera.whirr.cm.CmServerClusterInstance;
import com.cloudera.whirr.cm.server.CmServer;
import com.cloudera.whirr.cm.server.CmServerCluster;
import com.cloudera.whirr.cm.server.CmServerException;
import com.cloudera.whirr.cm.server.CmServerService;
import com.cloudera.whirr.cm.server.CmServerServiceType;
import com.cloudera.whirr.cm.server.impl.CmServerFactory;
import com.cloudera.whirr.cm.server.impl.CmServerLog;
import com.google.common.collect.ImmutableMap;
import com.jcraft.jsch.JSchException;

public abstract class BaseTestHandler extends BaseServiceDryRunTest implements BaseTest {

  @BeforeClass
  public static void mockCmServer() {

    CmServerFactory factory = mock(CmServerFactory.class);
    CmServerClusterInstance.getFactory(factory);

    when(factory.getCmServer(anyString(), anyInt(), anyString(), anyString(), (CmServerLog) any())).thenReturn(
        new CmServer() {

          @Override
          public boolean getServiceConfigs(CmServerCluster cluster, File directory) throws CmServerException {
            return false;
          }

          @Override
          public List<CmServerService> getServiceHosts() throws CmServerException {
            return null;
          }

          @Override
          public CmServerService getServiceHost(CmServerService service) throws CmServerException {
            return null;
          }

          @Override
          public CmServerService getServiceHost(CmServerService service, List<CmServerService> services)
              throws CmServerException {
            return null;
          }

          @Override
          public CmServerCluster getServices(CmServerCluster cluster) throws CmServerException {
            return null;
          }

          @Override
          public CmServerService getService(CmServerCluster cluster, CmServerServiceType type) throws CmServerException {
            return null;
          }

          @Override
          public CmServerCluster getServices(CmServerCluster cluster, CmServerServiceType type)
              throws CmServerException {
            return null;
          }

          @Override
          public boolean isProvisioned(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean isConfigured(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean isStarted(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean isStopped(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public Map<String, String> initialise(Map<String, String> config) throws CmServerException {
            return null;
          }

          @Override
          public boolean provision(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean configure(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean start(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean stop(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean unconfigure(CmServerCluster cluster) throws CmServerException {
            return false;
          }

          @Override
          public boolean unprovision(CmServerCluster cluster) throws CmServerException {
            return false;
          }
        });

  }

  @Override
  public ClusterSpec newClusterSpecForProperties(Map<String, String> properties) throws IOException,
      ConfigurationException, JSchException {
    ClusterSpec clusterSpec = super.newClusterSpecForProperties(ImmutableMap.<String, String> builder()
        .putAll(properties).put(CONFIG_WHIRR_USER, CLUSTER_USER).put(CONFIG_WHIRR_NAME, CONFIG_WHIRR_NAME_DEFAULT)
        .build());
    clusterSpec.setPrivateKey(FILE_KEY_PRIVATE);
    clusterSpec.setPublicKey(FILE_KEY_PUBLIC);
    return clusterSpec;
  }

  @Before
  public void clearClusterSingleton() {
    CmServerClusterInstance.getCluster(true);
  }

}
