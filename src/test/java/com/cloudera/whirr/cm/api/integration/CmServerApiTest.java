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
package com.cloudera.whirr.cm.api.integration;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.whirr.cm.api.CmServerApiException;
import com.cloudera.whirr.cm.api.CmServerService;
import com.cloudera.whirr.cm.api.CmServerService.CmServerServiceStatus;
import com.cloudera.whirr.cm.api.CmServerServiceType;

public class CmServerApiTest extends BaseTestIntegration {

  @Test
  public void testClean() throws CmServerApiException {
    Assert.assertTrue(true);
  }

  @Test
  public void testGetServiceHost() throws CmServerApiException {
    List<CmServerService> serviceHosts = api.getServiceHosts();
    Assert.assertEquals(clusterSize, serviceHosts.size());
    Assert.assertTrue(serviceHosts.size() > 2);
    Assert.assertEquals(serviceHosts.get(2).getHost(),
        api.getServiceHost(new CmServerService(serviceHosts.get(2).getHost(), null)).getHost());
    Assert.assertEquals(serviceHosts.get(2).getHost(),
        api.getServiceHost(new CmServerService((String) null, serviceHosts.get(2).getIp())).getHost());
    Assert.assertEquals(
        serviceHosts.get(2).getHost(),
        api.getServiceHost(
            new CmServerService((String) null, null, serviceHosts.get(2).getIp(), CmServerServiceStatus.UNKNOWN))
            .getHost());
    Assert.assertEquals(
        serviceHosts.get(2).getHost(),
        api.getServiceHost(
            new CmServerService("some-rubbish", "192.168.1.89", serviceHosts.get(2).getIp(),
                CmServerServiceStatus.UNKNOWN)).getHost());
    boolean caught = false;
    try {
      Assert.assertEquals(
          serviceHosts.get(2).getHost(),
          api.getServiceHost(
              new CmServerService("some-rubbish", "192.168.1.89", "192.168.1.90", CmServerServiceStatus.UNKNOWN))
              .getHost());
    } catch (CmServerApiException e) {
      caught = true;
    }
    Assert.assertTrue(caught);
    Assert.assertEquals(
        serviceHosts.get(2).getHost(),
        api.getServiceHost(
            new CmServerService("some-rubbish", "192.168.1.89", serviceHosts.get(2).getIp(),
                CmServerServiceStatus.UNKNOWN), serviceHosts).getHost());
  }

  @Test
  public void testGetServiceConfig() throws CmServerApiException {
    Assert.assertFalse(api.getServiceConfigs(cluster, DIR_CLIENT_CONFIG));
    Assert.assertTrue(api.configure(cluster));
    Assert.assertTrue(api.getServiceConfigs(cluster, DIR_CLIENT_CONFIG));
  }

  @Test
  public void testGetService() throws CmServerApiException {
    Assert.assertTrue(api.getServices(cluster).isEmpty());
    Assert.assertTrue(api.configure(cluster));  
    Assert.assertFalse(api.getServices(cluster).isEmpty());
    Assert.assertEquals(new CmServerService(CmServerServiceType.HDFS_NAMENODE, CLUSTER_TAG,
        CmServerService.NAME_QUALIFIER_DEFAULT, api.getService(cluster, CmServerServiceType.HDFS_NAMENODE).getHost()),
        api.getService(cluster, CmServerServiceType.HDFS_NAMENODE));
  }

  @Test
  public void testInitialise() throws CmServerApiException {
    Assert.assertTrue(api.initialise(CM_CONFIG).size() > 0);
  }

  @Test
  public void testProvision() throws CmServerApiException {
    Assert.assertFalse(api.provision(cluster));
  }

  @Test
  public void testConfigure() throws CmServerApiException {
    Assert.assertTrue(api.configure(cluster));
  }

  @Test
  public void testStart() throws CmServerApiException {
    Assert.assertTrue(api.start(cluster));
  }

  @Test
  public void testLifecycle() throws CmServerApiException {
    Assert.assertTrue(api.isProvisioned(cluster));
    Assert.assertTrue(api.unprovision(cluster));
    Assert.assertFalse(api.isProvisioned(cluster));
    Assert.assertFalse(api.unprovision(cluster));
    Assert.assertFalse(api.isProvisioned(cluster));
    Assert.assertFalse(api.isConfigured(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertFalse(api.stop(cluster));
    Assert.assertFalse(api.unconfigure(cluster));
    Assert.assertTrue(api.configure(cluster));
    Assert.assertTrue(api.isProvisioned(cluster));
    Assert.assertTrue(api.isConfigured(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertFalse(api.configure(cluster));
    Assert.assertTrue(api.isProvisioned(cluster));
    Assert.assertTrue(api.isConfigured(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertTrue(api.unprovision(cluster));
    Assert.assertFalse(api.isProvisioned(cluster));
    Assert.assertFalse(api.isConfigured(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertTrue(api.start(cluster));
    Assert.assertTrue(api.isProvisioned(cluster));
    Assert.assertTrue(api.isConfigured(cluster));
    Assert.assertTrue(api.isStarted(cluster));
    Assert.assertFalse(api.isStopped(cluster));
    Assert.assertFalse(api.start(cluster));
    Assert.assertTrue(api.isStarted(cluster));
    Assert.assertFalse(api.isStopped(cluster));
    Assert.assertFalse(api.start(cluster));
    Assert.assertTrue(api.isStarted(cluster));
    Assert.assertFalse(api.isStopped(cluster));
    Assert.assertTrue(api.stop(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertFalse(api.stop(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertFalse(api.stop(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertFalse(api.stop(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertTrue(api.start(cluster));
    Assert.assertTrue(api.isStarted(cluster));
    Assert.assertFalse(api.isStopped(cluster));
    Assert.assertTrue(api.unconfigure(cluster));
    Assert.assertTrue(api.isProvisioned(cluster));
    Assert.assertFalse(api.isConfigured(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
    Assert.assertFalse(api.unconfigure(cluster));
    Assert.assertTrue(api.isProvisioned(cluster));
    Assert.assertFalse(api.isConfigured(cluster));
    Assert.assertFalse(api.isStarted(cluster));
    Assert.assertTrue(api.isStopped(cluster));
  }

}
