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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.whirr.cm.BaseTest;
import com.cloudera.whirr.cm.CmServerHandler;
import com.cloudera.whirr.cm.api.CmServerApi;
import com.cloudera.whirr.cm.api.CmServerCluster;
import com.cloudera.whirr.cm.api.CmServerService;
import com.cloudera.whirr.cm.api.CmServerServiceType;
import com.google.common.collect.ImmutableMap;

public class CmServerApiTest implements BaseTest {

  private static final String CLUSTER_TAG = "whirr-test";

  private String CM_IP = getSystemProperty("whirr.test.cm.ipt", "5-79-4-101.static.cloud-ips.co.uk");
  private int CM_PORT = Integer.valueOf(getSystemProperty("whirr.test.cm.port", "7180"));

  private Map<String, String> CM_CONFIG = ImmutableMap.of("REMOTE_PARCEL_REPO_URLS",
      "http://10.178.197.160/tmph3l7m2vv103/cloudera-repos/cdh4/parcels/4.2.0.10" + ","
          + "http://10.178.197.160/tmph3l7m2vv103/cloudera-repos/impala/parcels/0.6.109");

  private CmServerApi api;

  @Before
  public void initApi() throws InterruptedException, IOException {
    Assert.assertNotNull(api = new CmServerApi(CM_IP, CM_PORT, CmServerHandler.CM_USER, CmServerHandler.CM_PASSWORD));
    Assert.assertTrue(api.initialise(CM_CONFIG).size() > 0);
  }

  @Test
  public void testProvisionAndStart() throws InterruptedException, IOException {
    Set<String> hosts = api.hosts();
    Assert.assertNotNull(hosts);
    Assert.assertTrue("Integration test cluster requires at least 4 nodes", hosts.size() > 3);
    hosts.remove(CM_IP);
    String[] hostNames = hosts.toArray(new String[] {});
    CmServerCluster cluster = new CmServerCluster();
    cluster.add(new CmServerService(CmServerServiceType.NAMENODE, CLUSTER_TAG, "1", hostNames[0]));
    cluster.add(new CmServerService(CmServerServiceType.SECONDARYNAMENODE, CLUSTER_TAG, "1", hostNames[0]));
    for (int i = 1; i < hostNames.length; i++) {
      cluster.add(new CmServerService(CmServerServiceType.DATANODE, CLUSTER_TAG, "" + i, hostNames[i]));
    }
    try {
      api.provision(cluster);
      api.start(cluster);
    } finally {
      api.unprovision(cluster);
    }
  }

  private static String getSystemProperty(String key, String value) {
    return System.getProperty(key) == null ? value : System.getProperty(key);
  }
}
