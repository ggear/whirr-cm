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

import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.whirr.cm.BaseTest;
import com.cloudera.whirr.cm.CmServerHandler;
import com.cloudera.whirr.cm.api.CmServerApi;
import com.cloudera.whirr.cm.api.CmServerApiLog;
import com.cloudera.whirr.cm.api.CmServerCluster;
import com.cloudera.whirr.cm.api.CmServerService;
import com.cloudera.whirr.cm.api.CmServerServiceType;
import com.google.common.collect.ImmutableMap;

public class CmServerApiTest implements BaseTest {

  private static final String CLUSTER_TAG = "whirr";

  private static String CM_IP = getSystemProperty("whirr.test.cm.ip", "164.177.149.71");
  private static int CM_PORT = Integer.valueOf(getSystemProperty("whirr.test.cm.port", "7180"));
  private static String CM_REPOS = getSystemProperty("whirr.test.cm.repos",
      "http://10.178.197.160/tmph3l7m2vv103/cloudera-repos/cdh4/parcels/4.2.0.10/" + ","
          + "http://10.178.197.160/tmph3l7m2vv103/cloudera-repos/impala/parcels/0.6.109/");

  private static Map<String, String> CM_CONFIG = ImmutableMap.of("REMOTE_PARCEL_REPO_URLS", CM_REPOS);

  private static CmServerApi api;

  private static String[] hostNames;

  @BeforeClass
  public static void provisionCluster() throws InterruptedException, IOException {
    Assert.assertNotNull(api = new CmServerApi(CM_IP, CM_PORT, CmServerHandler.CM_USER, CmServerHandler.CM_PASSWORD,
        new CmServerApiLog.CmServerApiLogSysOut()));
    Assert.assertTrue(api.initialise(CM_CONFIG).size() > 0);
    Set<String> hosts = api.hosts();
    Assert.assertNotNull(hosts);
    hosts.remove(CM_IP);
    Assert.assertTrue("Integration test cluster requires at least 4 (+ CM Server) nodes", hosts.size() > 3);
    hostNames = hosts.toArray(new String[] {});
    api.provision(getBasicCluster());
  }

  @Test
  public void testConfigure() throws InterruptedException, IOException {

    CmServerCluster cluster = getFullyLoadedCluster();
    api.configure(cluster);
    api.unconfigure(cluster);

  }

  @Test
  public void testConfigureAndStartFirst() throws InterruptedException, IOException {

    CmServerCluster cluster = getFullyLoadedCluster();
    api.configure(cluster);
    api.startFirst(cluster);
    api.stop(cluster);
    api.unconfigure(cluster);

  }

  @Test
  public void testConfigureAndStartFirstAndStart() throws InterruptedException, IOException {

    CmServerCluster cluster = getFullyLoadedCluster();
    api.configure(cluster);
    api.startFirst(cluster);
    api.stop(cluster);
    api.start(cluster);
    api.stop(cluster);
    api.unconfigure(cluster);

  }

  private static CmServerCluster getBasicCluster() throws IOException {

    CmServerCluster cluster = new CmServerCluster();
    cluster.add(new CmServerService(CmServerServiceType.HDFS_NAMENODE, CLUSTER_TAG, "1", hostNames[0]));
    for (int i = 1; i < hostNames.length; i++) {
      cluster.add(new CmServerService(CmServerServiceType.HDFS_DATANODE, CLUSTER_TAG, "" + i, hostNames[i]));
    }

    return cluster;
  }

  private static CmServerCluster getFullyLoadedCluster() throws IOException {

    CmServerCluster cluster = new CmServerCluster();
    cluster.add(new CmServerService(CmServerServiceType.HBASE_MASTER, CLUSTER_TAG, "1", hostNames[1]));
    cluster.add(new CmServerService(CmServerServiceType.HDFS_NAMENODE, CLUSTER_TAG, "1", hostNames[0]));
    cluster.add(new CmServerService(CmServerServiceType.HDFS_SECONDARY_NAMENODE, CLUSTER_TAG, "1", hostNames[0]));
    cluster.add(new CmServerService(CmServerServiceType.MAPREDUCE_JOB_TRACKER, CLUSTER_TAG, "1", hostNames[1]));
    for (int i = 1; i < hostNames.length; i++) {
      cluster.add(new CmServerService(CmServerServiceType.HBASE_REGIONSERVER, CLUSTER_TAG, "" + i, hostNames[i]));
      cluster.add(new CmServerService(CmServerServiceType.MAPREDUCE_TASK_TRACKER, CLUSTER_TAG, "" + i, hostNames[i]));
      cluster.add(new CmServerService(CmServerServiceType.HDFS_DATANODE, CLUSTER_TAG, "" + i, hostNames[i]));
      cluster.add(new CmServerService(CmServerServiceType.ZOOKEEPER_SERVER, CLUSTER_TAG, "" + i, hostNames[i]));
    }

    return cluster;
  }

  private static String getSystemProperty(String key, String value) {
    return System.getProperty(key) == null ? value : System.getProperty(key);
  }
}
