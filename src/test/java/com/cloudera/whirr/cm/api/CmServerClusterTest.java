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
package com.cloudera.whirr.cm.api;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.whirr.cm.BaseTest;

public class CmServerClusterTest implements BaseTest {

  private static final String CLUSTER_TAG = "whirr-test";

  private CmServerCluster cluster;

  @Before
  public void setupCluster() throws IOException {
    cluster = new CmServerCluster();
    cluster.add(new CmServerService(CmServerServiceType.DATANODE, CLUSTER_TAG, "2", "host-2"));
    cluster.add(new CmServerService(CmServerServiceType.NAMENODE, CLUSTER_TAG, "1", "host_1"));
    cluster.add(new CmServerService(CmServerServiceType.DATANODE, CLUSTER_TAG, "1", "host_1"));
    cluster.add(new CmServerService(CmServerServiceType.SECONDARYNAMENODE, CLUSTER_TAG, "1", "host_1"));
    cluster.add(new CmServerService(CmServerServiceType.DATANODE, CLUSTER_TAG, "3", "host-3"));
    cluster.add(new CmServerService(CmServerServiceType.DATANODE, CLUSTER_TAG, "4", "host-4"));
    cluster.add(new CmServerService(CmServerServiceType.REGIONSERVER, CLUSTER_TAG, "1", "host-4"));
    cluster.add(new CmServerService(CmServerServiceType.IMPALADEAMON, CLUSTER_TAG, "1", "host-4"));
  }

  @Test
  public void testGetTypes() throws InterruptedException, IOException {
    Assert.assertArrayEquals(new CmServerServiceType[] { CmServerServiceType.HDFS, CmServerServiceType.HBASE,
        CmServerServiceType.IMPALA }, cluster.getServiceTypes().toArray());
  }

  @Test
  public void testGetServices() throws InterruptedException, IOException {
    Assert.assertEquals(8, cluster.getServices(CmServerServiceType.CLUSTER).size());
    Assert.assertEquals(6, cluster.getServices(CmServerServiceType.HDFS).size());
    Assert.assertEquals(1, cluster.getServices(CmServerServiceType.NAMENODE).size());
    Assert.assertEquals(4, cluster.getServices(CmServerServiceType.DATANODE).size());
    Assert.assertEquals(0, cluster.getServices(CmServerServiceType.CLIENT).size());
  }

  @Test
  public void testGetService() throws InterruptedException, IOException {
    Assert.assertNotNull(cluster.getService(CmServerServiceType.CLUSTER));
    Assert.assertNotNull(cluster.getService(CmServerServiceType.HDFS));
    Assert.assertNotNull(cluster.getService(CmServerServiceType.NAMENODE));
    Assert.assertNotNull(cluster.getService(CmServerServiceType.DATANODE));
    boolean caught = false;
    try {
      Assert.assertNotNull(cluster.getService(CmServerServiceType.CLIENT));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue(caught);
  }

  @Test
  public void testGetNames() throws InterruptedException, IOException {
    Assert.assertEquals(CLUSTER_TAG + "_" + CmServerServiceType.CLUSTER.toString().toLowerCase() + "_1",
        cluster.getServiceName(CmServerServiceType.CLUSTER));
    Assert.assertEquals(CLUSTER_TAG + "_" + CmServerServiceType.HDFS.toString().toLowerCase() + "_1",
        cluster.getServiceName(CmServerServiceType.HDFS));
    Assert.assertEquals(CLUSTER_TAG + "_" + CmServerServiceType.NAMENODE.toString().toLowerCase() + "_1",
        cluster.getServiceName(CmServerServiceType.NAMENODE));
    Assert.assertEquals(CLUSTER_TAG + "_" + CmServerServiceType.DATANODE.toString().toLowerCase() + "_1",
        cluster.getServiceName(CmServerServiceType.DATANODE));
    Assert.assertEquals(CLUSTER_TAG + "_" + CmServerServiceType.CLIENT.toString().toLowerCase() + "_1",
        cluster.getServiceName(CmServerServiceType.CLIENT));
    boolean caught = false;
    try {
      Assert.assertEquals(CLUSTER_TAG + "_" + CmServerServiceType.CLUSTER.toString().toLowerCase() + "_1",
          new CmServerCluster().getServiceName(CmServerServiceType.CLUSTER));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue(caught);
  }

  @Test
  public void testGetHosts() throws InterruptedException, IOException {
    Assert.assertEquals(4, cluster.getServiceHosts(CmServerServiceType.CLUSTER).size());
    Assert.assertEquals(4, cluster.getServiceHosts(CmServerServiceType.HDFS).size());
    Assert.assertEquals(1, cluster.getServiceHosts(CmServerServiceType.NAMENODE).size());
    Assert.assertEquals(4, cluster.getServiceHosts(CmServerServiceType.DATANODE).size());
    Assert.assertEquals(0, cluster.getServiceHosts(CmServerServiceType.CLIENT).size());
  }

}
