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

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.whirr.cm.api.CmServerApiException;
import com.cloudera.whirr.cm.api.CmServerServiceType;

public class CmServerShellTest implements BaseTest {

  private static final File FILE_CONFIG_EG_EXISTANT = new File("./target");
  private static final File FILE_CONFIG_EG_NONEXISTANT = new File("/some/non/existant/892342");

  @Test
  public void testGetClusterConfig() throws IOException, CmServerApiException {

    Assert.assertEquals(24,
        CmServerShell.getCluster(FILE_CONFIG_EG, FILE_CONFIG_EG_EXISTANT).getServices(CmServerServiceType.CLUSTER)
            .size());
    Assert.assertEquals(24,
        CmServerShell.getCluster(FILE_CONFIG_EG, FILE_CONFIG_EG_NONEXISTANT).getServices(CmServerServiceType.CLUSTER)
            .size());
    Assert.assertEquals(5,
        CmServerShell.getCluster(FILE_CONFIG_EG, FILE_CONFIG_EG_NONEXISTANT).getServices(CmServerServiceType.HDFS)
            .size());
    Assert.assertEquals(
        3,
        CmServerShell.getCluster(FILE_CONFIG_EG, FILE_CONFIG_EG_NONEXISTANT)
            .getServices(CmServerServiceType.HDFS_DATANODE).size());

  }

  @Test(expected = CmServerApiException.class)
  public void testGetClusterConfigInvalid1() throws IOException, CmServerApiException {

    Assert.assertEquals(24, CmServerShell.getCluster(FILE_CONFIG_EG_NONEXISTANT, FILE_CONFIG_EG_NONEXISTANT)
        .getServices(CmServerServiceType.CLUSTER).size());

  }

  @Test
  public void testGetClusterInstances() throws IOException, CmServerApiException {

    Assert.assertEquals(24,
        CmServerShell.getCluster(FILE_CONFIG_EG, FILE_HOME_EG).getServices(CmServerServiceType.CLUSTER).size());
    Assert.assertEquals(5, CmServerShell.getCluster(FILE_CONFIG_EG, FILE_HOME_EG).getServices(CmServerServiceType.HDFS)
        .size());
    Assert.assertEquals(3,
        CmServerShell.getCluster(FILE_CONFIG_EG, FILE_HOME_EG).getServices(CmServerServiceType.HDFS_DATANODE).size());

  }

  @Test(expected = CmServerApiException.class)
  public void testGetClusterInstancesInvalid1() throws IOException, CmServerApiException {

    Assert.assertEquals(24,
        CmServerShell.getCluster(FILE_CONFIG_EG_NONEXISTANT, FILE_HOME_EG).getServices(CmServerServiceType.CLUSTER)
            .size());

  }

}
