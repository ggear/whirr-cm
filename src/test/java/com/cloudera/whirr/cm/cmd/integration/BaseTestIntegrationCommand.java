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
package com.cloudera.whirr.cm.cmd.integration;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;

import com.cloudera.whirr.cm.BaseTestIntegration;
import com.cloudera.whirr.cm.server.CmServerCommand;
import com.cloudera.whirr.cm.server.CmServerException;

public abstract class BaseTestIntegrationCommand extends BaseTestIntegration {

  protected CmServerCommand command;
  protected String user = "whirr";
  protected String server = null;
  protected List<String> agents = new ArrayList<String>();
  protected List<String> nodes = new ArrayList<String>();

  @Override
  @Before
  public void provisionCluster() throws CmServerException {
    super.provisionCluster();
    command = CmServerCommand.get().host(CM_HOST).cluster(cluster).client(DIR_CLIENT_CONFIG.getAbsolutePath());
    server = CM_HOST;
    agents.addAll(hosts);
  }

}
