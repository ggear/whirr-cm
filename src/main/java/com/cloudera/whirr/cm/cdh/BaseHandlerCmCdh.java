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
package com.cloudera.whirr.cm.cdh;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.whirr.service.ClusterActionEvent;

import com.cloudera.whirr.cm.BaseHandler;
import com.cloudera.whirr.cm.api.CmServerCluster;
import com.cloudera.whirr.cm.api.CmServerService;
import com.cloudera.whirr.cm.api.CmServerServiceType;

public abstract class BaseHandlerCmCdh extends BaseHandler {

  public abstract CmServerServiceType getType();

  private static ConcurrentMap<String, CmServerServiceType> roleToType = new ConcurrentHashMap<String, CmServerServiceType>();

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeBootstrap(event);
    CmServerClusterSingleton.getInstance().add(new CmServerService(getType()));
    roleToType.putIfAbsent(getRole(), getType());
  }

  public static CmServerServiceType getType(String role) {
    return roleToType.get(role);
  }

  public static class CmServerClusterSingleton {

    private static CmServerCluster _instance;

    private CmServerClusterSingleton() {
    }

    public static synchronized CmServerCluster getInstance() {
      return _instance == null ? (_instance = new CmServerCluster()) : _instance;
    }

  }

}
