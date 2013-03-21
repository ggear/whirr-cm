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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class CmServerCluster {

  private static String CM_TAG_PARENT = "1";
  private static String CM_HOST_PARENT = "*";

  private Map<CmServerServiceType, List<CmServerService>> services = new HashMap<CmServerServiceType, List<CmServerService>>();

  public CmServerCluster() {
  }

  public synchronized void add(CmServerServiceType type) throws IOException {
    switch (type) {
    case NAMENODE:
      if (services.containsKey(type.getParent()) && services.get(type.getParent()).contains(type)) {
        throw new IOException("Illegal cluster topology: multiple [" + type + "] roles specified");
      }
    default:
      if (!services.containsKey(type.getParent())) {
        services.put(type.getParent(), new ArrayList<CmServerService>());
      }
    }
  }

  public synchronized void add(CmServerService service) throws IOException {
    switch (service.getType()) {
    default:
      add(service.getType());
      services.get(service.getType().getParent()).add(service);
    }
  }

  public synchronized Set<CmServerServiceType> getServiceTypes() {
    return new TreeSet<CmServerServiceType>(services.keySet());
  }

  public synchronized CmServerService getService(CmServerServiceType type) throws IOException {
    List<CmServerService> serviceCopy = getServices(type);
    if (serviceCopy.size() == 0) {
      throw new IOException("Could not find service with type [" + type + "]");
    }
    return serviceCopy.get(0);
  }

  public synchronized List<CmServerService> getServices(CmServerServiceType type) {
    // TODO: Do Deep Copy
    List<CmServerService> servicesCopy = new ArrayList<CmServerService>();
    if (type.equals(CmServerServiceType.CLUSTER)) {
      for (CmServerServiceType serviceType : services.keySet()) {
        servicesCopy.addAll(services.get(serviceType));
      }
    } else if (services.containsKey(type)) {
      servicesCopy.addAll(services.get(type));
    } else if (services.containsKey(type.getParent())) {
      for (CmServerService service : services.get(type.getParent())) {
        if (service.getType().equals(type)) {
          servicesCopy.add(service);
        }
      }
    }
    return servicesCopy;
  }

  public synchronized String getServiceName(CmServerServiceType type) throws IOException {
    if (services.get(type) != null) {
      CmServerService service = services.get(type).get(0);
      if (service.getType().equals(type)) {
        return service.getName();
      } else {
        return new CmServerService(type, service.getTag(), CM_TAG_PARENT, CM_HOST_PARENT).getName();
      }
    } else {
      List<CmServerService> servicesChild = null;
      if (!services.isEmpty() && !(servicesChild = services.get(getServiceTypes().iterator().next())).isEmpty()) {
        return new CmServerService(type, servicesChild.get(0).getTag(), CM_TAG_PARENT, CM_HOST_PARENT).getName();
      }
    }
    throw new IOException("Cannot determine service name, cluster is empty");
  }

  public synchronized Set<String> getServiceHosts(CmServerServiceType type) throws IOException {
    Set<String> hosts = new HashSet<String>();
    if (type.equals(CmServerServiceType.CLUSTER)) {
      for (CmServerServiceType serviceType : services.keySet()) {
        for (CmServerService service : services.get(serviceType)) {
          hosts.add(service.getHost());
        }
      }
    } else if (services.containsKey(type)) {
      for (CmServerService service : services.get(type)) {
        hosts.add(service.getHost());
      }
    } else if (services.containsKey(type.getParent())) {
      for (CmServerService service : services.get(type.getParent())) {
        if (service.getType().equals(type)) {
          hosts.add(service.getHost());
        }
      }
    }
    return hosts;
  }

}
