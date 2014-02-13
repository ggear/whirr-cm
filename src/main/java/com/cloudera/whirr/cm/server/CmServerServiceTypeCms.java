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
package com.cloudera.whirr.cm.server;

public enum CmServerServiceTypeCms {

  // Management
  CM(null, "CM", false),

  // Management
  MANAGEMENT(null, "MGMT", false),

  // Host Monitor
  HOST_MONITOR(MANAGEMENT, "HOSTMONITOR", false),

  // Service Monitor
  SERVICE_MONITOR(MANAGEMENT, "SERVICEMONITOR", false),

  // Activity Monitor
  ACTIVITY_MONITOR(MANAGEMENT, "ACTIVITYMONITOR", false),

  // Alerts Publisher
  ALERT_PUBLISHER(MANAGEMENT, "ALERTPUBLISHER", false),

  // Event Server
  EVENT_SERVER(MANAGEMENT, "EVENTSERVER", false),

  // Reports Manager
  REPORTS_MANAGER(MANAGEMENT, "REPORTSMANAGER", true),

  // Navigator
  NAVIGATOR(MANAGEMENT, "NAVIGATOR", true),

  // Navigator
  NAVIGATOR_METASERVER(MANAGEMENT, "NAVIGATORMETASERVER", true);

  private CmServerServiceTypeCms parent;
  private String id;
  private boolean enterprise;

  private CmServerServiceTypeCms(CmServerServiceTypeCms parent, String id, boolean enterprise) {
    this.parent = parent;
    this.id = id;
    this.enterprise = enterprise;
  }

  public CmServerServiceTypeCms getParent() {
    return parent;
  }

  public String getId() {
    return id;
  }

  public boolean getEnterprise() {
    return enterprise;
  }

  public String getName() {
    return MANAGEMENT.getId().toLowerCase()
        + (getParent() != null ? CmServerService.NAME_TOKEN_DELIM + getId().toLowerCase() : "");
  }

}
