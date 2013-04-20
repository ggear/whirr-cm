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
package com.cloudera.whirr.cm.server.impl;

import com.cloudera.whirr.cm.server.CmServerService;

public enum CmServerCmsType {

  // Management
  MANAGEMENT(null, "MGMT", null),

  // Host Monitor
  HOSTMONITOR(MANAGEMENT, "HOSTMONITOR", "hmon"),

  // Service Monitor
  SERVICEMONITOR(MANAGEMENT, "SERVICEMONITOR", "smon"),

  // Activity Monitor
  ACTIVITYMONITOR(MANAGEMENT, "ACTIVITYMONITOR", "amon"),

  // Reports Manager
  REPORTSMANAGER(MANAGEMENT, "REPORTSMANAGER", "rmgr"),

  // Event Server
  EVENTSERVER(MANAGEMENT, "EVENTSERVER", null),

  // Alerts Publisher
  ALERTPUBLISHER(MANAGEMENT, "ALERTPUBLISHER", null),

  // Navigator
  NAVIGATOR(MANAGEMENT, "NAVIGATOR", "nav");

  private CmServerCmsType parent;
  private String id;
  private String db;

  private CmServerCmsType(CmServerCmsType parent, String id, String db) {
    this.parent = parent;
    this.id = id;
    this.db = db;
  }

  public CmServerCmsType getParent() {
    return parent;
  }

  public String getId() {
    return id;
  }

  public String getDb() {
    return db == null ? null : "cm_" + db;
  }

  public String getName() {
    return MANAGEMENT.getId().toLowerCase()
        + (getParent() != null ? (CmServerService.NAME_TOKEN_DELIM + getId().toLowerCase()) : "");
  }

  public String getGroup() {
    return MANAGEMENT.getId().toLowerCase()
        + (getParent() != null ? (CmServerService.NAME_TOKEN_DELIM + getId().toLowerCase()
            + CmServerService.NAME_TOKEN_DELIM + CmServerService.NAME_QUALIFIER_GROUP)
            : (CmServerService.NAME_TOKEN_DELIM + CmServerService.NAME_QUALIFIER_GROUP));
  }
}
