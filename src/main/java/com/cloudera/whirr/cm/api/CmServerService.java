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

public class CmServerService {

  private String name;
  private CmServerServiceType type;
  private String tag;
  private String qualifier;
  private String host;
  
  public CmServerService(CmServerServiceType type, String tag, String qualifier, String host) {
    this.name = tag + "_" + type.toString().toLowerCase() + "_" + qualifier;
    this.type = type;
    this.tag = tag;
    this.host = host;
    this.qualifier = qualifier;
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    string.append("{");
    string.append("name=");
    string.append(name);
    string.append(", ");
    string.append("type=");
    string.append(type);
    string.append(", ");
    string.append("tag=");
    string.append(tag);
    string.append(", ");
    string.append("qualifier=");
    string.append(qualifier);
    string.append(", ");
    string.append("host=");
    string.append(host);
    string.append("}");
    return string.toString();
  }

  public String getName() {
    return name;
  }

  public CmServerServiceType getType() {
    return type;
  }

  public String getTag() {
    return tag;
  }

  public String getQualifier() {
    return qualifier;
  }

  public String getHost() {
    return host;
  }

}