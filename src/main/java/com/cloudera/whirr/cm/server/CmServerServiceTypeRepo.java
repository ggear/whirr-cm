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

import com.google.common.collect.ImmutableMap;

public enum CmServerServiceTypeRepo {

  CDH, IMPALA, SOLR, SPARK;

  private static final ImmutableMap<String, ImmutableMap<String, String>> TYPE_TO_REPO = ImmutableMap.of("CDH4",
      ImmutableMap.of("CDH", "CDH", "IMPALA", "IMPALA", "SOLR", "SOLR", "SPARK", "SPARK"), "CDH5",
      ImmutableMap.of("CDH", "CDH", "IMPALA", "CDH", "SOLR", "CDH", "SPARK", "CDH"));

  public String toString(String cdh) {
    if (!TYPE_TO_REPO.containsKey(cdh) || !TYPE_TO_REPO.get(cdh).containsKey(this.toString())) {
      throw new RuntimeException("Could not determine repo for [" + cdh + "] and [" + this + "]");
    }
    return TYPE_TO_REPO.containsKey(cdh) ? TYPE_TO_REPO.get(cdh).get(this.toString()) : null;
  }

};
