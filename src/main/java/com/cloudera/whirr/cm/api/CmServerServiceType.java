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

public enum CmServerServiceType {

  // Cluster
  CLUSTER(null, "CLUSTER"),

  // HDFS
  HDFS(CLUSTER, "HDFS"), HDFS_NAMENODE(HDFS, "NAMENODE"), HDFS_SECONDARY_NAMENODE(HDFS, "SECONDARYNAMENODE"), HDFS_DATANODE(
      HDFS, "DATANODE"),

  // MapReduce
  MAPREDUCE(CLUSTER, "MAPREDUCE"), MAPREDUCE_JOB_TRACKER(MAPREDUCE, "JOBTRACKER"), MAPREDUCE_TASK_TRACKER(MAPREDUCE,
      "TASKTRACKER"),

  // Zookeeper
  ZOOKEEPER(CLUSTER, "ZOOKEEPER"), ZOOKEEPER_SERVER(ZOOKEEPER, "SERVER"),

  // HBase
  HBASE(CLUSTER, "HBASE"), HBASE_MASTER(HBASE, "MASTER"), HBASE_REGIONSERVER(HBASE, "REGIONSERVER"),

  // Hive
  HIVE(CLUSTER, "HIVE"), HIVE_METASTORE(HIVE, "HIVEMETASTORE"),

  // Impala
  IMPALA(CLUSTER, "IMPALA"), IMPALA_STATE_STORE(IMPALA, "IMPALASTATESTORE"), IMPALA_DAEMON(IMPALA, "IMPALADAEMON"),

  // Client
  CLIENT(CLUSTER, "GATEWAY");

  private CmServerServiceType parent;
  private String label;

  private CmServerServiceType(CmServerServiceType parent, String label) {
    this.parent = parent;
    this.label = label;
  }

  public CmServerServiceType getParent() {
    return parent;
  }

  @Override
  public String toString() {
    return label;
  }

}
