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

public enum CmServerServiceType {

  // Cluster
  CLUSTER(null, "CLUSTER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED),

  // Gateway
  GATEWAY(null, "GATEWAY", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED),

  // HDFS
  HDFS(CLUSTER, "HDFS", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HDFS_GATEWAY(HDFS, "HDFS_GATEWAY", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HDFS_NAMENODE(HDFS, "NAMENODE",
      CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HDFS_SECONDARY_NAMENODE(
      HDFS, "SECONDARYNAMENODE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HDFS_BALANCER(HDFS, "BALANCER", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HDFS_FAILOVER_CONTROLLER(HDFS,
      "FAILOVERCONTROLLER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HDFS_JOURNALNODE(HDFS, "JOURNALNODE", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HDFS_HTTP_FS(HDFS, "HTTPFS",
      CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HDFS_DATANODE(
      HDFS, "DATANODE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED),

  // MapReduce
  MAPREDUCE(CLUSTER, "MAPREDUCE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), MAPREDUCE_GATEWAY(MAPREDUCE, "MAPREDUCE_GATEWAY",
      CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), MAPREDUCE_JOB_TRACKER(
      MAPREDUCE, "JOBTRACKER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), MAPREDUCE_TASK_TRACKER(MAPREDUCE, "TASKTRACKER", CmServerServiceTypeRepo.CDH,
      3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Zookeeper
  ZOOKEEPER(CLUSTER, "ZOOKEEPER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), ZOOKEEPER_SERVER(ZOOKEEPER, "SERVER", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // HBase
  HBASE(CLUSTER, "HBASE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HBASE_GATEWAY(HBASE, "HBASE_GATEWAY", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HBASE_MASTER(HBASE, "MASTER",
      CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HBASE_THRIFT_SERVER(
      HBASE, "HBASETHRIFTSERVER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HBASE_REST_SERVER(HBASE, "HBASERESTSERVER", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HBASE_REGIONSERVER(HBASE,
      "REGIONSERVER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED),

  // Hive
  HIVE(CLUSTER, "HIVE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HIVE_GATEWAY(HIVE, "HIVE_GATEWAY", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HIVE_METASTORE(HIVE, "HIVEMETASTORE",
      CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HIVE_SERVER2(
      HIVE, "HIVESERVER2", CmServerServiceTypeRepo.CDH, 4, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HIVE_HCATALOG(HIVE, "WEBHCAT", CmServerServiceTypeRepo.CDH, 4,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Solr
  SOLR(CLUSTER, "SOLR", CmServerServiceTypeRepo.SOLR, 4, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), SOLR_SERVER(SOLR, "SOLR_SERVER", CmServerServiceTypeRepo.SOLR, 4,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Solr Indexers
  SOLR_INDEXER(CLUSTER, "KS_INDEXER", CmServerServiceTypeRepo.SOLR, 5, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), SOLR_INDEXER_HBASE(SOLR_INDEXER, "HBASE_INDEXER",
      CmServerServiceTypeRepo.SOLR, 5, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Sqoop
  SQOOP(CLUSTER, "SQOOP", CmServerServiceTypeRepo.CDH, 4, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), SQOOP_SERVER(SQOOP, "SQOOP_SERVER", CmServerServiceTypeRepo.CDH, 4,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Oozie
  OOZIE(CLUSTER, "OOZIE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), OOZIE_SERVER(OOZIE, "OOZIE_SERVER", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Impala
  IMPALA(CLUSTER, "IMPALA", CmServerServiceTypeRepo.IMPALA, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), IMPALA_STATE_STORE(IMPALA, "STATESTORE", CmServerServiceTypeRepo.IMPALA, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), IMPALA_DAEMON(IMPALA, "IMPALAD",
      CmServerServiceTypeRepo.IMPALA, 3, CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), IMPALA_CATALOGSERVER(
      IMPALA, "CATALOGSERVER", CmServerServiceTypeRepo.IMPALA, 6, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED),

  // Flume
  FLUME(CLUSTER, "FLUME", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), FLUME_AGENT(FLUME, "AGENT", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED),

  // Hue
  HUE(CLUSTER, "HUE", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED), HUE_SERVER(HUE, "HUE_SERVER", CmServerServiceTypeRepo.CDH, 3,
      CmServerService.VERSION_UNBOUNDED, 4, CmServerService.VERSION_UNBOUNDED), HUE_BEESWAX_SERVER(HUE,
      "BEESWAX_SERVER", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4, 4),

  // Client
  CLIENT(CLUSTER, "GATEWAY", CmServerServiceTypeRepo.CDH, 3, CmServerService.VERSION_UNBOUNDED, 4,
      CmServerService.VERSION_UNBOUNDED);

  private CmServerServiceType parent;
  private String id;
  private CmServerServiceTypeRepo respository;
  private int versionApiMin;
  private int versionApiMax;
  private int versionCdhMin;
  private int versionCdhMax;

  private CmServerServiceType(CmServerServiceType parent, String id, CmServerServiceTypeRepo respository,
      int versionApiMin, int versionApiMax, int versionCdhMin, int versionCdhMax) {
    this.parent = parent;
    this.id = id;
    this.respository = respository;
    this.versionApiMin = versionApiMin;
    this.versionApiMax = versionApiMax;
    this.versionCdhMin = versionCdhMin;
    this.versionCdhMax = versionCdhMax;
  }

  public CmServerServiceType getParent() {
    return parent;
  }

  public String getId() {
    return id;
  }

  public CmServerServiceTypeRepo getRepository() {
    return respository;
  }

  public int getVersionApiMin() {
    return versionApiMin;
  }

  public int getVersionApiMax() {
    return versionApiMax;
  }

  public int getVersionCdhMin() {
    return versionCdhMin;
  }

  public int getVersionCdhMax() {
    return versionCdhMax;
  }

  public boolean isValid(int versionApi, int versionCdh) {
    return (versionApi < 0 || (versionApiMin < 0 || versionApi >= versionApiMin)
        && (versionApiMax < 0 || versionApi <= versionApiMax))
        && (versionCdh < 0 || (versionCdhMin < 0 || versionCdh >= versionCdhMin)
            && (versionCdhMax < 0 || versionCdh <= versionCdhMax));
  }

  public boolean isConcrete() {
    return getParent() != null && !getParent().equals(CLUSTER);
  }

  public static CmServerServiceType valueOfId(String id) {
    for (CmServerServiceType type : values()) {
      if (type.getId().equals(id)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown " + CmServerServiceType.class.getName() + " id [" + id + "]");
  }

}
