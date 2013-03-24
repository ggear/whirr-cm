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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiBulkCommandList;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiClusterVersion;
import com.cloudera.api.model.ApiCommand;
import com.cloudera.api.model.ApiConfig;
import com.cloudera.api.model.ApiConfigList;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiHostRef;
import com.cloudera.api.model.ApiHostRefList;
import com.cloudera.api.model.ApiParcel;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiRoleConfigGroup;
import com.cloudera.api.model.ApiRoleConfigGroupRef;
import com.cloudera.api.model.ApiRoleNameList;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceConfig;
import com.cloudera.api.model.ApiServiceList;
import com.cloudera.api.v3.ParcelResource;
import com.cloudera.api.v3.RootResourceV3;
import com.cloudera.whirr.cm.api.CmServerApiLog.CmServerApiLogSyncCommand;
import com.google.common.collect.Lists;

public class CmServerApi {

  public static final String CM_API_PARCEL_CDH = "CDH";
  public static final String CM_API_PARCEL_IMPALA = "IMPALA";

  private static final String CM_PARCEL_STAGE_DOWNLOADED = "DOWNLOADED";
  private static final String CM_PARCEL_STAGE_DISTRIBUTED = "DISTRIBUTED";
  private static final String CM_PARCEL_STAGE_ACTIVATED = "ACTIVATED";

  private static int API_POLL_PERIOD_MS = 500;

  private CmServerApiLog logger;
  final private RootResourceV3 apiResourceRoot;

  public CmServerApi(String ip, int port, String user, String password) {
    this(ip, port, user, password, new CmServerApiLog.CmServerApiLogSlf4j());
  }

  public CmServerApi(String ip, int port, String user, String password, CmServerApiLog logger) {
    this.logger = logger;
    this.apiResourceRoot = new ClouderaManagerClientBuilder().withHost(ip).withPort(port)
        .withUsernamePassword(user, password).build().getRootV3();
  }

  public String getName(CmServerCluster cluster) {
    try {
      return cluster.getServiceName(CmServerServiceType.CLUSTER);
    } catch (IOException e) {
      throw new RuntimeException("Could not resolve cluster name", e);
    }
  }

  public Map<String, String> initialise(Map<String, String> config) throws CmServerApiException {
    Map<String, String> configPostUpdate = null;
    try {

      logger.logOperationStartedSync("ClusterInitialise");

      configPostUpdate = provisionCmSettings(config);

      logger.logOperationFinishedSync("ClusterInitialise");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to initialise cluster", e);
    }
    return configPostUpdate;
  }

  public Set<String> hosts() throws CmServerApiException {

    final Set<String> hosts = new HashSet<String>();
    try {

      logger.logOperation("GetHosts", new CmServerApiLogSyncCommand() {
        @Override
        public void execute() {
          for (ApiHost host : apiResourceRoot.getHostsResource().readHosts(DataView.SUMMARY).getHosts()) {
            hosts.add(host.getHostId());
          }
        }
      });

    } catch (Exception e) {
      throw new CmServerApiException("Failed to list cluster hosts", e);
    }

    return hosts;
  }

  public void provision(CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterProvision");

      if (!cluster.getServiceTypes().isEmpty()
          && apiResourceRoot.getClustersResource().readClusters(DataView.SUMMARY).size() == 0) {
        provsionCluster(cluster);
        provisionParcels(cluster);
      }

      logger.logOperationFinishedSync("ClusterProvision");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to provision cluster", e);
    }

  }

  public void configure(CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterConfigure");

      configureServices(cluster);

      logger.logOperationFinishedSync("ClusterConfigure");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to configure cluster", e);
    }

  }

  public void startFirst(CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterFirstStart");

      for (CmServerServiceType type : cluster.getServiceTypes()) {
        initPreStartServices(cluster, type);
        startService(cluster, type);
        initPostStartServices(cluster, type);
      }

      logger.logOperationFinishedSync("ClusterFirstStart");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to first start cluster", e);
    }

  }

  public void start(final CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterStart");

      logger.logOperation("StartCluster", new CmServerApiLogSyncCommand() {
        @Override
        public void execute() throws IOException {
          apiResourceRoot.getClustersResource().startCommand(getName(cluster));
        }
      });

      logger.logOperationFinishedSync("ClusterStart");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to start cluster", e);
    }

  }

  public void stop(final CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterStop");

      logger.logOperation("StopCluster", new CmServerApiLogSyncCommand() {
        @Override
        public void execute() throws IOException {
          apiResourceRoot.getClustersResource().startCommand(getName(cluster));
        }
      });

      logger.logOperationFinishedSync("ClusterStop");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to stop cluster", e);
    }

  }

  public void unconfigure(final CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterUnConfigure");

      unconfigureServices(cluster);

      logger.logOperationFinishedSync("ClusterUnConfigure");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to unconfigure cluster", e);
    }

  }

  public void unprovision(final CmServerCluster cluster) throws CmServerApiException {

    try {

      logger.logOperationStartedSync("ClusterUnProvision");

      logger.logOperation("CreateCluster", new CmServerApiLogSyncCommand() {
        @Override
        public void execute() throws IOException {
          apiResourceRoot.getClustersResource().deleteCluster(getName(cluster));
        }
      });

      logger.logOperationFinishedSync("ClusterUnProvision");

    } catch (Exception e) {
      throw new CmServerApiException("Failed to unprovision cluster", e);
    }

  }

  private Map<String, String> provisionCmSettings(Map<String, String> config) throws InterruptedException {

    Map<String, String> configPostUpdate = new HashMap<String, String>();
    ApiConfigList apiConfigList = new ApiConfigList();
    if (!config.isEmpty()) {
      for (String key : config.keySet()) {
        apiConfigList.add(new ApiConfig(key, config.get(key)));
      }
      apiResourceRoot.getClouderaManagerResource().updateConfig(apiConfigList);
    }
    apiConfigList = apiResourceRoot.getClouderaManagerResource().getConfig(DataView.SUMMARY);
    for (ApiConfig apiConfig : apiConfigList) {
      configPostUpdate.put(apiConfig.getName(), apiConfig.getValue());
    }
    execute(apiResourceRoot.getClouderaManagerResource().inspectHostsCommand());

    return configPostUpdate;

  }

  private void provsionCluster(final CmServerCluster cluster) throws IOException, InterruptedException,
      CmServerApiException {

    final ApiClusterList clusterList = new ApiClusterList();
    ApiCluster apiCluster = new ApiCluster();
    apiCluster.setName(getName(cluster));
    apiCluster.setVersion(ApiClusterVersion.CDH4);
    clusterList.add(apiCluster);

    logger.logOperation("CreateCluster", new CmServerApiLogSyncCommand() {
      @Override
      public void execute() throws IOException {
        apiResourceRoot.getClustersResource().createClusters(clusterList);
      }
    });

    List<ApiHostRef> apiHostRefs = Lists.newArrayList();
    for (String host : hosts()) {
      apiHostRefs.add(new ApiHostRef(host));
    }
    apiResourceRoot.getClustersResource().addHosts(getName(cluster), new ApiHostRefList(apiHostRefs));

  }

  private void provisionParcels(final CmServerCluster cluster) throws InterruptedException, IOException {

    apiResourceRoot.getClouderaManagerResource().updateConfig(
        new ApiConfigList(Arrays.asList(new ApiConfig[] { new ApiConfig("PARCEL_UPDATE_FREQ", "1") })));

    execute("Wait For Parcels Availability", new Callback() {
      @Override
      public boolean poll() {
        return apiResourceRoot.getClustersResource().getParcelsResource(getName(cluster)).readParcels(DataView.FULL)
            .getParcels().size() >= 2;
      }
    });

    apiResourceRoot.getClouderaManagerResource().updateConfig(
        new ApiConfigList(Arrays.asList(new ApiConfig[] { new ApiConfig("PARCEL_UPDATE_FREQ", "60") })));

    DefaultArtifactVersion parcelVersionCdh = null;
    DefaultArtifactVersion parcelVersionImpala = null;
    for (ApiParcel apiParcel : apiResourceRoot.getClustersResource().getParcelsResource(getName(cluster))
        .readParcels(DataView.FULL).getParcels()) {
      if (apiParcel.getProduct().equals(CM_API_PARCEL_CDH)
          && (parcelVersionCdh == null || parcelVersionCdh
              .compareTo(new DefaultArtifactVersion(apiParcel.getVersion())) < 0)) {
        parcelVersionCdh = new DefaultArtifactVersion(apiParcel.getVersion());
      } else if (apiParcel.getProduct().equals(CM_API_PARCEL_IMPALA)
          && (parcelVersionImpala == null || parcelVersionImpala.compareTo(new DefaultArtifactVersion(apiParcel
              .getVersion())) < 0)) {
        parcelVersionImpala = new DefaultArtifactVersion(apiParcel.getVersion());
      }
    }
    final ParcelResource apiParcelCdh = apiResourceRoot.getClustersResource().getParcelsResource(getName(cluster))
        .getParcelResource(CM_API_PARCEL_CDH, parcelVersionCdh.toString());
    execute(apiParcelCdh.startDownloadCommand(), new Callback() {
      @Override
      public boolean poll() {
        return apiParcelCdh.readParcel().getStage().equals(CM_PARCEL_STAGE_DOWNLOADED);
      }
    }, false);

    execute(apiParcelCdh.startDistributionCommand(), new Callback() {
      @Override
      public boolean poll() {
        return apiParcelCdh.readParcel().getStage().equals(CM_PARCEL_STAGE_DISTRIBUTED);
      }
    }, false);
    execute(apiParcelCdh.activateCommand(), new Callback() {
      @Override
      public boolean poll() {
        return apiParcelCdh.readParcel().getStage().equals(CM_PARCEL_STAGE_ACTIVATED);
      }
    }, false);

    final ParcelResource apiParcelImpala = apiResourceRoot.getClustersResource().getParcelsResource(getName(cluster))
        .getParcelResource(CM_API_PARCEL_IMPALA, parcelVersionImpala.toString());
    execute(apiParcelImpala.startDownloadCommand(), new Callback() {
      @Override
      public boolean poll() {
        return apiParcelImpala.readParcel().getStage().equals(CM_PARCEL_STAGE_DOWNLOADED);
      }
    }, false);
    execute(apiParcelImpala.startDistributionCommand(), new Callback() {
      @Override
      public boolean poll() {
        return apiParcelImpala.readParcel().getStage().equals(CM_PARCEL_STAGE_DISTRIBUTED);
      }
    }, false);
    execute(apiParcelImpala.activateCommand(), new Callback() {
      @Override
      public boolean poll() {
        return apiParcelImpala.readParcel().getStage().equals(CM_PARCEL_STAGE_ACTIVATED);
      }
    }, false);

  }

  private void configureServices(final CmServerCluster cluster) throws IOException, InterruptedException {

    final ApiServiceList serviceList = new ApiServiceList();
    for (CmServerServiceType type : cluster.getServiceTypes()) {
      serviceList.add(buildServices(cluster, type));
    }

    logger.logOperation("CreateClusterServices", new CmServerApiLogSyncCommand() {
      @Override
      public void execute() throws IOException, InterruptedException {
        apiResourceRoot.getClustersResource().getServicesResource(getName(cluster)).createServices(serviceList);
      }
    });

    // Necessary, since createServices a habit of kicking off async commands (eg ZkAutoInit )
    for (CmServerServiceType type : cluster.getServiceTypes()) {
      for (ApiCommand command : apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
          .listActiveCommands(cluster.getServiceName(type), DataView.SUMMARY)) {
        CmServerApi.this.execute(command, false);
      }
    }

  }

  private void unconfigureServices(final CmServerCluster cluster) throws IOException, InterruptedException {

    Set<CmServerServiceType> types = new TreeSet<CmServerServiceType>(Collections.reverseOrder());
    types.addAll(cluster.getServiceTypes());
    for (final CmServerServiceType type : types) {
      logger.logOperation("DestroyClusterServices", new CmServerApiLogSyncCommand() {
        @Override
        public void execute() throws IOException {
          apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
              .deleteService(cluster.getServiceName(type));
        }
      });
    }

  }

  private ApiService buildServices(final CmServerCluster cluster, CmServerServiceType type) throws IOException {

    ApiService apiService = new ApiService();
    List<ApiRole> apiRoles = new ArrayList<ApiRole>();
    List<ApiRoleConfigGroup> apiRoleConfigGroups = Lists.newArrayList();

    apiService.setType(type.toString());
    apiService.setName(cluster.getServiceName(type));

    ApiServiceConfig apiServiceConfig = new ApiServiceConfig();
    // TODO Refactor, dont hardcode, pass through from whirr config
    switch (type) {
    case MAPREDUCE:
      apiServiceConfig.add(new ApiConfig("hdfs_service", cluster.getServiceName(CmServerServiceType.HDFS)));
      break;
    case HIVE:
      // TODO Hive is not supported yet
      apiServiceConfig.add(new ApiConfig("mapreduce_yarn_service", cluster
          .getServiceName(CmServerServiceType.MAPREDUCE)));
      apiServiceConfig.add(new ApiConfig("hive_metastore_database_type", "postgresql"));
      apiServiceConfig.add(new ApiConfig("hive_metastore_database_host", "31-222-189-24.static.cloud-ips.co.uk"));
      apiServiceConfig.add(new ApiConfig("hive_metastore_database_password", "hive"));
      apiServiceConfig.add(new ApiConfig("hive_metastore_database_port", "5432"));
      break;
    case HBASE:
      apiServiceConfig.add(new ApiConfig("hdfs_service", cluster.getServiceName(CmServerServiceType.HDFS)));
      apiServiceConfig.add(new ApiConfig("zookeeper_service", cluster.getServiceName(CmServerServiceType.ZOOKEEPER)));
    default:
      break;
    }
    apiService.setConfig(apiServiceConfig);

    for (CmServerServiceType subType : cluster.getServiceTypes(type)) {
      ApiConfigList apiConfigList = new ApiConfigList();
      // TODO Refactor, dont hardcode, pass through from whirr config
      switch (subType) {
      case HDFS_NAMENODE:
        apiConfigList.add(new ApiConfig("dfs_name_dir_list", "/data/1/dfs/nn"));
        break;
      case HDFS_SECONDARY_NAMENODE:
        apiConfigList.add(new ApiConfig("fs_checkpoint_dir_list", "/data/1/dfs/snn"));
        break;
      case HDFS_DATANODE:
        apiConfigList.add(new ApiConfig("dfs_data_dir_list", "/mnt/data/1/dfs/dn"));
        break;
      case MAPREDUCE_JOB_TRACKER:
        apiConfigList.add(new ApiConfig("jobtracker_mapred_local_dir_list", "/data/1/mapred/jt"));
        break;
      case MAPREDUCE_TASK_TRACKER:
        apiConfigList.add(new ApiConfig("tasktracker_mapred_local_dir_list", "/data/1/mapred/local"));
        break;
      default:
        break;
      }
      ApiRoleConfigGroup apiRoleConfigGroup = new ApiRoleConfigGroup();
      apiRoleConfigGroup.setRoleType(subType.toString());
      apiRoleConfigGroup.setConfig(apiConfigList);
      apiRoleConfigGroup.setName(cluster.getService(subType).getGroup());
      apiRoleConfigGroup.setBase(false);
      apiRoleConfigGroups.add(apiRoleConfigGroup);
    }

    for (CmServerService subService : cluster.getServices(type)) {
      ApiRole apiRole = new ApiRole();
      apiRole.setName(subService.getName());
      apiRole.setType(subService.getType().toString());
      apiRole.setHostRef(new ApiHostRef(subService.getHost()));
      apiRole.setRoleConfigGroupRef(new ApiRoleConfigGroupRef(subService.getGroup()));
      apiRoles.add(apiRole);
    }

    apiService.setRoleConfigGroups(apiRoleConfigGroups);
    apiService.setRoles(apiRoles);

    return apiService;
  }

  private void initPreStartServices(final CmServerCluster cluster, CmServerServiceType type) throws IOException,
      InterruptedException {

    switch (type) {
    case HDFS:
      CmServerService service = cluster.getService(CmServerServiceType.HDFS_NAMENODE);
      if (service != null) {
        ApiRoleNameList formatList = new ApiRoleNameList();
        formatList.add(service.getName());
        execute(
            apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
                .getRoleCommandsResource(cluster.getServiceName(CmServerServiceType.HDFS)).formatCommand(formatList),
            false);
      }
      break;
    case HIVE:
      execute(apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
          .hiveCreateMetastoreDatabaseTablesCommand(cluster.getServiceName(CmServerServiceType.HIVE)));
      break;
    case HBASE:
      execute(apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
          .createHBaseRootCommand(cluster.getServiceName(CmServerServiceType.HBASE)));
      break;
    default:
      break;
    }

  }

  private void initPostStartServices(final CmServerCluster cluster, CmServerServiceType type) throws IOException,
      InterruptedException {

    switch (type) {
    case HDFS:
      CmServerService service = cluster.getService(CmServerServiceType.HDFS_NAMENODE);
      if (service != null) {
        ApiRoleNameList formatList = new ApiRoleNameList();
        formatList.add(service.getName());
        execute(apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
            .hdfsCreateTmpDir(cluster.getServiceName(CmServerServiceType.HDFS)));
      }
      break;
    default:
      break;
    }

  }

  private void startService(CmServerCluster cluster, CmServerServiceType type) throws InterruptedException, IOException {
    execute(apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
        .startCommand(cluster.getServiceName(type)));
  }

  @SuppressWarnings("unused")
  private void stopService(CmServerCluster cluster, CmServerServiceType type) throws InterruptedException, IOException {
    execute(apiResourceRoot.getClustersResource().getServicesResource(getName(cluster))
        .stopCommand(cluster.getServiceName(type)));
  }

  @SuppressWarnings("unused")
  private ApiCommand execute(final ApiBulkCommandList bulkCommand) throws InterruptedException {
    return execute(bulkCommand, true);
  }

  private ApiCommand execute(final ApiBulkCommandList bulkCommand, boolean checkReturn) throws InterruptedException {
    ApiCommand lastCommand = null;
    for (ApiCommand command : bulkCommand) {
      lastCommand = execute(command, checkReturn);
    }
    return lastCommand;
  }

  private ApiCommand execute(final ApiCommand command) throws InterruptedException {
    return execute(command, true);
  }

  private ApiCommand execute(final ApiCommand command, boolean checkReturn) throws InterruptedException {
    return execute(command.getName(), command, new Callback() {
      @Override
      public boolean poll() {
        return apiResourceRoot.getCommandsResource().readCommand(command.getId()).getEndTime() != null;
      }
    }, checkReturn);
  }

  private ApiCommand execute(ApiCommand command, Callback callback, boolean checkReturn) throws InterruptedException {
    return execute(command.getName(), command, callback, checkReturn);
  }

  private ApiCommand execute(String label, Callback callback) throws InterruptedException {
    return execute(label, null, callback, false);
  }

  private ApiCommand execute(String label, ApiCommand command, Callback callback, boolean checkReturn)
      throws InterruptedException {

    logger.logOperationStartedAsync(label);

    ApiCommand commandReturn = null;
    while (true) {

      logger.logOperationInProgressAsync(label);

      if (callback.poll()) {
        if (checkReturn && command != null
            && !(commandReturn = apiResourceRoot.getCommandsResource().readCommand(command.getId())).getSuccess()) {
          throw new RuntimeException("Command [" + command + "] failed [" + commandReturn + "]");
        }

        logger.logOperationFinishedAsync(label);

        return commandReturn;
      }
      Thread.sleep(API_POLL_PERIOD_MS);
    }
  }

  private static abstract class Callback {
    public abstract boolean poll();
  }

}
