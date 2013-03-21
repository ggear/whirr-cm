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

import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
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
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceList;
import com.cloudera.api.v3.ParcelResource;
import com.cloudera.api.v3.RootResourceV3;
import com.google.common.collect.Lists;

public class CmServerApi {

  private static Logger log = LoggerFactory.getLogger(CmServerApi.class);

  public static final String CM_API_PARCEL_CDH = "CDH";
  public static final String CM_API_PARCEL_IMPALA = "IMPALA";

  private static final String CM_PARCEL_STAGE_DOWNLOADED = "DOWNLOADED";
  private static final String CM_PARCEL_STAGE_DISTRIBUTED = "DISTRIBUTED";
  private static final String CM_PARCEL_STAGE_ACTIVATED = "ACTIVATED";

  private static int API_POLL_PERIOD_MS = 500;

  final private RootResourceV3 apiResourceRoot;

  public CmServerApi(String ip, int port, String user, String password) {
    super();
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

  public Map<String, String> initialise(Map<String, String> config) throws InterruptedException, IOException {
    Map<String, String> configPostUpdate = new HashMap<String, String>();
    try {

      if (log.isDebugEnabled()) {
        log.debug("Server API [INITIALISE] [PRE]");
      }

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

      if (log.isDebugEnabled()) {
        log.debug("Server API [INITIALISE] [" + configPostUpdate + "]");
        log.debug("Server API [INITIALISE] [POST]");
      }

    } catch (Exception e) {
      throw new IOException("Failed to initialise cluster", e);
    }
    return configPostUpdate;
  }

  public Set<String> hosts() throws InterruptedException, IOException {

    Set<String> hosts = new HashSet<String>();
    try {

      if (log.isDebugEnabled()) {
        log.debug("Server API [HOSTS] [PRE]");
      }

      for (ApiHost host : apiResourceRoot.getHostsResource().readHosts(DataView.SUMMARY).getHosts()) {
        hosts.add(host.getHostId());
      }

      if (log.isDebugEnabled()) {
        log.debug("Server API [HOSTS] [POST]");
      }

    } catch (Exception e) {
      throw new IOException("Failed to list cluster hosts", e);
    }

    return hosts;
  }

  public void provision(CmServerCluster cluster) throws InterruptedException, IOException {

    try {

      if (log.isDebugEnabled()) {
        log.debug("Server API [PROVISION] [PRE]");
      }

      if (!cluster.getServiceTypes().isEmpty()
          && apiResourceRoot.getClustersResource().readClusters(DataView.SUMMARY).size() == 0) {
        provsionCluster(cluster);
        provisionParcels(cluster);
        provisionServices(cluster);
      }

      if (log.isDebugEnabled()) {
        log.debug("Server API [PROVISION] [POST]");
      }

    } catch (Exception e) {
      throw new IOException("Failed to provision cluster", e);
    }

  }

  public void start(CmServerCluster cluster) throws InterruptedException {
  }

  public void stop(CmServerCluster cluster) throws InterruptedException {
  }

  public void unprovision(CmServerCluster cluster) throws InterruptedException, IOException {
    apiResourceRoot.getClustersResource().deleteCluster(cluster.getServiceName(CmServerServiceType.CLUSTER));
  }

  private void provsionCluster(final CmServerCluster cluster) throws IOException, InterruptedException {

    ApiClusterList clusterList = new ApiClusterList();
    ApiCluster apiCluster = new ApiCluster();
    apiCluster.setName(getName(cluster));
    apiCluster.setVersion(ApiClusterVersion.CDH4);
    clusterList.add(apiCluster);
    apiResourceRoot.getClustersResource().createClusters(clusterList);
    List<ApiHostRef> apiHostRefs = Lists.newArrayList();
    for (String host : hosts()) {
      apiHostRefs.add(new ApiHostRef(host));
    }
    apiResourceRoot.getClustersResource().addHosts(getName(cluster), new ApiHostRefList(apiHostRefs));

  }

  private void provisionParcels(final CmServerCluster cluster) throws InterruptedException, IOException {

    execute("Wait For Parcels Availability", null, new Callback() {
      @Override
      public boolean poll() {
        return apiResourceRoot.getClustersResource().getParcelsResource(getName(cluster)).readParcels(DataView.FULL)
            .getParcels().size() >= 2;
      }
    }, false);

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

  private void provisionServices(final CmServerCluster cluster) throws IOException {

    ApiServiceList serviceList = new ApiServiceList();
    for (CmServerServiceType type : cluster.getServiceTypes()) {
      switch (type) {
      case HDFS:
        serviceList.add(buildServiceHdfs(cluster));
        break;
      default:
        throw new IOException("Dont know how to provision type [" + type + "]");
      }
    }
    apiResourceRoot.getClustersResource().getServicesResource(getName(cluster)).createServices(serviceList);

  }

  private ApiService buildServiceHdfs(final CmServerCluster cluster) throws IOException {

    // TODO Refactor

    ApiService hdfsService = new ApiService();
    hdfsService.setType(CmServerServiceType.HDFS.toString());
    hdfsService.setName(cluster.getServiceName(CmServerServiceType.HDFS));

    List<ApiRoleConfigGroup> groupList = Lists.newArrayList();

    ApiRoleConfigGroup nnGrp = new ApiRoleConfigGroup();
    groupList.add(nnGrp);
    ApiConfigList nnConfig = new ApiConfigList();
    nnConfig.add(new ApiConfig("dfs_name_dir_list", "/data/1/dfs/nn"));
    nnGrp.setRoleType("NAMENODE");
    nnGrp.setConfig(nnConfig);
    nnGrp.setName("whirr-nn-group");
    nnGrp.setBase(false);

    ApiRoleConfigGroup snnGrp = new ApiRoleConfigGroup();
    groupList.add(snnGrp);
    ApiConfigList snnConfig = new ApiConfigList();
    snnConfig.add(new ApiConfig("fs_checkpoint_dir_list", "/data/1/dfs/snn"));
    snnGrp.setRoleType("SECONDARYNAMENODE");
    snnGrp.setConfig(snnConfig);
    snnGrp.setName("whirr-snn-group");
    snnGrp.setBase(false);

    ApiRoleConfigGroup dnGrp = new ApiRoleConfigGroup();
    groupList.add(dnGrp);
    ApiConfigList dnConfig = new ApiConfigList();
    dnConfig.add(new ApiConfig("dfs_data_dir_list", "/mnt/data/1/dfs/dn"));
    dnGrp.setRoleType("DATANODE");
    dnGrp.setConfig(dnConfig);
    dnGrp.setName("whirr-dn-group");
    dnGrp.setBase(false);

    hdfsService.setRoleConfigGroups(groupList);

    List<ApiRole> roles = new ArrayList<ApiRole>();

    ApiRole nnRole = new ApiRole();
    nnRole.setName("nn");
    nnRole.setType("NAMENODE");
    nnRole.setHostRef(new ApiHostRef(cluster.getService(CmServerServiceType.NAMENODE).getHost()));
    nnRole.setRoleConfigGroupRef(new ApiRoleConfigGroupRef("whirr-nn-group"));
    roles.add(nnRole);

    ApiRole snnRole = new ApiRole();
    snnRole.setName("snn");
    snnRole.setType("SECONDARYNAMENODE");
    snnRole.setHostRef(new ApiHostRef(cluster.getService(CmServerServiceType.SECONDARYNAMENODE).getHost()));
    snnRole.setRoleConfigGroupRef(new ApiRoleConfigGroupRef("whirr-snn-group"));
    roles.add(snnRole);

    for (CmServerService service : cluster.getServices(CmServerServiceType.DATANODE)) {
      ApiRole dnRole = new ApiRole();
      dnRole.setName(service.getName());
      dnRole.setType(service.getType().toString());
      dnRole.setHostRef(new ApiHostRef(service.getHost()));
      dnRole.setRoleConfigGroupRef(new ApiRoleConfigGroupRef("whirr-dn-group"));
      roles.add(dnRole);
    }

    hdfsService.setRoles(roles);

    return hdfsService;
  }

  private ApiCommand execute(final ApiCommand command) throws InterruptedException {
    return execute(command.getName(), command, new Callback() {
      @Override
      public boolean poll() {
        return apiResourceRoot.getCommandsResource().readCommand(command.getId()).getEndTime() != null;
      }
    }, true);
  }

  private ApiCommand execute(ApiCommand command, Callback callback, boolean checkReturn) throws InterruptedException {
    return execute(command.getName(), command, callback, checkReturn);
  }

  private ApiCommand execute(String label, ApiCommand command, Callback callback, boolean checkReturn)
      throws InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("Server API Command Execute [" + label + "] [PRE]");
    }
    ApiCommand commandReturn = null;
    while (true) {
      if (log.isDebugEnabled()) {
        log.debug("Server API Command Execute [" + label + "] [POLL]");
      }
      if (callback.poll()) {
        if (checkReturn && command != null
            && !(commandReturn = apiResourceRoot.getCommandsResource().readCommand(command.getId())).getSuccess()) {
          throw new RuntimeException("Command [" + command + "] failed [" + commandReturn + "]");
        }
        if (log.isDebugEnabled()) {
          log.debug("Server API Command Execute [" + label + "] [POST]");
        }
        return commandReturn;
      }
      Thread.sleep(API_POLL_PERIOD_MS);
    }
  }

  private static abstract class Callback {

    public abstract boolean poll();

  }

}
