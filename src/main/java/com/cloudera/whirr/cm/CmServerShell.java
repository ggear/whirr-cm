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
package com.cloudera.whirr.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.cloudera.whirr.cm.api.CmServerApiException;
import com.cloudera.whirr.cm.api.CmServerApiShell;
import com.cloudera.whirr.cm.api.CmServerCluster;
import com.cloudera.whirr.cm.api.CmServerService;
import com.cloudera.whirr.cm.api.CmServerServiceType;
import com.cloudera.whirr.cm.cdh.BaseHandlerCmCdh;

public class CmServerShell {

  public static void main(String[] arguments) {

    try {

      Map<String, String> argumentsPreProcessed = CmServerApiShell.argumentsPreProcess(arguments);
      if (!argumentsPreProcessed.containsKey("config")) {
        throw new CmServerApiException("Required paramater [config] not set");
      }
      File config = new File(argumentsPreProcessed.remove("config"));
      if (!argumentsPreProcessed.containsKey("home")) {
        throw new CmServerApiException("Required paramater [home] not set");
      }
      File home = new File(argumentsPreProcessed.remove("home"));
      Properties properties = getClusterProperties(config);

      CmServerApiShell.get().arguments(arguments).cluster(getCluster(properties, config, home))
          .client(getClusterHomeFile(properties, config, home).getAbsolutePath()).execute();

    } catch (Exception exception) {
      System.err.println("Failed to execute CM Server command");
      exception.printStackTrace(System.err);
    }

  }

  public static File getClusterHomeFile(Properties properties, File config, File home) throws CmServerApiException {
    return getClusterHomeFile(properties, config, home, null);
  }

  public static File getClusterHomeFile(Properties properties, File config, File home, String name)
      throws CmServerApiException {

    String clusterName = properties.getProperty(BaseHandler.CONFIG_WHIRR_NAME);
    if (clusterName == null) {
      throw new CmServerApiException("Cloud not find cluster name [" + BaseHandler.CONFIG_WHIRR_NAME + "] in file ["
          + config + "]");
    }

    return (name == null ? new File(home.getAbsoluteFile() + File.separator + clusterName) : new File(
        home.getAbsoluteFile() + File.separator + clusterName + File.separator + name));
  }

  public static Properties getClusterProperties(File config) throws CmServerApiException, FileNotFoundException,
      IOException {

    if (config == null || !config.exists() || !config.canRead()) {
      throw new CmServerApiException("Invalid config file [" + config + "]");
    }

    Properties properties = new Properties();
    InputStream inputStream = null;
    try {
      properties.load(inputStream = new FileInputStream(config));
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }

    return properties;

  }

  public static CmServerCluster getCluster(File config, File home) throws IOException, CmServerApiException {
    return getCluster(getClusterProperties(config), config, home);
  }

  public static CmServerCluster getCluster(Properties properties, File config, File home) throws IOException,
      CmServerApiException {

    CmServerCluster cluster = new CmServerCluster();
    Map<String, CmServerServiceType> roleToType = BaseHandlerCmCdh.getRoleToTypeGlobal();

    String clusterName = properties.getProperty(BaseHandler.CONFIG_WHIRR_NAME);
    if (clusterName == null) {
      throw new CmServerApiException("Cloud not find cluster name [" + BaseHandler.CONFIG_WHIRR_NAME + "] in file ["
          + config + "]");
    }

    File instances = (home == null ? null : getClusterHomeFile(properties, config, home, "instances"));
    if (instances != null && instances.exists() && instances.isFile() && instances.canRead()) {

      BufferedReader instancesReader = null;
      try {
        instancesReader = new BufferedReader(new FileReader(instances));
        String instance = null;
        while ((instance = instancesReader.readLine()) != null) {
          try {
            String[] instanceDetail = instance.split("\t");
            String[] instanceRoles = instanceDetail[1].split(",");
            String instanceIp = instanceDetail[2];
            String instanceIpPrivate = instanceDetail[3];
            for (String instanceRole : instanceRoles) {
              CmServerServiceType type = roleToType.get(instanceRole);
              if (type != null) {
                cluster.add(new CmServerService(type, clusterName, "" + (cluster.getServices(type).size() + 1), null,
                    instanceIp, instanceIpPrivate));
              }
            }
          } catch (Exception e) {
            throw new CmServerApiException("Invalid instance row [" + instance + "] defined in instances file ["
                + instances + "]", e);
          }
        }
      } finally {
        if (instancesReader != null) {
          instancesReader.close();
        }
      }

    } else {

      String instanceTemplates = properties.getProperty(BaseHandler.CONFIG_WHIRR_INSTANCES);
      if (instanceTemplates == null) {
        throw new CmServerApiException("Cloud not find instance templates [" + BaseHandler.CONFIG_WHIRR_INSTANCES
            + "] in file [" + config + "]");
      }
      try {
        for (String instanceTemplate : instanceTemplates.split(",")) {
          String[] instanceTemplateDetails = instanceTemplate.split(" ");
          int instanceNumber = Integer.parseInt(instanceTemplateDetails[0]);
          String[] instanceRoles = instanceTemplateDetails[1].split("\\+");
          for (int i = 0; i < instanceNumber; i++) {
            for (int j = 0; j < instanceRoles.length; j++) {
              CmServerServiceType type = roleToType.get(instanceRoles[j]);
              if (type != null) {
                cluster.add(new CmServerService(type, clusterName, "" + (cluster.getServices(type).size() + 1)));
              }
            }
          }
        }
      } catch (Exception e) {
        throw new CmServerApiException("Invalid instance templates [" + instanceTemplates
            + "] defined in config file [" + config + "]", e);
      }

    }

    return cluster;

  }
}
