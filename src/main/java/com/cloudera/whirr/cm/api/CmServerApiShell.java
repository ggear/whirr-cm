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

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.WordUtils;
import org.codehaus.plexus.util.StringUtils;

public class CmServerApiShell {

  public static final String ARGUMENT_PREFIX = "--";

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface CmServerApiShellMethod {
    String name();
  }

  private String host;
  private int port = 7180;
  private String user = "admin";
  private String password = "admin";

  private File client;
  private String command;

  private CmServerCluster cluster;

  private CmServerApiLog logger = new CmServerApiLog.CmServerApiLogSysOut();

  private CmServerApi server;

  private static final Map<String, Method> COMMANDS = new HashMap<String, Method>();
  static {
    for (Method method : CmServerApiImpl.class.getMethods()) {
      if (method.isAnnotationPresent(CmServerApiShellMethod.class)) {
        COMMANDS.put(method.getAnnotation(CmServerApiShellMethod.class).name(), method);
      }
    }
  }

  private static final Map<String, Method> CONFIG_COMMANDS = new HashMap<String, Method>();
  static {
    for (Method method : CmServerApiShell.class.getMethods()) {
      if (method.isAnnotationPresent(CmServerApiShellMethod.class)) {
        CONFIG_COMMANDS.put(method.getAnnotation(CmServerApiShellMethod.class).name(), method);
      }
    }
  }

  private CmServerApiShell() throws CmServerApiException {
  }

  public static Set<String> getCommands() {
    return new HashSet<String>(COMMANDS.keySet());
  }

  public static CmServerApiShell get() throws CmServerApiException {
    return new CmServerApiShell();
  }

  public CmServerApiShell arguments(String[] arguments) throws CmServerApiException {
    return arguments(argumentsPreProcess(arguments));
  }

  public CmServerApiShell arguments(Map<String, String> arguments) throws CmServerApiException {
    for (String argument : arguments.keySet()) {
      if (CONFIG_COMMANDS.containsKey(argument)) {
        try {
          CONFIG_COMMANDS.get(argument).invoke(this, new Object[] { arguments.get(argument) });
        } catch (Exception exception) {
          throw new CmServerApiException("Unexpected runtime exception setting argument", exception);
        }
      }
    }
    return this;
  }

  @CmServerApiShellMethod(name = "host")
  public CmServerApiShell host(String host) throws CmServerApiException {
    if (host == null || host.equals("")) {
      throw new CmServerApiException("Illegal host argument passed [" + host + "]");
    }
    this.host = host;
    this.server = null;
    return this;
  }

  @CmServerApiShellMethod(name = "port")
  public CmServerApiShell port(String port) throws CmServerApiException {
    if (port == null || port.equals("") || !StringUtils.isNumeric(port)) {
      throw new CmServerApiException("Illegal port argument passed [" + port + "]");
    }
    this.port = Integer.parseInt(port);
    this.server = null;
    return this;
  }

  @CmServerApiShellMethod(name = "user")
  public CmServerApiShell user(String user) throws CmServerApiException {
    if (user == null || user.equals("")) {
      throw new CmServerApiException("Illegal user argument passed [" + user + "]");
    }
    this.user = user;
    this.server = null;
    return this;
  }

  @CmServerApiShellMethod(name = "password")
  public CmServerApiShell password(String password) throws CmServerApiException {
    if (password == null || password.equals("")) {
      throw new CmServerApiException("Illegal password argument passed [" + password + "]");
    }
    this.password = password;
    this.server = null;
    return this;
  }

  @CmServerApiShellMethod(name = "client")
  public CmServerApiShell client(String client) throws CmServerApiException {
    if (client == null) {
      throw new CmServerApiException("Illegal client argument passed [" + client + "]");
    }
    this.client = new File(client);
    return this;
  }

  @CmServerApiShellMethod(name = "command")
  public CmServerApiShell command(String command) throws CmServerApiException {
    if (command == null || !COMMANDS.containsKey(command)) {
      throw new CmServerApiException("Illegal command argument passed [" + command + "]");
    }
    this.command = command;
    return this;
  }

  public CmServerApiShell cluster(CmServerCluster cluster) throws CmServerApiException {
    if (cluster == null || cluster.isEmpty()) {
      throw new CmServerApiException("Illegal cluster argument passed [" + cluster + "]");
    }
    this.cluster = cluster;
    return this;
  }

  public CmServerApiShell logger(CmServerApiLog logger) throws CmServerApiException {
    if (logger == null) {
      throw new CmServerApiException("Illegal logger argument passed [" + logger + "]");
    }
    this.logger = logger;
    return this;
  }

  @SuppressWarnings("unchecked")
  public boolean execute() throws CmServerApiException {
    if (host == null) {
      throw new CmServerApiException("Required paramater [host] not set");
    }
    if (command == null) {
      throw new CmServerApiException("Required paramater [command] not set");
    }
    if (server == null) {
      server = CmServerApiFactory.getCmServerApi(host, port, user, password, new CmServerApiLog.CmServerApiLogNull());
    }
    List<Object> paramaters = new ArrayList<Object>();
    for (Class<?> clazz : COMMANDS.get(command).getParameterTypes()) {
      if (clazz.equals(CmServerCluster.class)) {
        if (cluster == null) {
          throw new CmServerApiException("Required paramater [cluster] not set");
        }
        paramaters.add(cluster);
      } else if (clazz.equals(File.class)) {
        if (client == null) {
          throw new CmServerApiException("Required paramater [client] not set");
        }
        paramaters.add(client);
      } else {
        throw new CmServerApiException("Unexpected paramater type [" + clazz.getName() + "]");
      }
    }
    try {

      logger.logOperationStartedSync(WordUtils.capitalize(command));

      Object commandReturn = COMMANDS.get(command).invoke(server, paramaters.toArray());

      boolean commandReturnBoolean = false;
      if (commandReturn instanceof Boolean) {

        commandReturnBoolean = ((Boolean) commandReturn).booleanValue();

      } else if (commandReturn instanceof List<?>) {

        List<?> commandReturnList = ((List<?>) commandReturn);
        commandReturnBoolean = !commandReturnList.isEmpty();
        if (commandReturnBoolean && commandReturnList.get(0) instanceof CmServerService) {
          for (CmServerService service : ((List<CmServerService>) commandReturnList)) {
            logger.logOperationInProgressSync(WordUtils.capitalize(command),
                "  " + (service.getName() + "@" + service.getHost() + "=" + service.getStatus()));
          }
        }

      } else if (commandReturn instanceof CmServerCluster) {

        CmServerCluster commandReturnCluster = ((CmServerCluster) commandReturn);
        commandReturnBoolean = !commandReturnCluster.isEmpty();
        for (CmServerServiceType type : commandReturnCluster.getServiceTypes()) {
          logger.logOperationInProgressSync(WordUtils.capitalize(command), "  " + type.toString());
          for (CmServerService service : commandReturnCluster.getServices(type)) {
            logger.logOperationInProgressSync(WordUtils.capitalize(command),
                "    " + service.getName() + "@" + service.getHost() + "=" + service.getStatus());
          }
        }

      }

      logger.logOperationFinishedSync(WordUtils.capitalize(command));

      return commandReturnBoolean;

    } catch (Exception exception) {
      throw new CmServerApiException("Unexpected runtime exception executing CM Server API", exception);
    }
  }

  public static Map<String, String> argumentsPreProcess(String[] arguments) throws CmServerApiException {
    Map<String, String> argumentsProcessed = new HashMap<String, String>();
    try {
      for (int i = 0; i < arguments.length; i++) {
        if (arguments[i] == null || arguments[i].indexOf(ARGUMENT_PREFIX) != 0) {
          throw new IllegalArgumentException("Argument [" + arguments[i] + "] does not include valid prefix ["
              + ARGUMENT_PREFIX + "]");
        }
        argumentsProcessed.put(arguments[i].replaceFirst(ARGUMENT_PREFIX, ""), arguments[++i]);
      }
    } catch (Exception exception) {
      throw new CmServerApiException("Illegal arguments " + Arrays.asList(arguments)
          + " must be of the form [--name1, value1, --name2, value2, ...]", exception);
    }
    return argumentsProcessed;
  }

}