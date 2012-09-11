#!/usr/bin/env python
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

import glob
import simplejson as json
import os
import psutil
import pyjavaproperties
import subprocess
import sys
import zipfile

# This generates a zip file (configs.zip) with the following layout:
# 
# daemon_info.json
# hadoop/*-site.xml
# hbase/*-site.xml
# zookeeper/zoo.cfg
#          /myid
#
# If configurations for any of these services are missing, those directories 
# will not exist.

# Configuration files we grab from each service's config dir.
HADOOP_FILES = ["core-site.xml", "hdfs-site.xml", "mapred-site.xml"]
HBASE_FILES = ["hbase-site.xml"] 
ZOOKEEPER_FILES = ["zoo.cfg"] # grab the myid file by looking through zoo.cfg

# This maps fully-qualified Java main classes to the (service, role) they corresponse
# to. In order to detect roles, we look through JPS output for these classnames.
DAEMON_CLASS_NAMES = {
    # For unclear reasons, the secure datanode starter results in JPS seeing no name
    # for the Main class. As a side effect, our JPS output parsing code interprets
    # the first jvm arg as the Main class name. In the case of the datanode starter,
    # this is "-DProc_datanode" which we can then match on. As this arg is irrelevant
    # to our jvm arg parsing, everything works out in the end. Hah.
    "-Dproc_datanode": ("HDFS", "DATANODE"),
    "org.apache.hadoop.hdfs.server.datanode.DataNode": ("HDFS", "DATANODE"),
    "org.apache.hadoop.hdfs.server.namenode.NameNode": ("HDFS", "NAMENODE"),
    "org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode": ("HDFS", "SECONDARYNAMENODE"),
    "org.apache.hadoop.mapred.JobTracker": ("MAPREDUCE", "JOBTRACKER"),
    "org.apache.hadoop.mapred.TaskTracker": ("MAPREDUCE", "TASKTRACKER"),
    "org.apache.hadoop.hbase.master.HMaster": ("HBASE", "MASTER"),
    "org.apache.hadoop.hbase.regionserver.HRegionServer": ("HBASE", "REGIONSERVER"),
    "org.apache.zookeeper.server.quorum.QuorumPeerMain": ("ZOOKEEPER", "SERVER")
}

# Set JPS_COMMAND if $JAVA_HOME and $JAVA_HOME/bin/jps exist.
# 
# -l: output the fully-qualified main classname
# -v: output command line options passed the JVM
# 
# Example output:
# 986 org.apache.hadoop.mapred.JobTracker -Dproc_jobtracker -Xmx1000m -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote 
# -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote -Dhadoop.log.dir=/usr/lib/hadoop-0.20/logs 
# -Dhadoop.log.file=hadoop-hadoop-jobtracker-scabbers.log -Dhadoop.home.dir=/usr/lib/hadoop-0.20 -Dhadoop.id.str=hadoop -Dhadoop.root.logger=INFO,DRFA 
# -Djava.library.path=/usr/lib/hadoop-0.20/lib/native/Linux-amd64-64 -Dhadoop.policy.file=hadoop-policy.xml
# 1154 org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode -Dproc_secondarynamenode -Xmx1000m -Dcom.sun.management.jmxremote 
# -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote 
# -Dhadoop.log.dir=/usr/lib/hadoop-0.20/logs -Dhadoop.log.file=hadoop-hadoop-secondarynamenode-scabbers.log -Dhadoop.home.dir=/usr/lib/hadoop-0.20 
# -Dhadoop.id.str=hadoop -Dhadoop.root.logger=INFO,DRFA -Djava.library.path=/usr/lib/hadoop-0.20/lib/native/Linux-amd64-64 
# -Dhadoop.policy.file=hadoop-policy.xml
#
# NOTE: JPS ships with JDK 1.6.
JPS_COMMAND = None
JAVA_HOME = os.environ.get("JAVA_HOME")
if JAVA_HOME:
    jps_path = os.path.join(JAVA_HOME, "bin", "jps")
    if os.path.exists(jps_path):
        JPS_COMMAND = jps_path + " -l -v"

def _get_jps_cmd_results():
    proc = subprocess.Popen(JPS_COMMAND, shell=True, stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE, env=os.environ.copy())
    stdout, stderr = proc.communicate()
    ret_code = proc.wait()

    if ret_code is not 0:
        raise Exception("%s failed with stderr: %s" % (str(JPS_COMMAND), stderr))

    results = []
    for line in stdout.splitlines():
        try:
            pid, classname, jvm_args = line.strip().split(None, 2)
        except:
            # Skip malformed lines.
            continue

        jvm_args = jvm_args.split()
        results.append((pid, classname, jvm_args))
    return results

def _get_daemon_info_with_jps():
    """Returns daemon info along with JVM arguments using 'jps'."""
    daemon_info = {}

    jps_cmd_results = _get_jps_cmd_results()
    for pid, classname, jvm_args in jps_cmd_results:
        if classname not in DAEMON_CLASS_NAMES:
            continue

        service_type, role_type = DAEMON_CLASS_NAMES[classname]
        if service_type not in daemon_info:
            daemon_info[service_type] = {}
        daemon_info[service_type][role_type] = jvm_args

    return daemon_info

def _get_daemon_info_with_psutil():
    """Returns daemon info without JVM arguments. This is a fallback in case 
    'jps' is not available, which should not usually be the case."""
    daemon_info = {}
    
    for process in psutil.process_iter():
        for classname, (service_type, role_type) in DAEMON_CLASS_NAMES.items():
            if classname in process.cmdline:
                if service_type not in daemon_info:
                    daemon_info[service_type] = {}
                daemon_info[service_type][role_type] = [] # No JVM arg parsing.
                break
            
    return daemon_info

def get_daemon_info():
    """Returns a map of service_type --> {map of role_type --> [list of command line options]} 
    for each role that's running on this host."""

    if JPS_COMMAND:
        return _get_daemon_info_with_jps()
    else:
        return _get_daemon_info_with_psutil()

def _write_files(zip_fd, conf_dir, filename_list, archive_dir):
    """Looks for all filenames in filename_list in conf_dir. If the file exists,
    this will create an entry in zip_fd for "archive_dir/filename" and write the
    file to it."""
    for filename in filename_list:
        config_file_path = os.path.join(conf_dir, filename)
        if os.path.exists(config_file_path):
            zip_fd.write(config_file_path, archive_dir + "/" + filename)
    
def _write_zk_myid(zip_fd, zookeeper_dir):
    """If a zoo.cfg file exists in zookeeper_dir, this will pull the dataDir
    property from it, look for a myid file in dataDir, and write it to the zip
    file at zookeeper/myid."""
    zoo_cfg_path = os.path.join(zookeeper_dir, "zoo.cfg")
    if not os.path.exists(zoo_cfg_path):
        return

    props = pyjavaproperties.Properties()
    props.load(open(zoo_cfg_path))

    data_dir = props.getProperty("dataDir")
    if not data_dir:
        return

    myid_path = os.path.join(data_dir, "myid")
    if os.path.exists(myid_path):
        zip_fd.write(myid_path, "zookeeper" + "/" + "myid")
        
def main():
    if len(sys.argv) < 4:
        raise Exception("This script requires three configuration directory arguments: [Hadoop, HBase, ZooKeeper]")
    
    hadoop_dir, hbase_dir, zookeeper_dir = sys.argv[1:]

    if not os.environ.get("CONF_DIR"):
        raise Exception("CONF_DIR is not set.")
    
    zip_path = os.path.join(os.environ.get("CONF_DIR"), "configs.zip")
    zip_fd = zipfile.ZipFile(zip_path, "w")

    # Write configuration files.
    _write_files(zip_fd, hadoop_dir, HADOOP_FILES, "hadoop")
    _write_files(zip_fd, hbase_dir, HBASE_FILES, "hbase")
    _write_files(zip_fd, zookeeper_dir, ZOOKEEPER_FILES, "zookeeper")

    # Find, read, and write the Zookeeper myid file, if one exists.
    _write_zk_myid(zip_fd, zookeeper_dir)

    # Write any running role data.
    daemon_info = get_daemon_info()
    zip_fd.writestr("daemon_info.json", json.dumps(daemon_info))

    zip_fd.close()

if __name__ == "__main__":
    main()
