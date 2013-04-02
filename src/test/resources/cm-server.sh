#!/bin/sh

java -cp "../../../target/runtime-lib/*:../../../target/whirr-cm-1.1-SNAPSHOT-jar-with-dependencies.jar" com.cloudera.whirr.cm.CmServerShell \
	--config ./cm-server.properties \
	--home ../../../target/test-classes \
	$*
