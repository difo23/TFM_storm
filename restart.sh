#!/usr/bin/env bash
source ~/.bashrc
#storm kill MachineStormFrankManualDRPC
storm kill MachineStorm
 mvn clean
 mvn clean package
 mvn -f pom.xml package
#storm jar  target/minetur_storm-1.0-SNAPSHOT.jar com.ayscom.minetur.frankestain_tree.topology.FrankestainTreeTopology Frank_topology
#storm jar  target/minetur_storm-1.0-SNAPSHOT-jar-with-dependencies.jar com.ayscom.minetur.frankestain_tree.topology.FrankestainTreeTopology Frank_topology
#storm jar  target/WordCount-1.0-SNAPSHOT.jar com.ayscom.example.frankestain_tree.topology.FrankestainTreeTopology Frank_topology
#storm jar  target/WordCount-1.0-SNAPSHOT-jar-with-dependencies.jar com.ayscom.example.frankestain_tree.topology.FrankestainTreeTopology Frank_topology
#mvn exec:java -Dexec.mainClass="FrankUserDRPCManualTopology" > out.js  #-Dexec.args="src/main/resources/words.txt"
storm jar target/minetur_storm-1.0-SNAPSHOT-jar-with-dependencies.jar com.ayscom.minetur.FrankXDRReload.topologies.FrankReloadTopology MachineStorm