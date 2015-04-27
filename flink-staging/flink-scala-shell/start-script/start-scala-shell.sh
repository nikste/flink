#!/bin/bash


if [ "$1" == "-h" ]; then
  echo "Usage:"
  echo " -h -help shows this message"
  echo " -h -host <hostname> specifies remote host"
  echo " -p -port <portnum> specifies the port for remote host"
  exit 0
fi


java -cp ../lib/flink-dist-0.9-SNAPSHOT.jar org.apache.flink.api.scala.FlinkShell $1 $2 $3 $4


