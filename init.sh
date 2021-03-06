#!/bin/bash
#set -x
#set -v

function do_help {
  echo HELP:
  echo "Supported commands:"
  echo "   master              - Start a Kudu Master"
  echo "   tserver             - Start a Kudu TServer"
  echo "   test                - Run the test"
  echo "   populate            - Populate with tpch data"
  echo "   cli                 - Start a Kudu CLI"
  echo "   help                - print useful information and exit"
  echo ""
  echo "Other commands can be specified to run shell commands."
  echo "Set the environment variable KUDU_OPTS to pass additional"
  echo "arguments to the kudu process. DEFAULT_KUDU_OPTS contains"
  echo "a recommended base set of options."

  exit 0
}

mkdir -p /var/lib/kudu/
DEFAULT_KUDU_OPTS="-logtostderr -use_hybrid_clock=false"

KUDU_OPTS=${KUDU_OPTS:-${DEFAULT_KUDU_OPTS}}

if [ "$1" = 'master' ]; then
  cd /spark
  ./sbin/start-master.sh
  cd /kudu/build/release
	exec bin/kudu-master -fs_wal_dir=/var/lib/kudu/master ${KUDU_OPTS}
elif [ "$1" = 'tserver' ]; then
  cd /spark
  /jdk1.8.0_91/jre/bin/java -cp /spark/conf/:/spark/assembly/target/scala-2.11/jars/* -Xmx1g -agentpath:/jprofiler9/bin/linux-x64/libjprofilerti.so=port=11002,nowait org.apache.spark.deploy.worker.Worker spark://kudu-master:7077 &
  cd /kudu/build/release
  exec bin/kudu-tserver -fs_wal_dir=/var/lib/kudu/tserver -tserver_master_addrs ${KUDU_MASTER} ${KUDU_OPTS}

elif [ "$1" = 'populate' ]; then
  cd /dbgen
  ./dbgen -qf -s 1
  cd /spark-kudu
  sbt "run --kuduMaster ${KUDU_MASTER}:7051 \
     --sparkMaster local[*] \
     --inputDir /dbgen/ \
     --partitionCount 20 \
     --executorMemory 1g \
     -r /root/.m2/repository \
     --mode populate"
elif [ "$1" = 'test' ]; then
  echo "Test not supported!"
  exit 1

elif [ "$1" = 'cli' ]; then
  shift; # Remove first arg and pass remainder to kudu cli
  cd /kudu/build/release
  exec bin/kudu-ts-cli -server_address=${KUDU_TSERVER} "$@"
elif [ "$1" = 'help' ]; then
  do_help
fi
