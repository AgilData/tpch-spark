platform=`uname`

TSERVERS=1

DOCKER_NET="host"
DOCKER_HOST_IP="127.0.0.1"

if [ "$platform" == 'Darwin' ]; then
  if docker network ls | grep kudu > /dev/null ; then
    echo 'Docker network kudu already exists. Not creating it.'
  else
    echo 'Creating docker network kudu...'
    docker network create kudu
  fi
  DOCKER_NET="kudu"
  DOCKER_HOST_IP=`docker-machine ip`
else
  echo "OS not supported $platform"
  exit 1
fi

./run-master.sh $DOCKER_NET
sleep 5

workers=""
for i in `seq 1 $TSERVERS`; do
  ./run-worker.sh $DOCKER_NET $i
  workers+=" kudu-tserver-$i"
done

./run-populate.sh $DOCKER_NET

echo "ADD this entry to your /etc/hosts directory to connect from your local host"
entry="$DOCKER_HOST_IP   kudu-docker kudu-master $workers"
echo $entry
