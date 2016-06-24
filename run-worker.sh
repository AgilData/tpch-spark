DOCKER_NET=$1
TSERVER_ID=$2
CONTAINER_NAME="kudu-tserver-$TSERVER_ID"

docker run \
--name $CONTAINER_NAME \
-p 7050:7050 \
-p 8050:8050 \
-p 8081:8081 \
-e KUDU_MASTER=kudu-master \
-d \
--entrypoint "/init.sh" \
--net=$DOCKER_NET \
-h $CONTAINER_NAME \
docker-kudu \
tserver
