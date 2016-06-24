DOCKER_NET=$1

docker run \
-it \
--net=$DOCKER_NET \
-e KUDU_MASTER=kudu-master \
--entrypoint "./init.sh" \
docker-kudu \
populate
