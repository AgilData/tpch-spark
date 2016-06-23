docker run \
-it \
--net=host \
-e KUDU_MASTER=kudu-master \
-p 4040:4040 \
docker-kudu
