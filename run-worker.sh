docker run \
--name kudu-tserver \
-p 7050:7050 \
-p 8050:8050 \
-p 8081:8081 \
-e KUDU_MASTER=127.0.1.1 \
-d \
--entrypoint "/init.sh" \
--net=host \
docker-kudu \
tserver
