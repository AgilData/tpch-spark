docker run \
-v "$HOME/.ssh" \
--name kudu-master \
-p 6066:6066 \
-p 7051:7051 \
-p 7077:7077 \
-p 8051:8051 \
-p 8080:8080 \
-d \
--entrypoint "/init.sh" \
--net=host \
docker-kudu \
master
