rm dbgen/*.tbl
rm spark-kudu/dbgen/*.tbl
cd dbgen && make clean && cd ../
docker build -t docker-kudu .
