FROM ubuntu:16.04

# --------------------------- ubuntu ------------------------------------------
RUN apt-get -y update
RUN	apt-get -y install build-essential cmake clang wget vim git \
	autoconf automake libboost-thread-dev libboost-system-dev curl gcc \
	g++ libsasl2-dev libsasl2-modules libtool ntp patch pkg-config \
	make rsync unzip vim-common gdb python asciidoctor xsltproc

# ------------------------------- kudu ----------------------------------------
RUN git clone https://github.com/apache/incubator-kudu kudu
RUN cd kudu && thirdparty/build-if-necessary.sh
RUN	apt-get -y install cmake
RUN mkdir -p /kudu/build/release && cd /kudu/build/release && \
	../../thirdparty/installed/bin/cmake -DCMAKE_BUILD_TYPE=release ../.. && \
	make -j4
ENV KUDU_HOME /kudu/build/release

# ------------------------------ dbgen ---------------------------------------- Not needed
COPY dbgen /dbgen
RUN cd dbgen && make

# ------------------------------ Java -----------------------------------------
RUN wget --no-cookies \
  --no-check-certificate \
	--quiet \
  --header "Cookie: oraclelicense=accept-securebackup-cookie" \
  "http://download.oracle.com/otn-pub/java/jdk/8u91-b14/jdk-8u91-linux-x64.tar.gz" \
  -O jdk-8u91-linux-x64.tar.gz
RUN tar -xf jdk-8u91-linux-x64.tar.gz

# ------------------------------ Maven ----------------------------------------
RUN wget --quiet http://apache.osuosl.org/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
RUN tar -xf apache-maven-3.3.9-bin.tar.gz
ENV PATH /jdk1.8.0_91/bin/:/apache-maven-3.3.9/bin/:/usr/local/sbin:\
  /usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# ------------------------------- Spark ---------------------------------------
# a15ca5533db91fefaf3248255a59c4d94eeda1a9
RUN git clone https://github.com/apache/spark.git
RUN cd spark && \
	git checkout branch-2.0 && \
	git checkout ebbbf2136412c628421b88a2bc83091a2b473c55 && \
	build/mvn -DskipTests clean install

# -------------------------------- Scala --------------------------------------
RUN wget --quiet http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz
RUN tar -xf scala-2.11.8.tgz
RUN wget --quiet https://dl.bintray.com/sbt/native-packages/sbt/0.13.11/sbt-0.13.11.tgz
RUN tar -xf sbt-0.13.11.tgz
ENV PATH /scala-2.11.8/bin/:/sbt/bin/:/jdk1.8.0_91/bin/:\
/apache-maven-3.3.9/bin/:/usr/local/sbin:\
/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# -------------------------------- Kudu-Java ----------------------------------
RUN cd /kudu/java/kudu-spark && \
	sed -i 's/2\.10/2.11/g' pom.xml && \
	sed -i 's/1\.6\.1/2.0.0-SNAPSHOT/g' pom.xml && \
	cd /kudu/java/ && \
	mvn install -DskipTests

RUN cd / && \
	wget http://download.ej-technologies.com/jprofiler/jprofiler_linux_9_2.tar.gz && \
	tar xvzf jprofiler_linux_9_2.tar.gz

# -------------------------------- Spark-Kudu ---------------------------------
# mkdir -p tpc-h
# cd tpc-h
# curl -o pom.xml -u deployer:q0zQRkPLpkTD\!6h5 http://54.226.90.86:8081/artifactory/public-release/spark-tpc-h-queries/spark-tpc-h-queries_2.11/1.1-SNAPSHOT/spark-tpc-h-queries_2.11-1.1-20160608.151057-1.pom
# mkdir -p target/
# curl -o target/spark-tpc-h-queries_2.11-1.1-SNAPSHOT.jar -u deployer:q0zQRkPLpkTD\!6h5 http://54.226.90.86:8081/artifactory/public-release/spark-tpc-h-queries/spark-tpc-h-queries_2.11/1.1-SNAPSHOT/spark-tpc-h-queries_2.11-1.1-20160608.151058-2.jar
# echo -n "target/spark-tpc-h-queries_2.11-1.1-SNAPSHOT.jar:" > cp.txt
# mvn dependency:build-classpath -Dmdep.outputFile=cp2.txt
# cat cp2.txt >> cp.txt
# java -cp $(cat cp.txt) tpch.Main --kuduMaster kudu-master:7051 --sparkMaster spark://kudu-master:7077 --inputDir ./dbgen/ --mode populate
COPY spark-kudu /spark-kudu
COPY .credentials /root/.ivy2/
RUN cd /spark-kudu && sbt compile

# -------------------------------- Run ----------------------------------------
ENV KUDU_HOME /kudu/
RUN apt-get -y install net-tools iputils-ping telnet
COPY init.sh /
#ENTRYPOINT ["/init.sh"]
EXPOSE 7051 8050 8051 7077 8080 6066 7050 4040 11002
