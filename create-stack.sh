# Usage: ./create-stack.sh 2 5G m3.large 5G brent-keys2 50 1 20 8 50 500 true 20

UUID='unknown'
unamestr=`uname`
if [[ "$unamestr" == "Darwin" ]]; then
	UUID=$(uuidgen | tr 'A-Z' 'a-z' | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
else
	UUID=$(cat /proc/sys/kernel/random/uuid | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
fi

echo ParameterKey=BrokerCapacity,ParameterValue=$1			# Number of servers to spin up as Spark workers / Kudu tablets
echo ParameterKey=WorkerMem,ParameterValue=$2				# Overall amount of memory each worker should allocate (should be system memory / 2 to save room for kudu)
echo ParameterKey=InstanceType,ParameterValue=$3			# m3.medium, m3.2xlarge, etc https://aws.amazon.com/ec2/pricing/
echo ParameterKey=ExecMem,ParameterValue=$4					# Memory Spark should allocate for each executor (there can be multiple executors active on a worker - should be equal to WorkerMem)
echo ParameterKey=KeyName,ParameterValue=$5					# AWS secret key name
echo ParameterKey=DataVolumeSize,ParameterValue=$6
echo ParameterKey=ScaleFactor,ParameterValue=$7				# TPC-H scale factor 1=1GB, 10=10GB, etc
echo ParameterKey=PartitionCount,ParameterValue=$8			# Default partition count for Spark
echo ParameterKey=BenchmarkUsers,ParameterValue=$9			# How many simultaneous users to simulate
echo ParameterKey=WalVolumeSize,ParameterValue=${10}
echo ParameterKey=IOPS,ParameterValue=${11}					# Supposedly this should be <= disk size * 30, but > 1000 never reliably seems to work
echo ParameterKey=PowerOnly,ParameterValue=${12}			# if "true" then run only the power test, which is fast
echo ParameterKey=KuduPartitionCount,ParameterValue=${13} 	# From what the kudu team says, it sounds like this should be servers * 10

echo StackID=$UUID
aws cloudformation create-stack \
    --capabilities CAPABILITY_IAM \
	--stack-name $UUID \
	--template-body file://cloud-formation.json \
	--parameters \
		ParameterKey=KeyName,ParameterValue=$5 \
		ParameterKey=BrokerCapacity,ParameterValue=$1 \
		ParameterKey=InstanceType,ParameterValue=$3 \
		ParameterKey=WorkerMem,ParameterValue=$2 \
		ParameterKey=ExecMem,ParameterValue=$4 \
		ParameterKey=DataVolumeSize,ParameterValue=$6 \
		ParameterKey=ScaleFactor,ParameterValue=$7 \
		ParameterKey=PartitionCount,ParameterValue=$8 \
		ParameterKey=BenchmarkUsers,ParameterValue=$9 \
		ParameterKey=WalVolumeSize,ParameterValue=${10} \
		ParameterKey=IOPS,ParameterValue=${11} \
		ParameterKey=PowerOnly,ParameterValue=${12} \
		ParameterKey=KuduPartitionCount,ParameterValue=${13} 

UUID=$UUID ./poll.sh
