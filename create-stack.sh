# Usage: ./create-stack.sh 2 5G m3.large 5G brent-keys2 50 1

UUID='unknown'
unamestr=`uname`
if [[ "$unamestr" == "Darwin" ]]; then
	UUID=$(uuidgen | tr 'A-Z' 'a-z' | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
else
	UUID=$(cat /proc/sys/kernel/random/uuid | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
fi

echo ParameterKey=BrokerCapacity,ParameterValue=$1
echo ParameterKey=WorkerMem,ParameterValue=$2
echo ParameterKey=InstanceType,ParameterValue=$3
echo ParameterKey=ExecMem,ParameterValue=$4
echo ParameterKey=KeyName,ParameterValue=$5
echo ParameterKey=DataVolumeSize,ParameterValue=$6
echo ParameterKey=ScaleFactor,ParameterValue=$7

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
		ParameterKey=ScaleFactor,ParameterValue=$7

UUID=$UUID ./poll.sh
