# Usage: ./create-stack.sh 2 5G m3.large 5G brent-keys2 50

UUID='unknown'
unamestr=`uname`
if [[ "$unamestr" == "Darwin" ]]; then
	UUID=$(uuidgen | tr 'A-Z' 'a-z' | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
else
	UUID=$(cat /proc/sys/kernel/random/uuid | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
fi

echo ParameterKey=KeyName,ParameterValue=$5
echo ParameterKey=BrokerCapacity,ParameterValue=$1
echo ParameterKey=InstanceType,ParameterValue=$3
echo ParameterKey=WorkerMem,ParameterValue=$2
echo ParameterKey=ExecMem,ParameterValue=$4

echo StackID=$UUID
aws cloudformation create-stack \
	--stack-name $UUID \
	--template-body file://cloud-formation.json \
	--parameters \
		ParameterKey=KeyName,ParameterValue=$5 \
		ParameterKey=BrokerCapacity,ParameterValue=$1 \
		ParameterKey=InstanceType,ParameterValue=$3 \
		ParameterKey=WorkerMem,ParameterValue=$2 \
		ParameterKey=ExecMem,ParameterValue=$4 \
		ParameterKey=DataVolumeSize,ParameterValue=$6

UUID=$UUID ./poll.sh
