# Usage: ./create-stack.sh a b c 2 d 5G m3.large 5G brent-keys2

UUID='unknown'
unamestr=`uname`
if [[ "$unamestr" == "Darwin" ]]; then
	UUID=$(uuidgen | tr 'A-Z' 'a-z')
else
	UUID=$(cat /proc/sys/kernel/random/uuid | egrep -o "[a-zA-Z][-a-zA-Z0-9]*")
fi

echo StackID=$UUID
aws cloudformation create-stack \
	--stack-name $UUID \
	--template-body file://cloud-formation.json \
	--parameters \
		ParameterKey=KeyName,ParameterValue=$9 \
		ParameterKey=ArtifactoryPassword,ParameterValue=$5 \
		ParameterKey=BrokerCapacity,ParameterValue=$4 \
		ParameterKey=AgilDataVersion,ParameterValue=$3 \
		ParameterKey=InstanceType,ParameterValue=$7 \
		ParameterKey=ArtifactId,ParameterValue=$1 \
		ParameterKey=ArtifactVersion,ParameterValue=$2 \
		ParameterKey=WorkerMem,ParameterValue=$6 \
		ParameterKey=ExecMem,ParameterValue=$8 \

date1=$(date +"%s")

URL=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[0].OutputValue')
while [ "$URL" = "null" ]
do
	sleep 15
	date2=$(date +"%s")
	diff=$(($date2-$date1))
	echo "Polling $(($diff / 60))m $(($diff % 60))s $URL"
	URL=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[0].OutputValue')
done

echo URL=$URL
