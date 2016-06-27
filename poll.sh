date1=$(date +"%s")

URL=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[0].OutputValue' | egrep -o "[0-9\.]*")
while [ "$URL" = "" ]
do
	sleep 15
	date2=$(date +"%s")
	diff=$(($date2-$date1))
	echo "Polling $(($diff / 60))m $(($diff % 60))s $URL"
	URL=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[0].OutputValue' | egrep -o "[0-9\.]*")
done

DRIVER_IP=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[1].OutputValue' | egrep -o "[0-9\.]*")

echo "Spark Master http://$DRIVER_IP:8080/"
echo "Kudu Master http://$DRIVER_IP:8051/"
echo "Graphite URL http://$DRIVER_IP/"
echo "Grafana URL http://$DRIVER_IP:3000/login"
echo "Spark Driver http://$URL:4040/"
echo "Logs http://$URL/log.txt"
