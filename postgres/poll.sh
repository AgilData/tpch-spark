date1=$(date +"%s")

MASTER_IP=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[] | select(.OutputKey == "MasterIP").OutputValue' | egrep -o "[0-9\.]*")
while [ "$DRIVER_IP" = "" ]
do
	sleep 15
	date2=$(date +"%s")
	diff=$(($date2-$date1))
	echo "Polling $(($diff / 60))m $(($diff % 60))s $URL"
	MASTER_IP=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[] | select(.OutputKey == "MasterIP").OutputValue' | egrep -o "[0-9\.]*")
done

echo "Graphite URL http://$MASTER_IP/"
echo "Grafana URL http://$MASTER_IP:3000/login"
echo "Logs http://$DRIVER_IP:8011/log.txt"
echo "Charts http://$DRIVER_IP:8011/charts/index.html"
