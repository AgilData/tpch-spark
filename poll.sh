date1=$(date +"%s")

DRIVER_IP=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[] | select(.OutputKey == "TestRunnerIp").OutputValue' | egrep -o "[0-9\.]*")
while [ "$DRIVER_IP" = "" ]
do
	sleep 15
	date2=$(date +"%s")
	diff=$(($date2-$date1))
	echo "Polling $(($diff / 60))m $(($diff % 60))s $URL"
	DRIVER_IP=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[] | select(.OutputKey == "TestRunnerIp").OutputValue' | egrep -o "[0-9\.]*")
done

MASTER_IP=$(aws cloudformation describe-stacks --stack-name $UUID | jq '.Stacks[0].Outputs[] | select(.OutputKey == "MasterIP").OutputValue' | egrep -o "[0-9\.]*")

echo "Spark Master http://$MASTER_IP:8080/"
echo "Kudu Master http://$MASTER_IP:8051/"
echo "Graphite URL http://$MASTER_IP/"
echo "Grafana URL http://$MASTER_IP:3000/login"
echo "Spark Driver http://$DRIVER_IP:4040/"
echo "Logs http://$DRIVER_IP:8011/spark-kudu/log.txt"
echo "Power http://$DRIVER_IP:8011/spark-kudu/tpch_result/power.csv"
echo "Result http://$DRIVER_IP:8011/spark-kudu/tpch_result/result.csv"
echo "Charts http://$DRIVER_IP:8011/charts/index.html"
