# gvinodh89

It analysis the 10 most frequent routes of the trips reported in the last 30 minutes and runs for every 1 second

1. External jar simplelatlng-1.3.1.jar is required for latitude and longitude calculations
2. Streaming data: while read -r line ; do echo "$line"; sleep 1; done < /tmp/sorted_data.csv | nc -l -p 8787
3. spark-submit --class SparkApp --num-executors 4 --jars simplelatlng-1.3.1.jar TaxiFrequentRoutes.jar
