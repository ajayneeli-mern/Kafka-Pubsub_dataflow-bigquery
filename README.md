bulid kafka producer that take data from csv and produce streamming

bulid consumer that read each message and valid it and publish it to pubsub topic

check
--------
check messages are publishing or not
gcloud pubsub subscriptions pull streaming-df-sub --project=just-camera-432415-h9 --limit=10 --auto-ack

-------------------------------
bulid dataflow job

with apache beam and runit with dataflow runner

then with dataflowrunner and then uncomment temlete so that it will create templetein the location

run job with command
----------------------
gcloud dataflow jobs run dataflow-csvbigquery   --gcs-location gs://newpipeline001/kafkaPubdataflowBq/dataflow_template   --region us-central1


and check streaming data updated in biqtable
--------------------------------------------------
SELECT * FROM `just-camera-432415-h9.001projectbigquery.covidIndia2` limit 10
