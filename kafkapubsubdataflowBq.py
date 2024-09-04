import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json

class TransformMessage(beam.DoFn):
    def process(self, element):
        try:
            # Parse JSON
            data = json.loads(element.decode('utf-8'))
            
            # Map fields to match BigQuery schema
            transformed_data = {
                'State_UTs': data.get('State/UTs'),
                'Total_Cases': data.get('Total Cases'),
                'Active': data.get('Active'),
                'Discharged': data.get('Discharged'),
                'Deaths': data.get('Deaths'),
                'Active_Ratio': data.get('Active Ratio'),
                'Discharge_Ratio': data.get('Discharge Ratio'),
                'Death_Ratio': data.get('Death Ratio'),
                'Population': data.get('Population')
            }
            
            yield transformed_data
        except json.JSONDecodeError:
            pass  # Optionally log or handle invalid JSON

def run_pipeline(argv=None):
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Run on Dataflow
        project='just-camera-432415-h9',  # GCP Project ID
        region='asia-east1',  # Region
        temp_location='gs://001project/temp',  # GCS bucket for temporary files
        staging_location='gs://001project/staging',  # GCS bucket for staging files
        job_name='kafka-pubsub-dataflow-bq',  # Unique job name
        save_main_session=True,  # Required for Dataflow
        #template_location='gs://newpipeline001/kafkaPubdataflowBq/dataflow_template'
    )
    
    # Set up standard options for the pipeline
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True  # Enable streaming mode for Dataflow
    
    # Define your pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read messages from Pub/Sub topic
        messages = (p
                    | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic='projects/just-camera-432415-h9/topics/streaming-df')
                    | "TransformMessage" >> beam.ParDo(TransformMessage())
                    | "WindowIntoFixedIntervals" >> beam.WindowInto(beam.window.FixedWindows(60))  # Define window size, e.g., 60 seconds
                   )
        
        # Deduplicate using Distinct
        deduplicated = (messages
                        | "Distinct" >> beam.Distinct()
                       )
        
        # Write messages to BigQuery
        (deduplicated
         | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
             table='just-camera-432415-h9:001projectbigquery.covidIndia2',
             schema='State_UTs:STRING,Total_Cases:INTEGER,Active:INTEGER,Discharged:INTEGER,Deaths:INTEGER,Active_Ratio:FLOAT,Discharge_Ratio:FLOAT,Death_Ratio:FLOAT,Population:INTEGER',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run_pipeline()
