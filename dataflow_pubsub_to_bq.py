import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


class ParseJsonMessage(beam.DoFn):
    def process(self, element):
        try:
            row = json.loads(element.decode("utf-8"))
            yield row
        except Exception as e:
            pass


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--region', required=True, help='GCP region')
    parser.add_argument('--input_topic', required=True, help='Pub/Sub topic to read from')
    parser.add_argument('--output_table', required=True, help='BigQuery table to write to')
    parser.add_argument('--temp_location', required=True, help='GCS temp location')
    parser.add_argument('--runner', default='DataflowRunner')

    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | 'Parse JSON' >> beam.ParDo(ParseJsonMessage())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                args.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=args.temp_location,
                schema={
                    'fields': [
                        {'name': 'traffic_volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'holiday', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'temp', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'rain_1h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'snow_1h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'clouds_all', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'weather_main', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'weather_description', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'date_time', 'type': 'STRING', 'mode': 'NULLABLE'},
                    ]
                }
            )
        )


if __name__ == '__main__':
    run()
