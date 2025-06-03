import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from collections import Counter
import apache_beam.io.filesystems as fs
import csv
from apache_beam.transforms.window import FixedWindows

class ExtractColsForAggregation(beam.DoFn):
    def __init__(self, schema):
        self.schema = schema

    def process(self, row):
        for col, dtype in self.schema.items():
            val = row.get(col)
            if val is None or val == '':
                continue
            try:
                if dtype == 'int':
                    val = int(val)
                    yield (col, val)
                elif dtype == 'double':
                    val = float(val)
                    yield (col, val)
                else:
                    yield (col, val)
            except:
                # Skip invalid values
                pass
        yield beam.pvalue.TaggedOutput('rows', row)

class ComputeFillValues(beam.DoFn):
    def process(self, element):
        col, values = element
        values_list = list(values)
        if not values_list:
            return
        first_val = values_list[0]
        if isinstance(first_val, (int, float)):
            fill_val = sum(values_list) / len(values_list)
            if isinstance(first_val, int):
                fill_val = int(round(fill_val))
        else:
            fill_val = Counter(values_list).most_common(1)[0][0]
        yield (col, fill_val)

class CleanRows(beam.DoFn):
    def __init__(self, fill_values, schema):
        self.fill_values = fill_values
        self.schema = schema

    def process(self, row):
        vin = row.get('vin')
        if vin is None or vin == '':
            # Remove rows missing VIN
            return
        cleaned_row = {}
        for col, dtype in self.schema.items():
            val = row.get(col)
            if val is None or val == '':
                cleaned_row[col] = self.fill_values.get(col)
            else:
                try:
                    if dtype == 'int':
                        cleaned_row[col] = int(val)
                    elif dtype == 'double':
                        cleaned_row[col] = float(val)
                    else:
                        cleaned_row[col] = val
                except:
                    cleaned_row[col] = self.fill_values.get(col)
        yield cleaned_row

class ReadCSVFile(beam.DoFn):
    def process(self, file_path):
        with fs.FileSystems.open(file_path) as f:
            reader = csv.DictReader(line.decode('utf-8') for line in f)
            for row in reader:
                yield row

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='zinc-wares-460713-v0',
        region='africa-south1',
        temp_location='gs://p2-temp-bucket/temp',
        staging_location='gs://p2-temp-bucket/staging',
        job_name='p2-pipeline-job',
        streaming=True
    )
    options.view_as(StandardOptions).streaming = True

    schema = {
        'date_id': 'int',
        'seller_id': 'int',
        'make': 'string',
        'model': 'string',
        'trim': 'string',
        'body': 'string',
        'transmission': 'string',
        'vin': 'string',
        'state': 'string',
        'condition': 'double',
        'odometer': 'double',
        'color': 'string',
        'interior': 'string',
        'seller': 'string',
        'mmr': 'double',
        'sellingprice': 'double',
        'saledate': 'string',
        'vehicle_id': 'string',
    }

    bq_schema_str = (
        'date_id:INTEGER, '
        'seller_id:INTEGER, '
        'make:STRING, '
        'model:STRING, '
        'trim:STRING, '
        'body:STRING, '
        'transmission:STRING, '
        'vin:STRING, '
        'state:STRING, '
        'condition:FLOAT, '
        'odometer:FLOAT, '
        'color:STRING, '
        'interior:STRING, '
        'seller:STRING, '
        'mmr:FLOAT, '
        'sellingprice:FLOAT, '
        'saledate:STRING, '
        'vehicle_id:STRING'
    )
    print(f"Schema being used:\n{bq_schema_str}")

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "ReadPubSub" >> ReadFromPubSub(
                subscription='projects/zinc-wares-460713-v0/subscriptions/p2-topic-sub',
                with_attributes=True
            )
        )

        file_paths = (
            messages
            | "GetFilePath" >> beam.Map(lambda msg: f"gs://{msg.attributes['bucket']}/{msg.att>
        )

        rows = file_paths | "ReadCSV" >> beam.ParDo(ReadCSVFile())

        extract_results = rows | "ExtractForAgg" >> beam.ParDo(ExtractColsForAggregation(schem>

        fill_values = (
            extract_results.cols
            | "WindowForAggregation" >> beam.WindowInto(FixedWindows(60))  # 60-second fixed w>
            | "GroupByCol" >> beam.GroupByKey()
            | "ComputeFillValues" >> beam.ParDo(ComputeFillValues())
        )

        fill_values_dict = beam.pvalue.AsDict(fill_values)

        cleaned_rows = (
            extract_results.rows
            | "CleanRows" >> beam.FlatMap(
                lambda row, fill_vals: CleanRows(fill_vals, schema).process(row),
                fill_vals=fill_values_dict
            )
        )

        cleaned_rows | "WriteToBigQuery" >> WriteToBigQuery(
            table='zinc-wares-460713-v0:p2_vehicles_dataset.p2_table',
            schema=bq_schema_str,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.FILE_LOADS,
            triggering_frequency=60  # Trigger BigQuery load jobs every 60 seconds in streaming
        )

if __name__ == '__main__':
    run()