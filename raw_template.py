import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

class RemoveWhiteSpace(beam.DoFn):
    def process(self, element):
        cleaned_element = element.strip()
        yield cleaned_element

class RemoveDuplicates(beam.DoFn):
    def process(self, element):
        unique_elements = set(element)
        for unique_element in unique_elements:
            yield unique_element

table_schema = {
    "fields": [
        {"name": "Entity", "type": "STRING"},
        {"name": "Code", "type": "STRING"},
        {"name": "Year", "type": "INTEGER"},
        {"name": "Age_standardized_suicide_rate_both_sexes", "type": "FLOAT"}
    ]
}

def create_dataset(dataset_id):
    client = bigquery.Client()
    project_id = 'cobalt-abacus-415516'

    # Crie o conjunto de dados
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"  # Defina a localização do conjunto de dados
    client.create_dataset(dataset)  # Crie o conjunto de dados no BigQuery

def run():
    dataset_id = 'raw_data'
    create_dataset(dataset_id)

    options = PipelineOptions(
        runner='DataflowRunner',
        project='cobalt-abacus-415516',
        region='southamerica-east1',
        temp_location='gs://pipeline_templates_rods/temp',
        staging_location='gs://pipeline_templates_rods/staging',
    )

    with beam.Pipeline(options=options) as p:
        # Reading Raw Data
        raw_data = p | 'ReadData' >> beam.io.ReadFromText('gs://lz_car_data/death-rate-from-suicides-gho new.csv')

        # Cleaning White Spaces
        cleaned_data = raw_data | 'RemoveWhiteSpace' >> beam.ParDo(RemoveWhiteSpace())

        # Deduplicating Values
        deduplicated_data = cleaned_data | 'RemoveDuplicates' >> beam.ParDo(RemoveDuplicates()) 

        deduplicated_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            project            = 'cobalt-abacus-415516',
            table              = 'cobalt-abacus-415516.raw_data.raw_data',
            schema             =  table_schema,
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition  = 'WRITE_APPEND' 
        )

if __name__ == '__main__':
    run()
