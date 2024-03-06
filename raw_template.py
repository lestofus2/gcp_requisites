import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class RemoveWhiteSpace(beam.DoFn):
    def process(self, element):
        cleaned_element = element.strip()
        yield cleaned_element

class RemoveDuplicates(beam.DoFn):
    def process(self, element):
        unique_elements= set(element)
        for unique_element in unique_elements:
            yield unique_element

table_schema = {
    'fields': [
    {
      "value": "Store_ID",
      "type": "INTEGER"
    },
    {
      "value": "Round",
      "type": "INTEGER"
    },
    {
      "value": "Fieldworker_Code",
      "type": "STRING"
    },
    {
      "value": "Country",
      "type": "STRING"
    },
    {
      "value": "Currency",
      "type": "STRING"
    },
    {
      "value": "Day",
      "type": "INTEGER"
    },
    {
      "value": "Month",
      "type": "INTEGER"
    },
    {
      "value": "Year",
      "type": "INTEGER"
    },
    {
      "value": "Date",
      "type": "DATE"
    },
    {
      "value": "Province",
      "type": "STRING"
    },
    {
      "value": "City",
      "type": "STRING"
    },
    {
      "value": "Suburb",
      "type": "STRING"
    },
    {
      "value": "Outlet_Type",
      "type": "STRING"
    },
    {
      "value": "Retail_Subtype",
      "type": "STRING"
    },
    {
      "value": "Outlet_Name",
      "type": "STRING"
    },
    {
      "value": "Product",
      "type": "STRING"
    },
    {
      "value": "Brand",
      "type": "STRING"
    },
    {
      "value": "Sub_Brand",
      "type": "STRING"
    },
    {
      "value": "Quantity",
      "type": "INTEGER"
    },
    {
      "value": "Unusual_Quantity_Flg",
      "type": "BOOLEAN"
    },
    {
      "value": "Local_Price",
      "type": "FLOAT"
    },
    {
      "value": "Local_Price_Per_Stick_Cigarette",
      "type": "FLOAT"
    },
    {
      "value": "Dollar_Exchange_Rate",
      "type": "FLOAT"
    },
    {
      "value": "Dollar_Price",
      "type": "FLOAT"
    },
    {
      "value": "Dollar_Price_Per_Stick_Cigarette",
      "type": "FLOAT"
    },
    {
      "value": "Dollar_Price_Pack_Cigarettes",
      "type": "FLOAT"
    },
    {
      "value": "Fieldworker_Comment",
      "type": "STRING"
    },
    {
      "value": "Data_Cleaner_Comment",
      "type": "STRING"
    }
  ]
}

def run():

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