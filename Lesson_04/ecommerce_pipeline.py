import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants that students will modify
PROJECT_ID = "cas-daeng-2024-pect"  # Students replace with their project
REGION = "europe-west1"             # Cloud Storage bucket location
TRANSACTIONS_FILE = "gs://retail-data-pect/transactions.csv"  # Cloud Storage bucket location
TEMP_LOCATION = "gs://retail-data-pect/temp"  # Cloud Storage bucket location

# Additional constants
DATASET_ID = "ecommerce"
TABLE_ID = "transactions"
FULL_TABLE_ID = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"


class ParseCSV(beam.DoFn):
    """Parse each CSV row into a dictionary."""
    def process(self, element):
        try:
            # Skip header row
            if element.startswith('transaction_id'):
                return []
            
            # Parse CSV line
            transaction_id, customer_id, product_id, quantity, \
            timestamp, category, total_amount = element.strip().split(',')
            
            # Return formatted dictionary
            return [{
                'transaction_id': int(transaction_id),
                'customer_id': int(customer_id),
                'product_id': int(product_id),
                'quantity': int(quantity),
                'timestamp': timestamp,
                'category': category,
                'total_amount': float(total_amount)
            }]
        except Exception as e:
            logger.error(f"Error processing line: {element}, Error: {str(e)}")
            return []


def run_pipeline():
    """Execute the Dataflow pipeline."""
    logger.info(f"Starting pipeline with project ID: {PROJECT_ID}")
    
    # Define table schema
    schema = 'transaction_id:INTEGER,customer_id:INTEGER,product_id:INTEGER,' \
            'quantity:INTEGER,timestamp:TIMESTAMP,category:STRING,total_amount:FLOAT'

    # Configure pipeline options
    options = PipelineOptions([
        f'--project={PROJECT_ID}',
        f'--region={REGION}',
        f'--temp_location={TEMP_LOCATION}',
        '--job_name=ecommerce-processing',
        '--runner=DataflowRunner'
    ])

    logger.info(f"Reading data from: {TRANSACTIONS_FILE}")
    
    # Create and run the pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Process data
        (pipeline 
         | 'ReadCSV' >> beam.io.ReadFromText(TRANSACTIONS_FILE)
         | 'ParseCSV' >> beam.ParDo(ParseCSV())
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             FULL_TABLE_ID,
             schema=schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
         ))


if __name__ == '__main__':
    try:
        logger.info("Submitting Dataflow job...")
        run_pipeline()
        logger.info("Pipeline submitted successfully!")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise