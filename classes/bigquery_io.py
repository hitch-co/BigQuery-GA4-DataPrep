#bigquery_io.py
import os

from google.cloud.exceptions import NotFound, GoogleCloudError
from google.api_core.exceptions import GoogleAPIError

from classes.my_logging import create_logger

class BigQueryIO:
    def __init__(self, bq_client) -> None:
        #setup logger
        self.logger = create_logger(
            dirname='log', 
            logger_name='log_bigquery_io',
            debug_level='INFO',
            mode='w',
            stream_logs=True
            )

        self.bq_client = bq_client

    def _create_or_replace_bq_table_from_gcs(self,
            project_name, 
            source_bucket_name,
            source_dir_path,
            source_file_name,
            target_dataset_name, 
            target_table_name,
            schema,
            is_testing_run=False):

        self.logger.debug('---------------------------------')
        self.logger.debug(f"project_name: {project_name}")
        self.logger.debug(f"source_bucket_name: {source_bucket_name}")
        self.logger.debug(f"source_dir_path: {source_dir_path}")
        self.logger.debug(f"source_file_name: {source_file_name}")
        self.logger.debug(f"target_dataset_name: {target_dataset_name}")
        self.logger.debug(f"target_table_name: {target_table_name}")
        self.logger.debug(f"schema: {schema}")
        self.logger.debug(f"")
        try:
            client = bigquery.Client()
            gcs_uri = f"gs://{source_bucket_name}/{source_dir_path}/{source_file_name}"
            table_fullqual = f"{project_name}.{target_dataset_name}.{target_table_name}"
            table_ref = bigquery.TableReference.from_string(table_fullqual)

            self.logger.info(f"Source URI from GCS is: {gcs_uri}")
            self.logger.info(f"Target BQ table: {table_fullqual}")

            try: # Check if the table already exists in BQ
                client.get_table(table_ref)
                self.logger.info(f"{table_fullqual} already exists so a new one was not created. continuing with job load for {gcs_uri}.")
            except NotFound:
                self.logger.warning(f"{table_fullqual} was not found. Creating new table.")
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                self.logger.info(f"{table_fullqual} created, continuing with job load.")

            # Configure the external data source and start the BigQuery Load job
            job_config = bigquery.LoadJobConfig(
                autodetect=False,
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE   
            )
            load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job_result = load_job.result()

            self.logger.info(f"This is the load_job_result: {load_job_result}")
            return load_job_result

        except GoogleCloudError as e:
            self.logger.error(f"Google Cloud Error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return None

    def _generate_bq_query_from_file(self, table_id: str, sql_file_path: str) -> str:
        """
        Reads a BigQuery SQL command from a .sql file and formats it with the provided table ID.
        
        This function assumes that the .sql file contains a SQL template where `{table_id}` is a placeholder
        for the actual table name to be used in the CREATE OR REPLACE TABLE command.
        
        Parameters:
        - table_id (str): The ID of the table to create or replace in BigQuery.
        - sql_file_path (str): The file path to the .sql file containing the SQL template.
        
        Returns:
        - str: A string containing the formatted BigQuery SQL command.
        """
        self.logger.info('---------------------------------')

        sql_file_path = os.path.join(sql_file_path)

        # Read the SQL command from the .sql file
        with open(sql_file_path, 'r') as file:
            sql_template = file.read()
        
        # Format the SQL with the table_id
        query = sql_template.format(table_id=table_id)

        # Log the generation of the query
        self.logger.info(f"The BigQuery table creation query for [{table_id}]was generated from file:")
        self.logger.debug(query)

        # Return the formatted query string
        return query

    def _send_queryjob_to_bq(self, query):

        try:
            # Start the query job
            # bq_client = bigquery.Client()
            self.logger.info("Starting BigQuery job...")
            query_job = self.bq_client.query(query)

            # Wait for the job to complete (this will block until the job is done)
            self.logger.info(f"Executing query... result to come:")
            self.logger.info(query_job.result())

        except GoogleAPIError as e:
            # Log any API errors
            self.logger.error(f"BigQuery job failed: {e}")

        except Exception as e:
            # Log any other exceptions
            self.logger.error(f"An unexpected error occurred: {e}")

        else:
            # Optionally, get and log job statistics
            job_stats = query_job.query_plan
            self.logger.debug(f"Query plan: {job_stats}")

    def generate_query_and_send_to_bq(
            self,
            table_id:str,
            sql_file_path:str
    ):
        # Generate the query
        query = self._generate_bq_query_from_file(
            table_id=table_id,
            sql_file_path=sql_file_path
        )

        # Send the query to BigQuery
        self._send_queryjob_to_bq(query)

def main():
    bq = BigQueryIO()

    # # 1. Create or replace a table from a GCS file 
    # bq._create_or_replace_bq_table_from_gcs(
    #     project_name=, 
    #     source_bucket_name=,
    #     source_dir_path,
    #     source_file_name=,
    #     target_dataset_name=, 
    #     target_table_name=,
    #     schema=,
    #     is_testing_run=False)

    # # 2. Generate a query from a file
    # bq._generate_bq_query_from_file(
    #     table_id='', 
    #     sql_file_path=''
    #     )
    print(bq)

if __name__ == '__main__':
    main()
