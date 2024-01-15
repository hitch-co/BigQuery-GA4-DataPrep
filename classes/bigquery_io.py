#bigquery_io.py
import os

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from google.api_core.exceptions import GoogleAPIError

from classes.my_logging import create_logger

class BigQueryIO:
    def __init__(self, bq_client) -> None:
        self.logger = create_logger(
            dirname='log', 
            logger_name='log_bigquery_io',
            debug_level='INFO',
            mode='w',
            stream_logs=True
            )

        self.bq_client = bq_client

    def _log_parameters(self, **params):
        for key, value in params.items():
            self.logger.debug(f"{key}: {value}")
        self.logger.debug("")

    def _generate_bq_query_from_file(self, replacements: dict, sql_file_path: str, query_vs_create='query') -> str:
        self.logger.info('---------------------------------')
        self.logger.debug(f"replacements: {replacements}")
        self.logger.debug(f"sql_file_path: {sql_file_path}")

        # Read the SQL command from the .sql file
        sql_file_path = os.path.join(sql_file_path)
        with open(sql_file_path, 'r') as file:
            sql_template = file.read()

        # Format the SQL with the replacements dictionary
        query = sql_template.format(**replacements)

        # Add the optional create/replace logic
        if query_vs_create == 'create':
            assert 'dataset_id' in replacements.keys(), "replacements must contain a key for 'dataset_id'"
            assert 'table_id' in replacements.keys(), "replacements must contain a key for 'table_id'"

            query_final = f"CREATE OR REPLACE TABLE {replacements['dataset_id']}.{replacements['table_id']} AS ({query})"    
            message = f"The BigQuery table creation query for [{replacements['table_id']}] was generated from a local file:"

        elif query_vs_create == 'query':
            query_final = query
            message = f"The BigQuery Query was completed for .sql file: {sql_file_path}"

        # Log the generation of the query
        self.logger.info(message)
        self.logger.debug(query_final)

        # Return the formatted query string
        return query_final

    def _send_queryjob_to_bq(self, query):

        try:
            # Start the query job
            self.logger.info("Starting BigQuery job...")
            query_job = self.bq_client.query(query)

            # Wait for the job to complete (this will block until the job is done)
            self.logger.info(f"Executing query... result to come:")
            self.logger.info(query_job.result())

            # Get the results if available
            dataframe = query_job.to_dataframe()
            if dataframe is None:
                self.logger.info("Query likely set to 'create', thus returned no results")
            else: 
                return dataframe

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
            sql_file_path: str,
            dataset_id: str,
            table_id: str,
            query_vs_create='query'
            ):
        
        # Generate the query
        replacements = {
            'dataset_id': dataset_id,
            'table_id': table_id
        }

        # Generate the query from sql_file_path
        query = self._generate_bq_query_from_file(
            replacements=replacements,
            sql_file_path=sql_file_path,
            query_vs_create=query_vs_create
        )

        # Send the query to BigQuery
        results = self._send_queryjob_to_bq(query)
        
        self.logger.debug(f"results: {results}")
        return results
    
def main():
    bq = BigQueryIO()
    # # 2. Generate a query from a file
    # bq._generate_bq_query_from_file(
    #     table_id='', 
    #     sql_file_path=''
    #     )
    print(bq)

if __name__ == '__main__':
    main()
