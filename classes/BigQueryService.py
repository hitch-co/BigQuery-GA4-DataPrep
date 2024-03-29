#bigquery_io.py
import os
import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from google.api_core.exceptions import GoogleAPIError

from classes.LoggingManager import LoggerClass
from classes.ConfigManager import ConfigManager

logger_name_str = 'log_bigquery_io'
runtime_logger_level = 'INFO'

class BigQueryService:
    def __init__(self, bq_client) -> None:
        # Loading configuration from a YAML file
        ConfigManager(
            yaml_filepath='C:/Users/Admin/OneDrive/Desktop/_work/__repos (unpublished)/_____CONFIG/google-analytics-insight-generation/config',
            yaml_filename='config.yaml'
        )
        logger_service = LoggerClass(
            dirname='log', 
            logger_name=logger_name_str,
            debug_level=runtime_logger_level,
            mode='w',
            stream_logs=True
            )
        self.logger = logger_service.create_logger()

        self.bq_client = bq_client

    def _generate_bq_query_from_file(self,
        replacements: dict, 
        sql_file_path: str, 
        query_vs_create='query'
        ) -> str:
        
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
        self.logger.info("\n")
        return query_final

    def _send_queryjob_to_bq(self, query):
        try:
            # Start the query job
            query_job = self.bq_client.query(query)

            # Wait for the job to complete (this will block until the job is done)
            self.logger.debug(f"Starting BigQuery job... Executing query...")
            self.logger.debug(query_job.result())

            # Get the results if available
            dataframe = query_job.to_dataframe()
            if dataframe is None:
                self.logger.info("If query was set to 'create', no results are returned")
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

    def execute_query_from_filepath(
            self,
            sql_file_path: str,
            dataset_id: str,
            table_id: str,
            query_vs_create='query'
            ):
        # Input validation
        if not sql_file_path.endswith('.sql'):
            self.logger.error("The sql_file_path does not point to a .sql file.")
            return None

        if not dataset_id or not table_id:
            self.logger.error("Dataset ID and Table ID cannot be empty.")
            return None

        # Generate the query
        replacements = {
            'dataset_id': dataset_id,
            'table_id': table_id
        }

        try:
            # Generate the query from sql_file_path
            query = self._generate_bq_query_from_file(
                replacements=replacements,
                sql_file_path=sql_file_path,
                query_vs_create=query_vs_create
            )

            # Send the query to BigQuery
            results = self._send_queryjob_to_bq(query)
            
            if results is not None:
                self.logger.debug(f"Query results: {results}")
            else:
                self.logger.info("No results returned from the query.")

            return results

        except FileNotFoundError:
            self.logger.error(f"The SQL file at {sql_file_path} was not found.")
            return None
        except Exception as e:
            self.logger.error(f"An error occurred while executing the query: {e}")
            return None

    @LoggerClass.log_class_args(logger_name_str)
    def execute_queries_from_json(
        self, 
        json_array,
        query_vs_create='query'
        ):        
        required_keys = {'execution_order_id', 'dataset_id', 'query_path', 'schema'}
        for key, item in json_array.items():
            self.logger.debug(f"item: {item}")
            if not required_keys.issubset(item):
                missing_keys = required_keys - item.keys()
                raise ValueError(f"Missing required keys: {missing_keys}")

        sorted_queries = sorted(json_array.items(), key=lambda x: x[1]['execution_order_id'])

        for query_name, query_info in sorted_queries:
            sql_file_path = query_info['query_path']
            dataset_id = query_info['dataset_id']
            table_id = query_name      

            self.logger.info(f"query_name: {query_name}")
            self.logger.info(f"sql_file_path: {sql_file_path}")
            self.logger.info(f"dataset_id: {dataset_id}")
            self.logger.info(f"table_id: {table_id}")

            # Execute the query
            self.execute_query_from_filepath(
                sql_file_path=sql_file_path,
                dataset_id=dataset_id,
                table_id=table_id,
                query_vs_create=query_vs_create
            )
  
def main():
    bq_client = bigquery.Client()
    bq = BigQueryService(bq_client)

    # # 2. Generate a query from a filepath
    # bq._generate_bq_query_from_file(
    #     replacements={'dataset_id': 'test_dataset', 'table_id': 'test_table'},
    #     table_id='', 
    #     query_vs_create=''
    #     )

    #3. Execute queries from a json array
    with open(r'C:\_repos\google-analytics-insight-generation\config\bq_table_config.json', 'r') as file:
        json_array = json.load(file)
    
    json_array_updated = {'_transaction_items': json_array['_transaction_items']}
    bq.execute_queries_from_json(
        json_array=json_array_updated,
        query_vs_create='query'
        )

if __name__ == '__main__':
    main()
