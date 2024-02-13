# Importing necessary modules
from google.cloud import bigquery
from classes.BigQueryService import BigQueryService
from classes.ConfigManager import ConfigManager
from classes.LoggingManager import LoggerClass

class DataProcessor:
    def __init__(self, config_path, config_file, logger_name, logger_level='DEBUG'):
        self.config = self.load_config(config_path, config_file)
        self.logger = self.setup_logger(logger_name, logger_level)
        self.bq_client = self.initialize_bq_client()
        self.bq_service = BigQueryService(self.bq_client)

    def load_config(self, path, filename):
        return ConfigManager(
            yaml_filepath=path,
            yaml_filename=filename
        )

    def setup_logger(self, logger_name, debug_level):
        logger_service = LoggerClass(
            dirname='log',
            logger_name=logger_name,
            debug_level=debug_level,
            mode='w',
            stream_logs=True
        )
        return logger_service.create_logger()

    def initialize_bq_client(self):
        return bigquery.Client(project=self.config.bq_project_id)

    def process_data(self, runtime_table_id='all', action='create'):
        try:
            if runtime_table_id not in ['all', '', None]:
                json_array = {runtime_table_id: self.config.bq_table_config[runtime_table_id]}
            else:
                json_array = self.config.bq_table_config

            self.bq_service.execute_queries_from_json(
                json_array=json_array,
                query_vs_create=action
            )

        except ValueError as e:
            self.logger.error(e)
        except KeyError as e:
            self.logger.error(f"KeyError encountered: {e}. This may be due to an invalid bq_table_config.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

# Main execution
if __name__ == "__main__":

    # Initialize the data processing engine
    engine = DataProcessor(
        config_path = 'C:/Users/Admin/OneDrive/Desktop/_work/__repos (unpublished)/_____CONFIG/google-analytics-insight-generation/config',
        config_file = 'config.yaml',
        logger_name='main', 
        logger_level='DEBUG'
        )
    
    # Process selected tables using the 'create' or 'query' action
    engine.process_data(
        runtime_table_id='all', 
        action='create'
        )