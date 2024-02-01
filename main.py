# Importing necessary modules
from google.cloud import bigquery

from classes.BigQueryService import BigQueryService
from classes.ConfigManager import ConfigManager
from classes.LoggingManager import LoggerClass

# Setup logger
logger_name_str = 'main'
runtime_logger_level = 'DEBUG'
logger_service = LoggerClass(
    dirname='log', 
    logger_name=logger_name_str,
    debug_level=runtime_logger_level,
    mode='w',
    stream_logs=True
    )
logger = logger_service.create_logger()

# Loading config
config = ConfigManager(
    yaml_filepath='C:/Users/Admin/OneDrive/Desktop/_work/__repos (unpublished)/_____CONFIG/google-analytics-insight-generation/config',
    yaml_filename='config.yaml'
)

# Initializing BigQuery client and create an instance of BigQueryService for interacting with BigQuery
bq_client = bigquery.Client(project=config.bq_project_id)
bq_io = BigQueryService(bq_client)

##################################################
# Query data or create/replace a table in BigQuery. 
query_vs_create = 'create'  # Options: 'create' or 'query'

# Define the table_id to work with in BQ (specific ID, '', 'all', or None)
runtime_table_id = 'all'  # '_transaction_items'

#################################################
# Processing custom runtime_table_id if provided
if runtime_table_id != 'all' and runtime_table_id != '' and runtime_table_id is not None:
    try:
        json_ajson_array = {runtime_table_id: config.bq_table_config[runtime_table_id]}
        bq_io.execute_queries_from_json(
            json_array=json_ajson_array,
            query_vs_create=query_vs_create
        )

    except ValueError as e:
        logger.error(e)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

# runtime_table_id = 'all' tables are specified
else:
    try:
        bq_io.execute_queries_from_json(
            json_array=config.bq_table_config,
            query_vs_create=query_vs_create
        )

    except KeyError as e:
        logger.error(f"KeyError encountered: {e}. This may be due to an invalid bq_table_config.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in execute_queries_from_json(): {e}")
