# Importing necessary modules
from google.cloud import bigquery
from classes.BigQueryService import BigQueryService
from classes.ConfigManager import ConfigManager

# Loading configuration from a YAML file
config = ConfigManager(
    yaml_filepath='C:/Users/Admin/OneDrive/Desktop/_work/__repos (unpublished)/_____CONFIG/google-analytics-insight-generation/config',
    yaml_filename='config.yaml'
)

# Initializing BigQuery client and create an instance of BigQueryService for interacting with BigQuery
bq_client = bigquery.Client(project=config.bq_project_id)
bq_io = BigQueryService(bq_client)

# Query data or create/replace a table in BigQuery. 
query_vs_create = 'create'  # Options: 'create' or 'query'

# Define the table_id to work with in BQ (specific ID, '', 'all', or None)
runtime_table_id = '_transaction_items'  # Example: '_transaction_items'

# Printing runtime table IDs and their types for debugging and verification purposes
print(f"runtime_table_ids_json (type: {type(config.runtime_table_ids_json)}):{config.runtime_table_ids_json}")
print(f"runtime_table_ids_json['table_ids'] (type: {type(config.runtime_table_ids_json['table_ids'])}): {config.runtime_table_ids_json['table_ids']}")

# Processing custom table ID if provided
if runtime_table_id != 'all' and runtime_table_id != '' and runtime_table_id is not None:
    try:
        # Validate if runtime_table_id is present in the configuration
        if runtime_table_id not in config.runtime_table_ids_json['table_ids']:
            raise ValueError(f"Error: runtime_table_id: '{runtime_table_id}' not found in runtime_table_ids_json")
        else:
            # Set table_ids to only the specified runtime_table_id
            table_ids = [runtime_table_id]
            print(f"table_ids: '{table_ids}'")
    except KeyError:
        # Handle KeyError if the table_id is not found in configuration
        print(f"Error: Table ID '{runtime_table_id}' not found in configuration.")
    
    except ValueError as e:
        # Handle ValueError and display the custom error message
        print(e)

# Handling the case when 'all' tables are specified
else:
    # Set table_ids to all table IDs from the configuration
    table_ids = config.runtime_table_ids_json['table_ids']
    print(f"table_ids: {table_ids}")

# Looping over each table_id
for table_id in table_ids:
    try:
        # Fetching BigQuery table configuration from the config
        print(f"this is the config.bq_table_config: {config.bq_table_config}")
        bq_table_config = config.bq_table_config[table_id]
        print(f"this is the bq_table_config: {config.bq_table_config}")
        
        # Generating the query using the SQL file specified in bq_table_config
        query = bq_io.execute_query_from_filepath(
            dataset_id=bq_table_config['dataset_id'],
            table_id=table_id,
            sql_file_path=bq_table_config['query_path'],
            query_vs_create=query_vs_create
        )

        # Printing appropriate messages based on the action performed (create or query)
        if query_vs_create == 'create':
            print(f"Finished creating table_id: '{table_id}'. No preview available.")
        else:
            print(f"Preview of query:")
            print(query.head(5))
        print("\n")

    except KeyError as e:
        # Handling KeyError for an invalid table_id in the configuration
        print(f"KeyError encountered for table_id '{table_id}': {e}. This may be due to an invalid table_id in your config.")
    except Exception as e:
        # Handling any other unexpected errors
        print(f"An unexpected error occurred for table_id '{table_id}': {e}")