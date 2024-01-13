# Import the necessary modules
from google.cloud import bigquery

from classes.bigquery_io import BigQueryIO
from classes.ConfigManagerClass import ConfigManager

# Create an instance of the BigQueryIO class

config = ConfigManager(
    yaml_filepath='C:/Users/Admin/OneDrive/Desktop/_work/__repos (unpublished)/_____CONFIG/google-analytics-insight-generation/config',
    yaml_filename='config.yaml'
    )

bq_client = bigquery.Client(
    project = config.bq_project_id,
    credentials = config.service_account_credentials
)
bq_io = BigQueryIO(bq_client)


# Generate the query using the SQL file
query = bq_io.generate_query_and_send_to_bq(
    table_id = config.primary_bq_query_table_id,
    sql_file_path= config.primary_bq_query_querypath
    )
