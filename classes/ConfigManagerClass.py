import os
import yaml
import dotenv
import json

from classes.my_logging import create_logger

runtime_logger_level = 'DEBUG'

class ConfigManager:
    _instance = None

    def __new__(cls, yaml_filepath=None, yaml_filename=None):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance.init_attributes()
            cls._instance.initialize_config(yaml_filepath, yaml_filename)
        return cls._instance

    def init_attributes(self):
        # Create logger
        self.logger = create_logger(
            dirname='log', 
            logger_name='logger_ConfigManagerClass', 
            debug_level=runtime_logger_level,
            mode='w',
            stream_logs=True,
            encoding='UTF-8'
            )
        
        # Initialize all instance attributes
        self.env_dir_path = None

    def initialize_config(self, yaml_filepath, yaml_filename):
        yaml_full_path = os.path.join(yaml_filepath, yaml_filename)
        self.load_yaml_config(yaml_full_path)
        self.set_env_variables()
        self.load_json_schemas()

    def load_json_schemas(self):
        filepath = os.path.join(self.bq_table_config_dirpath, self.bq_table_config_filename)
        try:
            with open(filepath, 'r') as file:
                self.bq_table_config = json.load(file)
        except Exception as e:
            self.logger.error(f"Error loading JSON config file: {e}")  

    def get_json_item(self, json, json_key):
        return json.get(json_key)     

    def load_yaml_config(self, yaml_full_path):
        try:
            with open(yaml_full_path, 'r') as file:
                yaml_config = yaml.safe_load(file)
                self.update_config_from_yaml(yaml_config)
        except FileNotFoundError:
            self.logger.error(f"YAML configuration file not found at {yaml_full_path}")
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration: {e}")

    def set_env_variables(self):
        if self.env_dir_path and self.env_file_name:
            env_path = os.path.join(self.env_dir_path, self.env_file_name)
            if os.path.exists(env_path):
                dotenv.load_dotenv(env_path)
                self.update_config_from_env()
            else:
                self.logger.error(f".env file not found at {env_path}")

    def update_config_from_env(self):
        self.service_account_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    def update_config_from_yaml(self, yaml_config):
        # Update instance variables with YAML configurations
        self.env_dir_path = yaml_config.get('env_dir_path')
        self.env_file_name = yaml_config.get('env_file_name')

        # BQ table config paths (schemas and querypaths)
        self.bq_table_config_dirpath = yaml_config.get('bq_table_config_dirpath')
        self.bq_table_config_filename = yaml_config.get('bq_table_config_filename')

        # BQ Details/Queries:
        self.bq_project_id = yaml_config.get('bq_details', {}).get('bq_project_id')
        self.primary_bq_query_dataset_id = yaml_config.get('bq_details', {}).get('bq_dataset_id')

def main():
    config_manager = ConfigManager(
        yaml_filepath='C:/Users/Admin/OneDrive/Desktop/_work/__repos (unpublished)/_____CONFIG/google-analytics-insight-generation/config',
        yaml_filename='config.yaml'
        )
    
    # Test cases
    item = config_manager.bq_table_config['my_test_table1']
    print(config_manager.bq_table_config)
    print(item['query_path'])
    print(config_manager.bq_project_id)

if __name__ == "__main__":
    main()
