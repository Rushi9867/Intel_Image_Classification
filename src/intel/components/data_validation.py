import sys 
from src.intel.logger.logger import logging
from src.intel.exception.exception import IntelException
from src.intel.constants import * 
from src.intel.entity.config_entity import * 
from src.intel.entity.artifact_entity import *
from src.intel.utils import * 

class DataValidation:
    def __init__(self,data_ingestion_artifact : DataIngestionArtifacts):
        self.data_ingestion_artifact = data_ingestion_artifact
        self._schema_config = read_yaml_file(file_path=SCHEMA_PATH)

    def count_classes(self,path):
        outcomes = os.listdir(path)
        status = len(outcomes) == len(self._schema_config["calsses"])
        return status 

    def initiate_data_validation(self):
        try: 
            logging.info("Initiating the data ingestion componenet...")
            validation_error_msg = ''

            status = self.count_classes(path=self.data_ingestion_artifact.tarin_file_path)
            if not status:
                validation_error_msg += "Classes are missing in training data"

            status = self.count_classes(path=self.data_ingestion_artigfact.test_file_path)
            if not status:
                validation_error_msg += "Classes are missing in test data"

            validation_status = len(validation_error_msg) == 0

            data_validation_artifact = DataValidationArtifcat(
                validation_status = validation_status
            )

            logging.info(f"Data Validation Artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise IntelException(e,sys) from e 