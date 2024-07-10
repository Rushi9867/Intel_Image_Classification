import sys 
from src.intel.logger.logger import logging 
from src.intel.exception.exception import IntelException 
from src.intel.constants import * 
from src.intel.entity.config_entity import * 
from src.intel.entity.artifact_entity import * 
from src.intel.configuration.s3_operations import S3Operation 

class DataIngestion:
    def __init__(self,data_ingestion_config: DataIngestionConfig,S3_operations: S3Operation):
        self.data_ingestion_config = data_ingestion_config
        self.S3_operations = S3_operations 

    def get_iamges_from_s3(self,bucket_file_name: str ,bucket_name: str, output_filepath: str) -> zip:
        logging.info("Entered the get_data_from_s3 method of Data Ingestion class")
        try: 
            if not os.path.exists(output_filepath):
                self.S3_operations.read_data_from_s3(bucket_file_name, bucket_name, output_file_path)
            logging.info("Exited the get_datafromS3 method of Data Ingestion class")
        except Exception as e:
            raise IntelException(e,sys) from e

    def unzip_data(self,zip_data_filepath: str) -> Path:
        logging.info("Unzipping the downloaded zip file from download directory...")
        try: 
            if os.path.isdir(zip_data_filepath):
                logging.info(f"Unzipped folder already exist in {zip_data_filepath}")
            else:
                with ZipFile(zip_data_filepath,mode='r') as zip_ref:
                    zip_ref.extractall(self.data_ingestion_config.data_ingestion_artifact_dir)
                
                logging.info(f"Unzipping of data completed and extracted at {self.data_ingestion_config}")
        except Exception as e:
            raise IntelException(e,sys) from e 


    def initiate_data_ingestion(self) -> DataIngestionArtifacts:
        try: 
            logging.info("Initiating the data ingestion component...")
            os.makedirs(self.data_ingestion_config.data_ingestion_artfact_dir,exists_ok=True)
            logging.info(f"Created {os.path.basename(self.data_ingestion_config.data_ingestion_artfact_dir)} directory.")
            self.get_iamges_from_s3(bucket_file_name=S3_DATA_FOLDER_NAME,bucket_name=BUCKET_NAME,output_filepath=self.data_ingestion_config.zip_data_path)

            # Unziping the file 
            self.unzip_data(zip_data_filepath= self.data_ingestion_config.zip_data_path)

            data_ingestion_artifact = DataIngestionArtifacts(
                train_file_path = self.data_ingestion_config.train_path, 
                test_file_path = self.data_ingestion_config.test_path, 
                pred_file_path = self.data_ingestion_config.pred_path, 
                data_path = self.data_ingestion_config.data_path)
            
            logging.info(f"Data Ingestion Artifact {data_ingestion_artifact}")
            
            logging.info("Data Ingestion is Completed Successfully.")
        except Exception as e:
            raise IntelException(e,sys) from e 