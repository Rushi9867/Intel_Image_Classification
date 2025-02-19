import os
from dataclasses import dataclass 
from src.intel.constants import * 
from datetime import datetime 
from from_root import from_root

TIMESTAMP: str = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")

@dataclass
class DataIngestionConfig:
    data_ingestion_artfact_dir: str = os.path.join(from_root(), ARTIFACTS_DIR,DATA_INGESTION_ARTIFACTS_DIR)
    zip_data_path: str = os.path.join(data_ingestion_artfact_dir,S3_DATA_FOLDER_NAME)
    data_path: str = os.path.join(data_ingestion_artfact_dir,UNZIP_FOLDER_NAME)
    train_path: str = os.path.join(data_path,TRAIN_FOLDER_NAME)
    test_path: str = os.path.join(data_path,TEST_FOLDER_NAME)
    pred_path: str = os.path.join(data_path,PRED_FOLDER_NAME)

@dataclass
class DataValidationConfig:
    schema_file_path = os.path.join("config","schema.yaml")