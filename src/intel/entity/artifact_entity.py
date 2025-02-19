from dataclasses import dataclass 

# Data Ingestion Artifacts 
@dataclass
class DataIngestionArtifacts:
    train_file_path: str 
    test_file_path: str 
    pred_file_path: str 
    data_path: str 

@dataclass
class DataValidationArtifact:
    validation_status: bool