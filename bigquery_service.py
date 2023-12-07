from google.cloud import bigquery
import os

class BigqueryService:
    def __init__(self, google_config, name_mapping):
        self.__project_id = google_config['project_id']
        self.__dataset_id = google_config['bq_dataset_id']
        self.client = bigquery.Client()
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
        self.name_mapping = name_mapping
    
    def create_tables(self):
        for table_name, file_name in self.name_mapping.items():
            job_config = bigquery.LoadJobConfig(
                autodetect = True,
                source_format = bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            table_ref = f"{self.__dataset_id}.{table_name}"
            with open(file_name,'rb') as f:
                job = self.client.load_table_from_file(f,table_ref,job_config=job_config)
                job.result()