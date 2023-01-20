import os
import json
from datetime import datetime

import pandas as pd
from google.cloud import storage
from sodapy import Socrata

LAST_COMPLAINT_QUERY = (
    """
    SELECT created_date
    WHERE
        complaint_type = "Dead Animal"
        AND incident_zip IS NOT NULL
        AND community_board IS NOT NULL
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    ORDER BY
        created_date DESC,
        unique_key DESC
    LIMIT 1
    """
)

def query_socrata(event, context):
    # Env variables
    domain = os.environ['DOMAIN']
    dataset = os.environ['DATASET']
    app_token = os.environ['APP_TOKEN']
    project_name = os.environ['PROJECT_NAME']
    bucket_name = os.environ['BUCKET_NAME']
    metadata_name = os.environ['METADATA_NAME']
    data_path = os.environ['DATA_PATH']
    
    # Get metadata blob
    storage_client = storage.Client(project=project_name)
    bucket = storage_client.bucket(bucket_name)
    md_blob = bucket.blob(metadata_name)
    metadata = json.loads(md_blob.download_as_bytes())

    # Create Socrata client
    socrata_client = Socrata(domain=domain, app_token=app_token, timeout=60)

    # Check if there are new 311 Dead Animal complaints
    last_complaint = socrata_client.get(dataset_identifier=dataset, query=LAST_COMPLAINT_QUERY)[0]['created_date']
    last_date = metadata['last_date']
    md_date = datetime.fromisoformat(last_date)
    lc_date = datetime.fromisoformat(last_complaint)
    should_pull = True if md_date != lc_date else False

    if (should_pull):
        new_query = (
            """
            SELECT
                unique_key,
                created_date,
                descriptor,
                incident_zip,
                community_board,
                latitude,
                longitude
            WHERE
                created_date > '{}'
                AND complaint_type = "Dead Animal"
                AND incident_zip IS NOT NULL
                AND community_board IS NOT NULL
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
            ORDER BY
                created_date DESC,
                unique_key DESC
            LIMIT
                20000
            """.format(last_date)
        )
        results = socrata_client.get(dataset_identifier=dataset, query=new_query)
        new_df = pd.DataFrame.from_records(results)

        # Convert datatypes
        new_df['created_date'] = pd.to_datetime(new_df['created_date'])
        new_df['latitude'] = pd.to_numeric(new_df['latitude'])
        new_df['longitude'] = pd.to_numeric(new_df['longitude'])

        # Read old data
        old_df = pd.read_parquet(data_path)

        # Concat DataFrames and write to storage
        df = pd.concat([new_df, old_df])
        df.to_parquet(data_path)
        now = datetime.today().isoformat('T', 'seconds')
        print('Data Added: ' + now)

        # Rewrite metadata
        metadata['last_date'] = new_df.iloc[0]['created_date'].isoformat()
        metadata['last_pull'] = now

    else:
        print('No New Data: ' + datetime.today().isoformat('T', 'seconds'))

    socrata_client.close()
    storage_client.close()