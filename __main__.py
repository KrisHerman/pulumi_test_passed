# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta
from pulumi import Config, log, ResourceOptions, StackReference, export
import pulumi_gcp as gcp

from devops_plm_gcp_infra.iam.alpha import (
    ServiceAccount,
)
from devops_plm_gcp_infra.storage.alpha import Bucket
from devops_plm_gcp_infra.storagetransfer.alpha import TransferJob, TransferJobArgs
from pulumi_google_native.storagetransfer import v1 as storagetransfer
from pulumi_google_native.bigquery import v2 as bigquery

fetch_rewards = Config()

#######################################################################################
# General Config
#######################################################################################
general_cfg = fetch_rewards.require_object("general")

business_unit = general_cfg.get("business_unit")
initiative = general_cfg.get("initiative")
platform = general_cfg.get("platform")
environment = general_cfg.get("environment")
default_region = general_cfg.get("default_region")

stack_infra = f"{business_unit}-{initiative}-{platform}-{environment}"


infra = StackReference(f"KrisHerman/da_kh_example_dev/fetch_rewards")

#######################################################################################
# Service Accounts Config
#######################################################################################
sas_cfg = fetch_rewards.require_object("service_accounts")

sas_cfg = list(sas_cfg) if sas_cfg else []

#######################################################################################
# Storage Config
#######################################################################################
buckets = fetch_rewards.require_object("buckets") or []
for bucket in buckets:
    bucket["name"] = f"{stack_infra}-{bucket.get('location')}-bkt-{bucket.get('name')}"

#######################################################################################
# Storage Transfer Config
#######################################################################################
storage_transfer_cfg = fetch_rewards.require_object("storage_transfer_fetch_rewards")
storage_transfer_name = storage_transfer_cfg.get("name")
storage_transfer_src_bkt = storage_transfer_cfg.get("source_bucket_name")
storage_transfer_dst_bkt = storage_transfer_cfg.get("dest_bucket_name")
storage_transfer_notif = storage_transfer_cfg.get("topic_notif")

#######################################################################################
# Init
#######################################################################################
try:
    saInit = ServiceAccount(
        resource_name=stack_infra,
        project=stack_infra,
        service_accounts=sas_cfg,
        opts=ResourceOptions(
            provider=None,
            protect=False
        )
    )

    bucket = Bucket(
        resource_name=stack_infra,
        project=stack_infra,
        buckets=buckets,
        opts=ResourceOptions(
            depends_on=[]
        )
    )

    transferJob = TransferJob(
        resource_name=stack_infra,
        project=stack_infra,
        args=TransferJobArgs(
            description='Fetch Tst Transfer',
            status='ENABLED',
            schedule=storagetransfer.ScheduleArgs(
                schedule_start_date=storagetransfer.DateArgs(
                    year=datetime.now().year,
                    month=datetime.now().month,
                    day=datetime.now().day,
                ),
                schedule_end_date=storagetransfer.DateArgs(
                    year=(datetime.now() + timedelta(days=7300)).year,
                    month=datetime.now().month,
                    day=datetime.now().day,
                ),
                start_time_of_day=storagetransfer.TimeOfDayArgs(
                    hours=21,
                    minutes=10,
                    seconds=0,
                    nanos=0,
                )
            ),
            transfer_spec=storagetransfer.TransferSpecArgs(
                gcs_data_source=storagetransfer.GcsDataArgs(
                    bucket_name=storage_transfer_src_bkt,
                ),
                gcs_data_sink=storagetransfer.GcsDataArgs(
                    bucket_name=storage_transfer_dst_bkt,
                ),
                transfer_options=storagetransfer.TransferOptionsArgs(
                    delete_objects_from_source_after_transfer=True,
                    overwrite_objects_already_existing_in_sink=True,
                )
            )
            # notification_config=storagetransfer.NotificationConfigArgs(
            #     pubsub_topic=storage_transfer_notif,
            #     payload_format=storagetransfer.NotificationConfigPayloadFormat.JSON,
            #     event_types=[
            #         storagetransfer.NotificationConfigEventTypesItem.TRANSFER_OPERATION_ABORTED,
            #         storagetransfer.NotificationConfigEventTypesItem.TRANSFER_OPERATION_FAILED,
            #         storagetransfer.NotificationConfigEventTypesItem.TRANSFER_OPERATION_SUCCESS,
            #     ]
            # ),
        ),
        opts=ResourceOptions(
            depends_on=[
                bucket
            ]
        )
    )

    bqDataset = gcp.bigquery.Dataset("da-datalake-test-dev-bq-dataset",
        project=stack_infra,
        dataset_id="fetch_rewards_dataset",
        friendly_name="fetch_rewards_dataset",
        description="Dataset for Fetch Rewards",
        location="US"
    ) 

    table_fields = []
    schema_file = "schemas/output_table_schema.json"
    with open(schema_file, 'r') as fs_schema:
        SCHEMA = fs_schema.read().strip()
    for i in json.loads(SCHEMA):
        table_field=bigquery.TableFieldSchemaArgs(name=i['name'], type=i['type'], mode=i['mode'])
        table_fields.append(table_field)
    table_schema=bigquery.TableSchemaArgs(fields=table_fields)
    table_ref=bigquery.TableReferenceArgs(table_id="fetch_rewards_inbound_data")
    bqTable = bigquery.Table("da-datalake-test-dev-bq-table",
        project=stack_infra,
        dataset_id=bqDataset.dataset_id,
        friendly_name="fetch_rewards_inbound_data",
        schema =table_schema,
        table_reference =table_ref,    
        opts=ResourceOptions(depends_on=[bqDataset])
    )

    query_config = gcp.bigquery.DataTransferConfig("da-datalake-test-dev-bq-transfer",
    project=stack_infra,
    display_name="Fetch Rewards Transfer",
    location="US",
    data_source_id="google_cloud_storage",
    schedule="every day 21:20",
    destination_dataset_id=bqDataset.dataset_id,
    params={
        "data_path_template" : f"gs://{storage_transfer_dst_bkt}/*.csv",
        "destination_table_name_template": bqTable.friendly_name,
        "write_disposition": "APPEND",
        "file_format": "CSV",
        "skip_leading_rows": "1",
        "delete_source_files": "true",

    },
    opts=ResourceOptions(depends_on=[bqDataset])
    )

    export("fetch_rewards_sa", saInit)

except Exception as ex:
    log.error(f"Environment {environment} -> {ex}")
#######################################################################################