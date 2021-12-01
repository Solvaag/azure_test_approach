import csv

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.datafactory.models import DatasetResource, AzureBlobDataset, \
    DatasetReference, BlobSink, BlobSource, PipelineResource, CopyActivity, \
    LinkedServiceResource, AzureStorageLinkedService, LinkedServiceReference

import os

import time
from datetime import datetime

from configs.sdir_config import RESOURCE_GROUP, DATA_FACTORY

# from azure.common.credentials import ServicePrincipalCredentials  # To login with service principal (appid and client secret) use this
from azure.identity import InteractiveBrowserCredential

import subprocess
import json

from reports.boatlistings import get_really_fast_boats

from testbed import developer_validation

def piperun_by_callsign(parameters, credentials, subscription_id, pipeline_name=None):
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    run_response = adf_client.pipelines.create_run(RESOURCE_GROUP, DATA_FACTORY, pipeline_name, parameters=parameters)

    print("Beginning on set: \n {}".format(parameters))

    track_runs(adf_client, run_response)

    return run_response


def track_runs(adf_client, run_response):
    now = datetime.now()
    in_progress = True

    while in_progress:
        time.sleep(30)
        runtime = datetime.now()
        duration = runtime - now
        print("\tDuration {} seconds".format(duration.seconds))

        pipeline_run = adf_client.pipeline_runs.get(RESOURCE_GROUP, DATA_FACTORY, run_response.run_id)

        print("\tPipeline run status: {}".format(pipeline_run.status))

        if pipeline_run.status != "InProgress":
            in_progress = False


def main():
    fast_boats = get_really_fast_boats()

    print(fast_boats)

    boat_count = 60 # 50 - 60

    # callSign;company;vesselType

    pipeline = "risk_module"

    subscription_id, tenant_id = developer_validation()

    credentials = InteractiveBrowserCredential(tenant_id=tenant_id)

    run_responses = {}

    with open('outputs/log.txt', 'a+') as log:
        writer = csv.DictWriter(log, fieldnames=['call_sign', 'index', 'start', 'end', 'run_id'])
        writer.writeheader()



        for index, boat in enumerate(fast_boats[:boat_count]):
            print()
            call_sign = boat['callSign']
            company = boat['company']
            vessel_type = boat['vesselType']
            run_id = '{}_{}_sigve_run'.format(datetime.now().strftime("%Y_%m_%d_%H_%M"), call_sign)

            parameters = {
                'call_sign': call_sign,
                'company': company,
                'vessel_type': vessel_type,
                'model_types': ["FireMaintain", "GroundingMaintain", "CrushMaintain", "CollisionMaintain", "CapsizingMaintain", "OverboardMaintain"],
                'run_id': run_id,
                'timespan_months': 36
            }


            start = datetime.now()

            pipeline_run = piperun_by_callsign(parameters, credentials, subscription_id, pipeline)

            run_responses[call_sign] = {'run_response': pipeline_run, 'run_id': run_id}

            end = datetime.now()

            row_packet = {
                'call_sign': call_sign,
                'index': index,
                'start': start,
                'end': end,
                'run_id': run_id
            }

            writer.writerow(row_packet)



if __name__ == '__main__':
    main()
