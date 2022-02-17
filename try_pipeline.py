from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.datafactory.models import DatasetResource, AzureBlobDataset, \
    DatasetReference, BlobSink, BlobSource, PipelineResource, CopyActivity, \
    LinkedServiceResource, AzureStorageLinkedService, LinkedServiceReference

from azure.identity import InteractiveBrowserCredential
from testbed import developer_validation

from configs.sdir_config import RESOURCE_GROUP, DATA_FACTORY

import time

def set_up_adf_client():
    subscription_id, tenant_id = developer_validation()

    credentials = InteractiveBrowserCredential(tenant_id=tenant_id)

    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    return adf_client


def set_up_temp_pipeline(activities, parameters, test_label, adf_client):
    pipeline_object = PipelineResource(
        activities=activities, parameters=parameters)

    factory_output = adf_client.pipelines.create_or_update(RESOURCE_GROUP, DATA_FACTORY, test_label, pipeline_object)

    return {'object': pipeline_object, 'factory_output': factory_output, 'test_label': test_label}


def test_pipeline(pipeline, parameters):

    adf_client = set_up_adf_client()

    run_response = adf_client.pipelines.create_run(RESOURCE_GROUP, DATA_FACTORY, pipeline, parameters=parameters)

    result = track_run(adf_client, run_response)

    # handle assertion logic here

    return result



def track_run(adf_client, run_response):
    in_progress = True
    pipeline_run = None

    while in_progress:
        time.sleep(30)

        pipeline_run = adf_client.pipeline_runs.get(RESOURCE_GROUP, DATA_FACTORY, run_response.run_id)

        if pipeline_run.status != "InProgress":
            in_progress = False

    return pipeline_run.status


if __name__ == '__main__':

    # LK3681,155188,Hurtigbåt

    pipeline = "risk_module"

    parameters = {
        'call_sign': 'LK3681',
        'company': '155188',
        'vessel_type': 'Hurtigbåt',
        'model_types': ["FireMaintain", "GroundingMaintain", "CrushMaintain", "CollisionMaintain",
                        "CapsizingMaintain", "OverboardMaintain"],
        'run_id': "sigve_test_01",
        'timespan_months': 36
    }

    pack = test_pipeline(pipeline, parameters)

    print(pack)
