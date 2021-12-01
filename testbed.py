from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.datafactory.models import DatasetResource, AzureBlobDataset, \
    DatasetReference, BlobSink, BlobSource, PipelineResource, CopyActivity, \
    LinkedServiceResource, AzureStorageLinkedService, LinkedServiceReference

import os

from configs.sdir_config import RESOURCE_GROUP, DATA_FACTORY

from azure.identity import InteractiveBrowserCredential

import subprocess
import json



def developer_validation(file='creds.json'):
    """
    Creds.json was made by copy-pasting the subscription list from Azure CLI into a json file.

    Use the get_subscriptions() function to get a complete list of authorized subs.

    This function should only be used for development.

    :param file:
    :return: str subscription_id, str tenant_id
    """

    with open(file, 'r') as creds:
        data = json.load(creds)

    return data["id"], data["tenantId"]


def load_configuration(file='parameters.json'):
    with open(file, 'r') as parameter_file:
        parameters = json.load(parameter_file)

    return parameters


def get_subscriptions():
    subscriptions = json.loads(subprocess.check_output('az account list', shell=True).decode('utf-8'))

    return subscriptions


def get_specific_subscription(sub_id, subscriptions):
    for subscription in subscriptions:
        if sub_id == subscription['id']:
            return subscription


def get_resources_in_group(resource_group, credentials, subscription_id):
    res_client = ResourceManagementClient(credentials, subscription_id)

    resources = res_client.resources.list_by_resource_group(resource_group)

    return resources


def trigger_run():
    subscription_id, tenant_id = developer_validation()

    # subs = get_subscriptions()

    # subscription = get_specific_subscription(subscription_id, subs)

    # credentials = ServicePrincipalCredentials(client_id='appid',
    #                                           client_secret='client secret',
    #                                           tenant_id='tenantid')  # To login with serv ppal

    credentials = InteractiveBrowserCredential(tenant_id=tenant_id)

    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    doof = {
        # 'indicators': '["callsToPortPerDistanceTraveled","portCalls","avgVelocity"]',
        'call_sign': 'JXNC',
    }

    # foo = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters=doof)

    # https://sdirdstdataadfrisk00.blob.core.windows.net/sandbox/source/help_i_am_a_textfile.txt

    connstring = ""

    ls_name = 'storageLinkedService001'
    ls_azure_storage = LinkedServiceResource(properties=AzureStorageLinkedService(connection_string=connstring))
    ls = adf_client.linked_services.create_or_update(RESOURCE_GROUP, DATA_FACTORY, ls_name, ls_azure_storage)
    ds_ls = LinkedServiceReference(reference_name=ls_name)

    source_set = create_dataset('sandbox', 'source', 'help_i_am_a_textfile.txt', ds_ls)
    source_name = "foo"

    source_output = adf_client.datasets.create_or_update(RESOURCE_GROUP, DATA_FACTORY, source_name, source_set)

    print(source_output)

    sink_set = create_dataset('sandbox', 'sink', 'help_i_am_a_textfile.txt', ds_ls)
    sink_name = "bar"

    sink_output = adf_client.datasets.create_or_update(RESOURCE_GROUP, DATA_FACTORY, sink_name, sink_set)

    print(sink_output)

    p_name = "prepare_data"

    factory_output = adf_client.pipelines.create_or_update(RESOURCE_GROUP, DATA_FACTORY, pipename, pipeobj)

    print(factory_output)

    run_response = adf_client.pipelines.create_run(RESOURCE_GROUP, DATA_FACTORY, pipename, parameters={})

    print(run_response)

    # foo = get_resources_in_group(rg_name, credentials, subscription_id)
    #
    # res_client = ResourceManagementClient(credentials, subscription_id)
    #
    #
    #
    # print(foo)
    # for thing in foo:
    #     print(thing)


def create_dataset(container, folder_path, filename, linked_service=None):
    blob_path = os.path.join(container, folder_path)
    blob_filename = filename

    ds_azure_blob = DatasetResource(properties=AzureBlobDataset(
        linked_service_name=linked_service, folder_path=blob_path, file_name=blob_filename))

    return ds_azure_blob  # must still be updated in client


def copy_results(source_set_name, sink_set_name):
    # Create a copy activity
    act_name = 'copyResults'
    blob_source = BlobSource()
    blob_sink = BlobSink()
    dsin_ref = DatasetReference(reference_name=source_set_name)
    dsOut_ref = DatasetReference(reference_name=sink_set_name)
    copy_activity = CopyActivity(name=act_name, inputs=[dsin_ref], outputs=[
        dsOut_ref], source=blob_source, sink=blob_sink)

    # Create a pipeline with the copy activity
    pipeline_name = 'copyPipeline'
    params_for_pipeline = {}
    pipeline_object = PipelineResource(
        activities=[copy_activity], parameters=params_for_pipeline)

    return pipeline_name, pipeline_object


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


