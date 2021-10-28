from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient

# from azure.common.credentials import ServicePrincipalCredentials  # To login with service principal (appid and client secret) use this
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

    rg_name = "sdir-d-rg-risk"
    df_name = "sdir-d-adf-risk"

    p_name = "prepare_data"

    # foo = adf_client.pipelines.create_run(rg_name, df_name, p_name)

    foo = get_resources_in_group(rg_name, credentials, subscription_id)

    print(foo)
    for thing in foo:
        print(thing)


if __name__ == '__main__':
    trigger_run()
