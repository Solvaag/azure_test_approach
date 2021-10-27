from azure.mgmt.datafactory import DataFactoryManagementClient

# from azure.common.credentials import ServicePrincipalCredentials  # To login with service principal (appid and client secret) use this
from azure.identity import InteractiveBrowserCredential

import subprocess
import json


def get_subscriptions():
    subscriptions = json.loads(subprocess.check_output('az account list', shell=True).decode('utf-8'))

    return subscriptions


def get_specific_subscription(sub_id, subscriptions):
    for subscription in subscriptions:
        if sub_id == subscription['id']:
            return subscription


def trigger_run():
    subscription_id = ""

    subs = get_subscriptions()

    subscription = get_specific_subscription(subscription_id, subs)

    # credentials = ServicePrincipalCredentials(client_id='appid',
    #                                           client_secret='client secret',
    #                                           tenant_id='tenantid')  # To login with serv ppal
    credentials = InteractiveBrowserCredential(tenant_id=subscription['homeTenantId'])
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    rg_name = "sdir-d-rg-risk"
    df_name = "sdir-d-adf-risk"

    p_name = "prepare_data"
    params = {
        "Param1": "value1",
        "Param2": "value2"
    }

    adf_client.pipelines.create_run(rg_name, df_name, p_name)


if __name__ == '__main__':
    trigger_run()
