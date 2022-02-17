# Show Azure subscription information

import os, json
from azure.identity import ClientSecretCredential


def developer_validation(file='C:/Users/ssolvaag/PycharmProjects/azure_test_approach/creds.json'):
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


def get_credentials():

    subscription_id, tenant_id = developer_validation()
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    credential = ClientSecretCredential(tenant=tenant_id, client_id=client_id, secret=client_secret)

    return credential