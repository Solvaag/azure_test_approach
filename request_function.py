import requests

def poll_topbraid():
    url = "http://localhost:7071/api/poll_topbraid"

    data = {
        'sdir_name': 'horse',
        'sdir_token': 'hei, jeg er et bearer token',
        'sdir_items': '["https://www.sdir.no/SDIR_Simulator#REG201112221523S57P1","https://www.sdir.no/SDIR_Simulator#REG19870615507S2P1SP11","https://www.sdir.no/SDIR_Simulator#REG19870615507S14aP1","https://www.sdir.no/SDIR_Simulator#REG201112221523S92P2","https://www.sdir.no/SDIR_Simulator#REG19920915693S10P1SP3"]',
        'sdir_endpoint': 'http://localhost:7071/api/PuppetEndpoint',
        'sdir_path': "tests/rompe.json",
        'sdir_debug': "True",
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json',
    }

    body = '{"items":["https://www.sdir.no/SDIR_Simulator#REG201112221523S57P1","https://www.sdir.no/SDIR_Simulator#REG19870615507S2P1SP11","https://www.sdir.no/SDIR_Simulator#REG19870615507S14aP1","https://www.sdir.no/SDIR_Simulator#REG201112221523S92P2","https://www.sdir.no/SDIR_Simulator#REG19920915693S10P1SP3"]}'

    response = requests.post(url, headers=data, data=body)

    print(response.status_code)
    print(response.content)

def poll_puppet():

    url = "http://localhost:7071/api/PuppetEndpoint/"

    response = requests.get(url)

    print(response.status_code)
    print(response.content)

if __name__ == '__main__':
    poll_topbraid()