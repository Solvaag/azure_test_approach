

import csv

def get_really_fast_boats():
    
    with open('C:/Users/ssolvaag/PycharmProjects/azure_test_approach/res/kjenningskoder.csv', encoding='windows-1252') as resource:

        reader = csv.DictReader(resource)

        return [x for x in reader]
