

import csv

def get_really_fast_boats():
    
    with open('/res/kjenningskoder.csv', encoding='windows-1252') as resource:

        reader = csv.DictReader(resource)

        return [x for x in reader]
