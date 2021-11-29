

import csv
import statistics
from file_consuption import read_file


SOURCE = "C:/Users/ssolvaag/PycharmProjects/azure_test_approach/output.csv"

# Field Names call_sign,CollisionMaintain,CrushMaintain,GroundingMaintain,FireMaintain

FIELDNAMES = ['call_sign', "FireMaintain", "GroundingMaintain", "CrushMaintain", "CollisionMaintain", "CapsizingMaintain", "OverboardMaintain"]

RISK_COLS = ["FireMaintain", "GroundingMaintain", "CrushMaintain", "CollisionMaintain", "CapsizingMaintain", "OverboardMaintain"]



def process_risk_sample(sample):

    goof = []

    for x in sample:

        try:
            goof.append(float(x))
        except:
            print(x)

    mean_value = statistics.mean(goof)

    stdev_value = statistics.stdev(goof)

    return {'mean_value': mean_value, 'stdev_value': stdev_value}


def process_results():

    data = read_file(SOURCE, fieldnames=FIELDNAMES)

    product = {}

    for risk in RISK_COLS:
        risk_sample = []

        for line in data:
            value = line[risk]
            risk_sample.append(value)

        risk_processed_values = process_risk_sample(risk_sample)

        product[risk] = risk_processed_values

    return product


if __name__ == '__main__':

    results = process_results()

    print(results)

    with open('scores.csv', 'w') as file:

        writer = csv.DictWriter(file, fieldnames=['RiskModel', 'Mean', 'StandardDeviation'])
        writer.writeheader()

        for model, values in results.items():

            packet = {'RiskModel': model, 'Mean': values['mean_value'], 'StandardDeviation': values['stdev_value']}
            writer.writerow(packet)

