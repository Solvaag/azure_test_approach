
import os
import csv


WORK_DIR = "C:/Repos/sdir_data"

example_file = "2021_11_10_15_30_LK3681_sigve_run.csv"



def collate_folder(dir):

    targets = []

    for element in os.listdir(dir):
        file = os.path.join(dir, element)
        if os.path.isfile(file):

            targets.append(file)

    return targets


# RiskModel,Score
# CollisionMaintain,3.00775593220339
# CrushMaintain,3.0263849723756904
# GroundingMaintain,3.0057259645464027
# FireMaintain,3.0000000000000004


def read_file(file_path):

    with open(file_path, 'r') as resource:

        reader = csv.DictReader(resource, fieldnames=["RiskModel", "Score"])
        next(reader, None)

        return [x for x in reader]


def consume_files(files):

    data = {}

    for file in files:
        file_data = read_file(file)
        print(file_data)
        call_sign = file.split("_")[6]

        data[call_sign] = file_data

    return data


def transpose_call_sign_scores(dir):

    files = collate_folder(dir)

    data = consume_files(files)

    fieldnames = ['call_sign', 'CollisionMaintain', 'CrushMaintain', 'GroundingMaintain', 'FireMaintain']

    with open('output.csv', 'w') as output:

        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()

        for call_sign, scores in data.items():

            row = {}
            row['call_sign'] = call_sign

            print(scores)

            for line in scores:
                row[line['RiskModel']] = line['Score']


            print(row)
            writer.writerow(row)


if __name__ == '__main__':

    transpose_call_sign_scores(WORK_DIR)




