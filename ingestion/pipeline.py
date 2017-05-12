from os.path import join, dirname
import pandas as pd
import numpy as np
from pymongo import MongoClient
import re
import math

"""

Use this file to read in the project nurse data, perform text pre-processing
and store data in mongo. The fields we're interested in storing are:

  'How many years of experience do you have?' -> experience,
  'What's your highest level of education?' -> education,
  'What is your hourly rate ($/hr)?' -> salary,
  'Department' -> department,
  'What (City, State) are you located in?' -> location,
  'What is the Nurse - Patient Ratio?' -> patientNurseRatio

Check server/models/Record.js for an example of the schema.

"""

def main():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['clipboardinterview']
    df = pd.read_csv(join(dirname(__file__), '../data/projectnurse.csv'))
    
    fields = {
        "How many years of experience do you have?" : 'experience',
        "What's your highest level of education?" : 'education',
        "What is your hourly rate ($/hr)?" : 'salary',
        "Department" : 'department',
        "What (City, State) are you located in?" : "location",
        "What is the Nurse - Patient Ratio?" : "patientNurseRatio"
    }

    records = db.records
    
    for index, row in df.iterrows():
        doc = {}
        for oldField, newField in fields.items():
            doc[newField] = row[oldField]
        print(doc['patientNurseRatio'])
        doc['patientNurseRatio'] = normalizePatientNurseRatio(doc['patientNurseRatio'])
        print(doc['patientNurseRatio'])
        print()
        doc['salary'] = normalizeSalary(doc['salary'])
        #records.insert(doc)

def normalizePatientNurseRatio(data):
    if not isinstance(data, str):
        return 0.0
    maxRatio = 0.0
    regex = r'\d+[:\-,\.]?\d*'
    # treat cases like 4:1
    regex = r'\d+:\d+'
    values = re.findall(regex, data)
    for s in values:
        vals = [float(v) for v in re.findall(r'\d+\.?\d*', s)]
        if min(vals) > 0:
            ratio = max(vals) / min(vals)
        else:
            ratio = 0
        maxRatio = max(maxRatio, ratio)
    # treat cases like 5-7
    regex = r'\d+\-\d+'
    values = re.findall(regex, data)
    for s in values:
        vals = [float(v) for v in re.findall(r'\d+\.?\d*', s)]
        ratio = float(sum(vals)) / float(len(vals))
        maxRatio = max(maxRatio, ratio)
    # treat cases like: 1 nurse to 5 patients
    if maxRatio == 0.0:
        regex = r'\d+\.?\d*'
        values = re.findall(regex, data)
        values = [float(v) for v in values]
        if len(values) == 1:
            maxRatio = values[0]
        elif len(values) > 1 and 1 in values:
            maxRatio = max(values)
    return maxRatio

def normalizeSalary(data, minSalary = 3, maxSalary = 200):
    monthsPerYear, daysPerMonth, daysPerWeek, hoursPerDay = 12, 21.6666, 5, 8
    regex = r'\d+\.?\d*'
    # remove commas for examples like "$60,000 salary"
    data = data.lower().replace(',', '')
    values = re.findall(regex, data)
    # append 0 in case no salary is matched by regex
    values.append('0')
    # return max value (e.g. $24/hr + $4/hr for nights will return 24)
    salary =  max([float(v) for v in values])
    divide = 1
    # daily salary
    if findOneKeyword(data, ['day', 'daily', 'diem']):
        divide = hoursPerDay
    # hourly salary
    elif salary < maxSalary:
        divide = 1
    # annual salary
    elif salary > 20000 or findOneKeyword(data,
            ['yr', 'year', 'yearly', 'annual', 'annually']):
        divide = monthsPerYear * daysPerMonth * hoursPerDay
    elif findOneKeyword(data, ['month', 'monthly']):
        divide = daysPerMonth * hoursPerDay
    # bi-weekly
    elif findOneKeyword(data, ['bi', 'other', '2', 'two', 'weeks']):
        divide = daysPerWeek * 2 *  hoursPerDay
    # weekly
    else:
        divide = daysPerWeek * hoursPerDay
    salary /= divide
    # hourly salary validation
    if salary < minSalary or salary > maxSalary:
        salary = 0
    return round(salary, 2)

def findOneKeyword(data, keywords):
    for keyword in keywords:
        if data.find(keyword) != -1:
            return True
    return False

if __name__ == "__main__":
    main()
