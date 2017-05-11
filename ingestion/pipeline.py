from os.path import join, dirname
import pandas as pd
import numpy as np
from pymongo import MongoClient
import re

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
    #for oldField, newField in fields.items():
    
    i = 0
    for index, row in df.iterrows():
        doc = {}
        for oldField, newField in fields.items():
            doc[newField] = row[oldField]
        print(doc['salary'])
        doc['salary'] = normalizeSalary(doc['salary'])
        print(doc['salary'])
        print()
        i += 1
        if i == 3:
            break    
    
    str = 'me at $63,000'
    print(str)
    salary = normalizeSalary(str)
    print(salary)
    #records.insert(doc)
    #for header in df:
        #print(header)
        #print(df[header])
        #break
    #print(df['Full-Time/Part-Time?'])

def normalizeSalary(data):
    regex = r'\d+\.?\d*'
    # remove commas for examples like "$60,000 salary"
    data = data.replace(',', '')
    values = re.findall(regex, data)
    # append 0 in case no salary is matched by regex
    values.append('0')
    # return max value (e.g. $24/hr + $4/hr for nights will return 24)
    return max([float(v) for v in values])

if __name__ == "__main__":
    main()
