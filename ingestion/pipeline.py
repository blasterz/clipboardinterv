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
    loadCityLocations()
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
    db.records.remove({})
    for index, row in df.iterrows():
        doc = {}
        for oldField, newField in fields.items():
            doc[newField] = row[oldField]
        doc['patientNurseRatio'] = normalizePatientNurseRatio(doc['patientNurseRatio'])
        doc['salary'] = normalizeSalary(doc['salary'])
        doc['location'] = getLocation(doc['location'])
        db.records.insert(doc)
    

def loadCityLocations():
    global locations
    locations = {}
    df = pd.read_csv(join(dirname(__file__), '../data/city_locations.csv'))
    for index, row in df.iterrows():
        city = row['city'].lower()
        doc = {
            'state' : row['state'].lower(),
            'lat' : row['latitude'],
            'lng' : row['longitude'],
        }
        if city not in locations:
            locations[city] = [doc]
        else:
            locations[city].append(doc)

def getLocation(data):
    global locations
    empty = {'lng' : 0, 'lat' : 0}
    result = empty
    data = data.lower().strip().replace('.','')
    names = data.split(',')
    city = names[0].lower().strip()
    state = ''
    # simple case 'San francisco, ca'
    if len(names) > 1:
        state = names[1].lower().strip()
        result = getCoords(city, state)
    else:
        names = names[0].split(' ')
        newCity = ''
        newState = ''
        if len(names) > 1:
            newState = names[-1]
            newCity = ' '.join(names[0:-1])
        else:
            newCity = names[0]
        # case 'San francisco ca'
        if result == empty:
            result = getCoords(newCity, newState)
        # case 'San Francisco'
        if result == empty:
            result = getCoords(data, '')
        # case 'San Francisco ojio'
        if result == empty:
            result = getCoords(newCity, '')
    return result

# try to get the coordinates with given city and state
def getCoords(city, state):
    global locations
    result = {'lng' : 0, 'lat' : 0}
    if city in locations:
        possibleLocations = locations[city]
        for location in possibleLocations:
            if matchStates(location['state'], state):
                result = location
                break
    result = {'lng' : result['lng'], 'lat' : result['lat'] }
    return result

# s1 is always a 2 letter like 'ca' while s2 can be 'ca' or 'california'
def matchStates(s1, s2):
    if s2 == '':
        return True
    s1 = s1.lower()
    s2 = s2.lower()
    if s1 == s2:
        return True
    if len(s2) > 2:
        if s2[0] == s1[0] and s2.find(s1[1]) != -1:
            return True
    return False


def normalizePatientNurseRatio(data):
    if not isinstance(data, str):
        return 0.0
    maxRatio = 0.0
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
        # no cases like '7-5'
        if vals[0] < vals[1]:
            ratio = float(sum(vals)) / float(len(vals))
            maxRatio = max(maxRatio, ratio)
    # other cases
    if maxRatio == 0.0:
        regex = r'\d+\.?\d*'
        values = re.findall(regex, data)
        values = [float(v) for v in values]
        # treat case when only one number is found, but avoid things like '7-May'
        if len(values) == 1 and len(re.findall(r'\d+\-\D*', data)) == 0:
            maxRatio = values[0]
        # treat cases like: 1 nurse to 5 patients
        elif len(values) > 1 and 1 in values:
            maxRatio = max(values)
    if maxRatio > 0 and maxRatio < 1:
        maxRatio = 1.0 / maxRatio
    return round(maxRatio, 1)

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
