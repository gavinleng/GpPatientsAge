__author__ = 'G'

import sys
import urllib
import pandas as pd
import argparse
import json
import datetime
import hashlib

# url = "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014.csv"
# output_path = "tempGpPatientsAge.csv"
# required_indicators = ["2014", "01", "MALE_0-4", "MALE_5-9", "MALE_10-14", "FEMALE_0-4", "FEMALE_5-9", "FEMALE_10-14"] or
# required_indicators = ["2014", "01", "all"]


def download(url, reqFields, outPath):
    reqReq = [x.upper() for x in reqFields]
    dName = outPath

    iYear = reqReq[0]
    iMonth = reqReq[1]

    if reqReq[2] != 'ALL':
        reqs = reqReq[2:]

    col = ['GP_PRACTICE_CODE', 'POSTCODE', 'CCG_CODE', 'NHSE_AREA_TEAM_CODE', 'NHSE_REGION_CODE', 'Sex', 'Age', 'Value', 'Year',  'Month', 'pkey']

    # open url
    socket = openurl(url)

    raw_data = {}
    for j in col:
        raw_data[j] = []

    # operate this csv file
    logfile.write(str(now()) + ' csv file loading\n')
    print('csv file loading------')
    df = pd.read_csv(socket, dtype='unicode')
    cList = df.columns.tolist()

    if reqReq[2] == 'ALL':
        reqs = cList[8:]

    # data reading
    logfile.write(str(now()) + ' data reading\n')
    print('data reading------')

    list0 = df.loc[:, col[0]].tolist()
    list1 = df.loc[:, col[1]].tolist()
    list2 = df.loc[:, col[2]].tolist()
    list3 = df.loc[:, col[3]].tolist()
    list4 = df.loc[:, col[4]].tolist()

    for req in reqs:
        if req not in cList:
            errfile.write(str(now()) + " Requested data " + str(req) + " don't match the csv file. Please check the file at: " + str(url) + " . End progress\n")
            logfile.write(str(now()) + ' error and end progress\n')
            sys.exit("Requested data " + str(req) + " don't match the excel file. Please check the file at: " + url)

        valueList = df.loc[:, req].tolist()

        raw_data[col[0]] = raw_data[col[0]] + list0
        raw_data[col[1]] = raw_data[col[1]] + list1
        raw_data[col[2]] = raw_data[col[2]] + list2
        raw_data[col[3]] = raw_data[col[3]] + list3
        raw_data[col[4]] = raw_data[col[4]] + list4
        raw_data[col[5]] = raw_data[col[5]] + [req.split('_')[0]] * len(valueList)
        raw_data[col[6]] = raw_data[col[6]] + [req.split('_')[1]] * len(valueList)
        raw_data[col[7]] = raw_data[col[7]] + valueList

    raw_data[col[8]] = [iYear] * len(raw_data[col[0]])
    raw_data[col[9]] = [iMonth] * len(raw_data[col[0]])
    logfile.write(str(now()) + ' data reading end\n')
    print('data reading end------')

    # create primary key by md5 for each row
    logfile.write(str(now()) + ' create primary key\n')
    print('create primary key------')
    keyCol = [0, 2, 3, 4, 5, 6, 8, 9]
    raw_data[col[-1]] = fpkey(raw_data, col, keyCol)
    logfile.write(str(now()) + ' create primary key end\n')
    print('create primary key end------')

    # save csv file
    logfile.write(str(now()) + ' writing to file\n')
    print('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns=col)
    dfw = dfw.sort([col[0], col[5], col[6]], ascending=[1, 1, 1])#.dropna()
    dfw.to_csv(dName, index=False)
    logfile.write(str(now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now()) + ' finished\n')
    print("finished")

def openurl(url):
    try:
        socket = urllib.request.urlopen(url)
        return socket
    except urllib.error.HTTPError as e:
        errfile.write(str(now()) + ' file download HTTPError is ' + str(e.code) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        errfile.write(str(now()) + ' file download URLError is ' + str(e.args) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download URLError = ' + str(e.args))
    except Exception:
        print('file download error')
        import traceback
        errfile.write(str(now()) + ' generic exception: ' + str(traceback.format_exc()) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('generic exception: ' + traceback.format_exc())

def fpkey(data, col, keyCol):
    mystring = ''
    pkey = []
    for i in range(len(data[col[0]])):
        for j in keyCol:
            mystring += str(data[col[j]][i])
        mymd5 = hashlib.md5(mystring.encode()).hexdigest()
        pkey.append(mymd5)

    return pkey

def now():
    return datetime.datetime.now()


parser = argparse.ArgumentParser(
    description='Extract online GP Patients by GP and Age csv file to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempGpPatientsAge.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "url": "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014.csv",
        "outPath": "tempGpPatientsAge.csv",
        #"reqFields": ["2014", "01", "MALE_0-4", "MALE_5-9", "MALE_10-14", "FEMALE_0-4", "FEMALE_5-9", "FEMALE_10-14"]
        "reqFields": ["2014", "01", "all"] #"all" means all "male" and "female" fields
    }

    logfile = open("log_tempGpPatientsAge.log", "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open("err_tempGpPatientsAge.err", "w")

    with open("config_tempGpPatientsAge.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempGpPatientsAge.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["reqFields"], oConfig["outPath"])
