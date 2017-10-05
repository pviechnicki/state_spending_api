#Connects to remote db via odbc and dsn
#http://codehandbook.org/working-with-json-in-python-flask/

from flask import Flask
from flask import request
import sys
from simplejson import dumps
from stateSpending import StateSpending
from utils import init_odbc
from settings import settings
from fields import stateSpendingFields, wioaAllotmentFields #List of fields

#------------------------------------------------------------------------#
# Put HTML tags around a json string so it displays properly             #
#------------------------------------------------------------------------#
def wrapWithHTML(myJsonStr):
    return("<html><head><meta type=\"application/json\"</head><body>{\"data\": [[" + myJsonStr + "]]}<//body></html>")

#-------------------------------------------------------------------------#
# From list of states, formulate a sql where clause
#-------------------------------------------------------------------------#
def getWhereClause(myStateList):
    #Handle state requests or use all for default
    if (myStateList == None):
        return('')
    else:
        return(" where state_abbrev in ('" + "','".join(myStateList.split(",")) + "')")

app = Flask(__name__)

#-------------------------------------------------------------------------#
# Define getWIOAAllotments endpoint
#-------------------------------------------------------------------------#
@app.route("/getWIOAAllotments")

def getWIOAAllotments():
    global conn #db connection
    global wioaAllotmentFields #List of fields for query
    cur2 = conn.cursor()

    #Get state and column parameters from http request
    myStateList = request.args.get('state')
    myVarList = request.args.get('var')

    #Buffer to hold output
    jsonStr = []

    #Format where clause (sql) from ?state param
    whereClause = getWhereClause(myStateList)

    #Handle var/column requests or use whole list of fields for default
    if (myVarList == None):
        queryVarList = wioaAllotmentFields
    else:
        queryVarList = myVarList.split(",")
        #Always add state_name, state_abbrev, state_fips, year to output
        queryVarList.insert(0, "year")
        queryVarList.insert(0, "state_fips")
        queryVarList.insert(0, "state_abbrev")
        queryVarList.insert(0, "state_name")
        
    #Compose the final query from the fields list and where clause
    try:
        query = "select " + ",".join(queryVarList) + " from wioa_yearly_allotments" + whereClause + ";"
        sys.stderr.write(query)
        f = open("query.sql", 'w+')
        f.write(query)
        f.close()
        
        cur2.execute(query)

        #Write record set out to list of dicts
        for record in cur2.fetchall():
            newState = {}
            for i in range(0,len(queryVarList)):
                newState[queryVarList[i]] = record[i]
            #Append each dict to the output buffer in json format
            jsonStr.append(dumps(newState))

    except:
        print("getWIOAAllotments() error: {}\n".format(sys.exc_info()[0]))

    return wrapWithHTML(",".join(jsonStr))

#-------------------------------------------------------------------------#
# Define getStateSpending endpoint
#-------------------------------------------------------------------------#
@app.route("/getStateSpending")

#Make sure to process the args from the request
def getStateSpending():
    global conn
    global stateSpendingFields #List of fields for query, output

    cur = conn.cursor()

    #Check if args passed to http request
    myStateList = request.args.get('state')
    myVarList = request.args.get('var')

    #Buffer to hold json output
    jsonStr = []

    #Format sql where clause from ?state param
    whereClause = getWhereClause(myStateList)
        
    #Handle var/column requests or use whole list of fields for default
    if (myVarList == None):
        queryVarList = stateSpendingFields
    else:
        queryVarList = myVarList.split(",")
        #Always add state_name, state_abbrev, state_fips to output
        queryVarList.insert(0, "year")
        queryVarList.insert(0, "state_fips")
        queryVarList.insert(0, "state_abbrev")
        queryVarList.insert(0, "state_name")
    #Compose the final query from the fields list and where clause
    try:
        query = "select " + ",".join(queryVarList) + " from state_yearly_spending" + whereClause + ";"
        #sys.stderr.write(query)
        cur.execute(query)

        #Write record set out to list of dicts
        for record in cur.fetchall():
            newState = {}
            for i in range(0,len(queryVarList)):
                newState[queryVarList[i]] = record[i]
            #Append each dict to the output buffer in json format
            jsonStr.append(dumps(newState))

    except:
        print("getStateSpending() error: {}\n".format(sys.exc_info()[0]))
    return wrapWithHTML(",".join(jsonStr))

#-------------------------------------------------------------------------#
# Main functionality
#-------------------------------------------------------------------------#
if __name__ == "__main__":
    try:
        conn = init_odbc("WIASRD")
    except:
        sys.stderr.write("Couldn't connect to db")

    app.run(port=5000, debug=True)
    
