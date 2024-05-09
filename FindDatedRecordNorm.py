
'''For each recording that was issued after 1950-01-01, find the recording name, songwriter name
and recording issue date in Normal db '''

import sys
import argparse
import pymongo
from bson.json_util import loads
from datetime import datetime

# parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("port", help="the port of the mongo server")
args = parser.parse_args()

# connect to the database
client = pymongo.MongoClient("mongodb://localhost:{}/".format(args.port))
db = client["A4dbNorm"]

# create/open the collections
songwriters = db["songwriters"]
recordings = db["recordings"]

pipeline = [
    {"$match": {"issue_date": {"$gt": datetime.strptime("1950-01-01T00:00:00.000+00:00", "%Y-%m-%dT%H:%M:%S.%f%z")}}},
   
    {"$unwind": "$songwriter_ids"},

    {"$lookup": {
            "from": "songwriters",
            "localField": "songwriter_ids",
            "foreignField": "songwriter_id",
            "as": "songs"
        }},
    {"$unwind": "$songs"},
    {"$project": {
            "_id": "$songs._id",
            "name": "$songs.name",
            "r_name": "$name",
            "r_issue_date": "$issue_date"
        }}    
]

results = list(recordings.aggregate(pipeline))
for result in results:
    print(result)

client.close()
