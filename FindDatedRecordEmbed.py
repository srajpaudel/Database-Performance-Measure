'''For each recording that was issued after 1950-01-01, find the recording name, songwriter name
and recording issue date in Embed db '''
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
db = client["A4dbEmbed"]

# create/open the collections
SongwritersRecordings = db["SongwritersRecordings"]


pipeline = [
    {"$unwind": "$recordings"},
    {"$match": {"recordings.issue_date": {"$gt": datetime.strptime("1950-01-01T00:00:00.000+00:00", "%Y-%m-%dT%H:%M:%S.%f%z")}}},

    
    {"$project": {
            "_id": "$_id",
            "name": "$name",
            "r_name": "$recordings.name",
            "issue_date": "$recordings.issue_date"
        }}    
]

results = list(SongwritersRecordings.aggregate(pipeline))
for result in results:
    print(result)

client.close()
