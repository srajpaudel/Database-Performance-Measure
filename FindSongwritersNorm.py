'''Gets the average rhythmicality for all recordings that have a recording_id beginning
with “70” in the Normal db.'''

import sys
import argparse
import pymongo
from bson.json_util import loads

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

#calculate the average rhythmicality by matching id with 70 first
pipeline = [
    {"$match": {"recording_id": {"$regex": "^70"}}},
    {"$group": {"_id": "", "avgRhythmicality": { "$avg": "$rhythmicality" }}}
]
result = list(recordings.aggregate(pipeline))

# print out the result
for average in result:
    print(average)

# close the connection
client.close()
