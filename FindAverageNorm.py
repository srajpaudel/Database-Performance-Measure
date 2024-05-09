#Finds the sum of the length of all recordings for each songwriter along with the songwriter’s id.

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


pipeline = [
    {"$unwind": "$songwriter_ids"},  

    {"$group": { 
        "_id": "$songwriter_ids",
        "total_length": {"$sum": "$length"},
    }},
  
]

results = list(recordings.aggregate(pipeline))
for result in results:
    print(result)

# close the connection
client.close()
