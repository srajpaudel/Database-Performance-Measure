'''Finds the ids and names of each songwriter that has at least one recording and the number of
recordings by each such songwriter from the Embed Db'''

import sys
import argparse
import pymongo
import os
from bson.json_util import loads


# parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("port", help="the port of the mongo server")
args = parser.parse_args()

# connect to the database
client = pymongo.MongoClient("mongodb://localhost:{}/".format(args.port))

db = client["A4dbEmbed"]

# create/open the collections 
songwriters_recordings = db["SongwritersRecordings"]

# find the ids and names of each songwriter that has at least one recording and the number of recordings by each such songwriter
pipeline = [
    {"$unwind": "$recordings"},
    {"$group": {
        "_id": {
            "songwriter_id": "$songwriter_id",
            "songwriter_name": "$name",
            "objectId" : "$_id"
        },
        "num_recordings": {"$sum": 1}
    }},
    {"$match": {
        "num_recordings": {"$gte": 1}
        }
     },
    {"$project": {
        "_id.objectId":1,
        "_id.songwriter_id": 1,
        "_id.songwriter_name": 1,
        "num_recordings": 1
        }
    },
    {"$sort": {
        "_id": 1 
        }
    }

]

result = list(songwriters_recordings.aggregate(pipeline))

for songwriter in result:
    print(songwriter)
client.close()
