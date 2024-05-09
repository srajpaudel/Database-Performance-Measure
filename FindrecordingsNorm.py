'''Finds the ids and names of each songwriter that has at least one recording and the number of
recordings by each such songwriter from the Normal Db'''

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

# find the ids and names of each songwriter that has at least one recording and the number of recordings by each such songwriter
pipeline = [
    {
        "$lookup": {
            "from": "songwriters",
            "localField": "id",
            "foreignField": "recording_id",
            "as": "songwriter"
        }
    },
    {
        "$match": {
        "recordings.0": {"$exists": True}
    }
    },
    {
        "$project": {
            "_id": 1,
            "songwriter_id":1,
            "name": 1,
            "num_recordings": {"$size": "$recordings"}
        }
    }
]

result = list(songwriters.aggregate(pipeline))
for songwriter in result:
    print(songwriter)

# close the connection
client.close()
