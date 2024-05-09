#Finds the sum of the length of all recordings for each songwriter along with the songwriter’s id in the Embed db.

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
db = client["A4dbEmbed"]

# create/open the collections 
songwriters_recordings = db["SongwritersRecordings"]


pipeline = [
    {"$group": { 
        "_id": "$songwriter_id",
        "total_length": {"$sum":{"$sum" : "$recordings.length"}},
    }},
    {"$match": {"total_length": {"$gt": 0}}},
    {"$sort": {"total_length": -1}}
]

results = list(songwriters_recordings.aggregate(pipeline))
for result in results:
    print(result)
    
    

# close the connection
client.close()
