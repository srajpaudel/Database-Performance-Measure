'''Gets the average rhythmicality for all recordings that have a recording_id beginning
with “70” in the Embed db.'''

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
    {"$unwind": "$recordings"},
    {"$project": {"recording_id": "$recordings.recording_id", "rhythmicality": "$recordings.rhythmicality"}},
    {"$match": {"recording_id": {"$regex": "^70"}}},
    {"$group": {"_id": "", "avg_Rhythmicality": {"$avg": "$rhythmicality"}}}
]
final_result = list(songwriters_recordings.aggregate(pipeline))
for average in final_result:
    print(average)

# close the connection
client.close()
