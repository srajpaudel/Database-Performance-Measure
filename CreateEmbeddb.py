#Create the Embed database first
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
songwriters = db["songwriters"]
recordings = db["recordings"]
songwriters_recordings = db["SongwritersRecordings"]

# delete all previous entries in the collections
songwriters.delete_many({})
recordings.delete_many({})
songwriters_recordings.delete_many({})

# insert the data into the collections using the bson.json_util.load() to convert the json files to bson
songwriters.insert_many(loads(open("songwriters.json", 'r').read()))
recordings.insert_many(loads(open("recordings.json", 'r').read()))

# create the indexes
songwriters.create_index("id")
recordings.create_index("id")
recordings.create_index("songwriter_id")
songwriters_recordings.create_index("id")

# create the embedded collection
for songwriter in songwriters.find():
    # get the recordings of the songwriter
    recordings_of_songwriter = recordings.find({"songwriter_ids": songwriter.get("songwriter_id")})
    # create a new document with the songwriter and the recordings
    new_document = songwriter
    new_document["recordings"] = list(recordings_of_songwriter)
    # insert the new document into the collection
    songwriters_recordings.insert_one(new_document)

#drop the extra collections
songwriters.drop()
recordings.drop()

# close the connection
client.close()
