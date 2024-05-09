#Create the Normal database first
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
db = client["A4dbNorm"]

# create/open the collections 
songwriters = db["songwriters"]
recordings = db["recordings"]

# delete all previous entries in the collections
songwriters.delete_many({})
recordings.delete_many({})

# insert the data into the collections using the bson.json_util.load() to convert the json files to bson
songwriters.insert_many(loads(open("songwriters.json", 'r').read()))
recordings.insert_many(loads(open("recordings.json", 'r').read()))

# create the indexes
songwriters.create_index("id")
recordings.create_index("id")
recordings.create_index("songwriter_id")

# close the connection
client.close()
