#!/usr/bin/env python3

##################################################
# Anton Rubisov                         20150119 #
# University of Toronto Sports Analytics Group   #
#                                                #
# Search through the Mongo database and return   #
# stats on the collection, query a particular    #
# athlete, etc.                                  #
##################################################

# For finding current largest ID in database:
# db.athleteRankings.find({},{"athlete_id":1}).sort({"athlete_id": -1}).limit(1)

# Drop a document
# db.athleteRankings.remove({"athlete_id" : {"$gt": 2563}})
# db.athleteRankings.remove({"athlete_id" : 2500})


# Required headers
import pymongo                               # MongoDB for Python3

host = 'localhost'
database = 'crossfitSoup'
collection = 'athleteId'


def mongo_connection():

    client = pymongo.MongoClient()
    db = client[database]
    col = db[collection]
    return col


def main():

    col = mongo_connection()
    sorted_id = col.find().sort("id", pymongo.DESCENDING)
    print('MongoDB {}.[{}]. {} documents.'.format(database, collection, col.count()))
    print('Max Athlete ID: {}'.format(sorted_id[0]['id']))
    print(col.find({"id": {"$gt": 97245}}).count())


if __name__ == "__main__":
    main()
