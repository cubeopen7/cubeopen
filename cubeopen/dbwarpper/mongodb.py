# -*- coding-utf8 -*-

import pymongo

def get_mongo_client():
    return pymongo.MongoClient()

def get_mongo_db(dbname="cubeopen"):
    return get_mongo_client().get_database(dbname)