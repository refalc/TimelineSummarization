import pymongo
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import logging


class ElasticSearchBridge:
    def __init__(self, address, port, db_name='elastic'):
        self.__address = address
        self.__port = port
        self.__db_client = MongoClient()
        self.__db_name = db_name
        self.__db_collection_name = 'doc_content'
        self.__elastic_search = Elasticsearch([{'host': self.__address , 'port': self.__port}])
        if self.__db_name not in self.__db_client.database_names() or \
                self.__db_collection_name not in self.__db_client[self.__db_name].collection_names():
            self.__db_client[self.__db_name][self.__db_collection_name].create_index([
                ('doc_id', pymongo.ASCENDING)], unique=True)
        info_msg = 'Loaded collection with {} docs'.format(
            self.__db_client[self.__db_name][self.__db_collection_name].count())
        logging.getLogger('timeline_file_logger').info(info_msg)

        self.__db_parsed_collection_name = 'parsed_doc_content'
        if self.__db_name not in self.__db_client.database_names() or \
                self.__db_parsed_collection_name not in self.__db_client[self.__db_name].collection_names():
            self.__db_client[self.__db_name][self.__db_parsed_collection_name].create_index(
                [('doc_id', pymongo.ASCENDING)], unique=True)
        info_msg = 'Loaded parsed collection with {} docs'.format(
            self.__db_client[self.__db_name][self.__db_parsed_collection_name].count())
        logging.getLogger('timeline_file_logger').info(info_msg)

    def retrieve_docs(self, query, params):
        pass

    def retrieve_docs_coll(self, query, params):
        pass

    def retrieve_doc(self, doc_id):
        pass
