import pymongo
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import logging
from .ts_primitives import TSDocument


class ElasticSearchBridge:
    def __init__(self, address, port, db_name='elastic', index_name='news_1', doc_type='_doc'):
        self.__address = address
        self.__port = port
        self.__index_name = index_name
        self.__doc_type = doc_type
        self.__db_client = MongoClient()
        self.__db_name = db_name
        self.__elastic_search = Elasticsearch([{'host': self.__address , 'port': self.__port}])

        self.__db_parsed_collection_name = 'parsed_doc_content'
        if self.__db_name not in self.__db_client.database_names() or \
                self.__db_parsed_collection_name not in self.__db_client[self.__db_name].collection_names():
            self.__db_client[self.__db_name][self.__db_parsed_collection_name].create_index(
                [('doc_id', pymongo.ASCENDING)], unique=True)
        info_msg = 'Loaded parsed collection with {} docs'.format(
            self.__db_client[self.__db_name][self.__db_parsed_collection_name].count())
        logging.getLogger('timeline_file_logger').info(info_msg)

        self.__hashed_docs = dict()

    def retrieve_docs(self, query, params):
        pass

    def retrieve_docs_coll(self, query, params):
        pass

    def retrieve_doc(self, doc_id):

        if doc_id in self.__hashed_docs:
            return self.__hashed_docs[doc_id]

        db_answer = self.__db_client[self.__db_name][self.__db_parsed_collection_name].find_one({'doc_id': doc_id})

        if db_answer is not None:
            document = TSDocument(doc_id)
            document.from_saved(db_answer['data'])
            self.__hashed_docs[doc_id] = document
            return document

        doc_content = self.__elastic_search.get(index=self.__index_name, doc_type=self.__doc_type, id=doc_id)
        if len(doc_content) == 0 or doc_content['found'] is not True:
            return None

        document = self.__create_doc(doc_id, doc_content)
        serr_doc_str = document.to_saved()

        magic_big_number = 16793598
        if len(serr_doc_str) > magic_big_number:
            return None

        self.__db_client[self.__db_name][self.__db_parsed_collection_name].insert_one(
            {'doc_id': doc_id, 'data': serr_doc_str})
        self.__hashed_docs[doc_id] = document
        return document

    def __create_doc(self, doc_id, doc_content):
        document = TSDocument(doc_id)
        self.__process_meta_data(document, doc_content)
        self.__process_index_data(document, doc_content)
        return document

    def __process_meta_data(self, document, doc_content):
        meta_dict = {'DATE': 'KRMN_DATE', 'SITE': 'KRMN_SITE', 'TITLE': 'KRMN_TITLE'}
        for meta_name, meta_pattern in meta_dict.items():
            meta_data = doc_content['_source'][meta_pattern]
            document.add_meta_data(meta_name, meta_data)

    def __process_index_data(self, document, doc_content):
        pass


