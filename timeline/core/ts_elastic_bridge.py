import pymongo
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import logging
from .ts_primitives import TSDocument, TSIndexItem, TSCollection
from ..utils.utils import SimpleTimer
import re
import math
import nltk


class ElasticSearchBridge:
    def __init__(self, address, port, index_name='news_1', doc_type='_doc', using_mongo=True, db_name='elastic'):
        self.__address = address
        self.__port = port
        self.__index_name = index_name
        self.__doc_type = doc_type
        self.__elastic_search = Elasticsearch([{'host': self.__address, 'port': self.__port}])

        self.__using_mongo = using_mongo
        self.__db_name = db_name
        self.__db_parsed_collection_name = 'parsed_doc_content'
        if self.__using_mongo:
            self.__db_client = MongoClient()
            if self.__db_name not in self.__db_client.database_names() or \
                    self.__db_parsed_collection_name not in self.__db_client[self.__db_name].collection_names():
                self.__db_client[self.__db_name][self.__db_parsed_collection_name].create_index(
                    [('doc_id', pymongo.ASCENDING)], unique=True)
            info_msg = 'Loaded parsed collection with {} docs'.format(
                self.__db_client[self.__db_name][self.__db_parsed_collection_name].count())
            logging.getLogger('timeline_file_logger').info(info_msg)

        self.__hashed_docs = dict()
        self.__stop_words = nltk.corpus.stopwords.words('russian')
        shards = self.__elastic_search.count(index=self.__index_name, doc_type=self.__doc_type)['_shards']['total']
        all_doc_count = self.__elastic_search.count(index=self.__index_name)['count']
        self.__mean_doc_in_shard = all_doc_count / shards

        try:
            self.m_Lock = __nldx_lock
        except Exception as e:
            self.m_Lock = None


    def retrieve_docs(self, query, params):
        query_list = []
        for index in query.index_iter():
            for item in index.get_sorted_index_data_list():
                query_list.append(item.m_Name)

        query_body = {
            "query": {
                "match": {
                    "lemmas": ' '.join(query_list)
                }
            }
        }

        info_msg = 'ir request={}'.format(' '.join(query_list))
        logging.getLogger('timeline_file_logger').info(info_msg)
        search_results = self.__elastic_search.search(index=self.__index_name, doc_type=self.__doc_type,
                                                      body=query_body, size=params['doccnt'], _source=False,
                                                      request_timeout=30)

        if len(search_results) == 0:
            info_msg = 'Zero answer from elastic query'
            logging.getLogger('timeline_file_logger').error(info_msg)
            return []

        docs = [(val_dict['_id'], val_dict['_score']) for val_dict in search_results['hits']['hits']]
        if len(docs) == 0:
            info_msg = 'Zero docs from elastic query={}'.format(search_results)
            logging.getLogger('timeline_file_logger').error(info_msg)

        return docs

    def retrieve_docs_coll(self, query, params):
        timer = SimpleTimer('NldxSearchEngineBridge.retrieve_docs_coll')

        collection = TSCollection()
        recieved_docs = self.retrieve_docs(query, params)
        info_msg = 'Retrieved {} doc ids'.format(len(recieved_docs))
        logging.getLogger('timeline_file_logger').info(info_msg)
        for doc_data in recieved_docs:
            doc_id = doc_data[0]
            document = self.retrieve_doc(doc_id)
            if document is not None:
                collection.add_doc(document)

        info_msg = 'Retrieved {} docs'.format(len(collection.get_docs()))
        logging.getLogger('timeline_file_logger').info(info_msg)

        return collection

    def retrieve_doc(self, doc_id):
        if doc_id in self.__hashed_docs:
            return self.__hashed_docs[doc_id]

        # db_answer = self.__db_client[self.__db_name][self.__db_parsed_collection_name].find_one({'doc_id': doc_id})
        db_answer = self.__find_one_mongo(self.__db_name, self.__db_parsed_collection_name, doc_id)
        if db_answer is not None:
            document = TSDocument(doc_id)
            document.from_saved(db_answer['data'])
            self.__hashed_docs[doc_id] = document
            return document

        doc_content = self.__elastic_search.get(index=self.__index_name, doc_type=self.__doc_type, id=doc_id)
        doc_content_lemmas_termvectors = self.__elastic_search.termvectors(
                index=self.__index_name, doc_type=self.__doc_type, id=doc_id, fields='lemmas', term_statistics=True,
                field_statistics=False, positions=False, offsets=False)
        if len(doc_content) == 0 or doc_content['found'] is not True \
                or doc_content_lemmas_termvectors['found'] is not True:
            return None

        document = self.__create_doc(doc_id, doc_content, doc_content_lemmas_termvectors)
        serr_doc_str = document.to_saved()

        magic_big_number = 16793598
        if len(serr_doc_str) > magic_big_number:
            return None

        # self.__db_client[self.__db_name][self.__db_parsed_collection_name].insert_one(
           #  {'doc_id': doc_id, 'data': serr_doc_str})

        self.__insert_one_mongo(self.__db_name, self.__db_parsed_collection_name, doc_id, serr_doc_str)
        self.__hashed_docs[doc_id] = document
        return document

    def __create_doc(self, doc_id, doc_content, doc_content_lemmas_termvectors):
        document = TSDocument(doc_id)
        self.__process_meta_data(document, doc_content)
        self.__process_index_data(document, doc_content, doc_content_lemmas_termvectors)
        return document

    @staticmethod
    def __process_meta_data(document, doc_content):
        meta_dict = {'DATE': 'KRMN_DATE', 'SITE': 'KRMN_SITE', 'TITLE': 'KRMN_TITLE'}
        for meta_name, meta_pattern in meta_dict.items():
            meta_data = doc_content['_source'][meta_pattern]
            document.add_meta_data(meta_name, meta_data)

    def __process_index_data(self, document, doc_content, doc_content_lemmas_termvectors):
        raw_doc_text = doc_content['_source']['raw_text']

        sentences_info = doc_content['_source']['sentences']
        sent_data_re = re.compile('<sent b=(.*?) e=(.*?)>(.*?)</sent>')
        lemmas_termvectors = doc_content_lemmas_termvectors['term_vectors']['lemmas']['terms']
        for sent_id, sent_data in enumerate(re.findall(sent_data_re, sentences_info)):
            sent_b = int(sent_data[0])
            sent_e = int(sent_data[1])
            pos_info = [(sent_b, sent_e - sent_b, sent_id)]
            document.add_sentence(sent_id, pos_info)
            sent_lemmas = sent_data[2].split()
            for lemma in sent_lemmas:
                if lemma in self.__stop_words or lemma not in lemmas_termvectors:
                    continue

                df = lemmas_termvectors[lemma]['doc_freq']
                tf = lemmas_termvectors[lemma]['term_freq']
                tf_idf = tf * math.log(self.__mean_doc_in_shard / df)
                lemma_pos_info = [(-1, -1, sent_id)]
                document.add_index_item_to_sentence(TSIndexItem('lemma', lemma, tf_idf, lemma_pos_info), sent_id)

        document.add_meta_data('raw_view', raw_doc_text)
        for sent in document.sentence_iter():
            sent_b = sent.get_meta_data('sentence_start_word_num')
            sent_len = sent.get_meta_data('sentence_word_len')
            sent.add_meta_data('raw_view', raw_doc_text[sent_b:sent_b + sent_len])

    def __find_one_mongo(self, db_name, coll_name, doc_id):
        db_answer = None
        if self.__using_mongo:
            db_answer = self.__db_client[db_name][coll_name].find_one({'doc_id': doc_id})
        return db_answer

    def __insert_one_mongo(self, db_name, coll_name, doc_id, data):
        if self.__using_mongo:
            try:
                if self.m_Lock is not None:
                    self.m_Lock.acquire()
                    try:
                        if self.__find_one_mongo(self.__db_name, self.__db_parsed_collection_name, doc_id) is None:
                            self.__db_client[db_name][coll_name].insert_one({'doc_id': doc_id, 'data': data})
                    finally:
                        self.m_Lock.release()
                elif self.__find_one_mongo(self.__db_name, self.__db_parsed_collection_name, doc_id) is None:
                    self.__db_client[db_name][coll_name].insert_one({'doc_id': doc_id, 'data': data})
            except:
                print('WTF')
