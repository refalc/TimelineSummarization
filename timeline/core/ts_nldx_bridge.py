import pymongo
from pymongo import MongoClient
import requests
from xml.etree import ElementTree
import nltk
import re
from .ts_primitives import *
from ..utils.utils import SimpleTimer


class NldxSearchEngineBridge:
    def __init__(self, address, port, db_name='nldx'):
        self.m_Address = address
        self.m_Port = port
        self.m_DBClient = MongoClient()
        self.m_DBName = db_name
        self.m_DBCollectionName = 'doc_content'
        if self.m_DBName not in self.m_DBClient.database_names() or \
                self.m_DBCollectionName not in self.m_DBClient[self.m_DBName].collection_names():
            self.m_DBClient[self.m_DBName][self.m_DBCollectionName].create_index([('doc_id', pymongo.ASCENDING)],
                                                                                 unique=True)
        print('Loaded collection with {} docs'.format(self.m_DBClient[self.m_DBName][self.m_DBCollectionName].count()))
        self.m_DBParsedCollectionName = 'parsed_doc_content'
        if self.m_DBName not in self.m_DBClient.database_names() or \
                self.m_DBParsedCollectionName not in self.m_DBClient[self.m_DBName].collection_names():
            self.m_DBClient[self.m_DBName][self.m_DBParsedCollectionName].create_index([('doc_id', pymongo.ASCENDING)],
                                                                                       unique=True)
        #self.m_DBClient[self.m_DBName][self.m_DBParsedCollectionName].remove()
        print('Loaded parsed collection with {} docs'.format(
            self.m_DBClient[self.m_DBName][self.m_DBParsedCollectionName].count()))

        self.m_StopWords = [elem.upper() for elem in nltk.corpus.stopwords.words('russian')]
        self.m_StopWords += ['HDR', 'CIR', 'CIR_HDR', 'CIRHDR', 'CIRPAR', 'NUM']

        self.m_HashedDocs = dict()
        try:
            self.m_Lock = lock
        except Exception as e:
            self.m_Lock = None

    def retrieve_docs(self, query, params):
        common_query_list = []
        termins_query_list = []
        for index in query.index_iter():
            if index.get_type() == 'ТЕРМИН':
                for item in index.get_sorted_index_data_list():
                    termins_query_list.append('/ТЕРМИН=\"' + item.m_Name + '\"')
            else:
                for item in index.get_sorted_index_data_list():
                    common_query_list.append(item.m_Name)

        query_list = common_query_list + termins_query_list
        req_params = {'doccnt': params['doccnt'], 'soft_or_coef': params['soft_or_coef'],
                      'reqtext': '+'.join(query_list)}
        req_query = 'http://{}:{}'.format(self.m_Address, self.m_Port)

        print('ir request={}'.format(req_params))

        if self.m_Lock is not None:
            self.m_Lock.acquire()
            try:
                req_res = requests.get(req_query, req_params)
            finally:
                self.m_Lock.release()
        else:
            req_res = requests.get(req_query, req_params)

        if len(req_res.text) == 0:
            print(req_res)
            return []

        docs = []
        res_root = ElementTree.fromstring(req_res.text)
        for doc in res_root.iter('doc'):
            doc_id = int(doc.get('id'))
            doc_rank_w = float(doc.get('rank'))
            if doc_rank_w < params['min_doc_rank']:
                break

            docs.append((doc_id, doc_rank_w))
        if len(docs) == 0:
            print(req_res.text)
        return docs

    def retrieve_docs_coll(self, query, params):
        timer = SimpleTimer('NldxSearchEngineBridge.retrieve_docs_coll')
        collection = TSCollection()
        recieved_docs = self.retrieve_docs(query, params)

        print('Retrieved {} doc ids'.format(len(recieved_docs)))
        for doc_data in recieved_docs:
            doc_id = doc_data[0]
            document = self.retrieve_doc(doc_id)
            if document is not None:
                collection.add_doc(document)

        print('Retrieved {} docs'.format(len(collection.get_docs())))
        return collection

    def retrieve_doc(self, doc_id):
        if doc_id in self.m_HashedDocs:
            return self.m_HashedDocs[doc_id]

        db_answer = self.m_DBClient[self.m_DBName][self.m_DBParsedCollectionName].find_one({'doc_id': doc_id})

        if db_answer is not None:
            document = TSDocument(doc_id)
            document.from_saved(db_answer['data'])
            self.m_HashedDocs[doc_id] = document
            return document

        doc_content = self._retrieve_doc_content(doc_id)
        if len(doc_content) == 0:
            return None

        document = self._create_doc(doc_id, doc_content)
        serr_doc_str = document.to_saved()
        if len(serr_doc_str) > 16793598:
            return None

        self.m_DBClient[self.m_DBName][self.m_DBParsedCollectionName].insert_one(
            {'doc_id': doc_id, 'data': serr_doc_str})
        self.m_HashedDocs[doc_id] = document
        return document

    def _retrieve_doc_content(self, doc_id):
        params = {'doc_id': str(doc_id), 'show_all_feats': str(1)}
        req_query = 'http://{}:{}/doc_text'.format(self.m_Address, self.m_Port)
        db_answer = self.m_DBClient[self.m_DBName][self.m_DBCollectionName].find_one({'doc_id': doc_id})
        if db_answer is not None:
            return db_answer['data']

        if self.m_Lock is not None:
            self.m_Lock.acquire()
            try:
                req_res = requests.get(req_query, params)
            finally:
                self.m_Lock.release()
        else:
            req_res = requests.get(req_query, params)

        req_text = req_res.text
        if len(req_text) > 500000:
            req_text = ''

        self.m_DBClient[self.m_DBName][self.m_DBCollectionName].insert_one({'doc_id': doc_id, 'data': req_text})
        return req_text

    def _extract_meta_data(self, document, doc_content):
        meta_dict = {'DATE': 'KRMN_DATE', 'LENGTH': 'KRMN_LENGTH', 'SITE': 'KRMN_SITE', 'TITLE': 'KRMN_TITLE'}
        for meta_name, meta_pattern in meta_dict.items():
            meta_data = re.search(re.escape(meta_pattern) + '(.*?)= (.*?)<BR>', doc_content)
            if meta_data is not None:
                document.add_meta_data(meta_name, meta_data.group(2))

    def _extract_index_data(self, document, doc_content):
        end_of_general_content = doc_content.find('</HTML>')
        table_start = doc_content.find('<table', end_of_general_content)
        table_end = doc_content.find('</table>', end_of_general_content)
        if table_start < 0 or table_end < 0:
            raise Exception('ERROR: table_start < 0 or table_end < 0')
        entity_table = doc_content[table_start:table_end + len('</table>')].replace('&AMP;', '&').replace('&QUOT;',
                                                                                                          '\"')
        index_data_types = {'ТЕРМИН', 'ЛЕММА', 'СЛОВО'}
        entity_list = []
        last_hdr_position = -1

        re_tr = re.compile('<tr><td>(.*?)</td><td>(.*?)</td><td><b>(.*?)</b></td><td>(.*?)</td>'
                           '<td>(.*?)</td><td>(.*?)</td></tr>', re.DOTALL)

        if document.get_doc_id() == 10381755:
            print(10381755)
        lemmas_pos_index = dict()
        max_pos_index = 0
        for tr in re.findall(re_tr, entity_table):
            td_list = tr
            if len(td_list) != 6:
                continue
            entity_type = td_list[1]

            entity_name = td_list[2]
            entity_tfidf = float(td_list[3])
            entity_pos_info = self._parse_pos_info(td_list[5])
            index_item = TSIndexItem(entity_type, entity_name, entity_tfidf, entity_pos_info)
            entity_list.append(index_item)

            if entity_type == 'ЛЕММА':
                for pos in entity_pos_info:
                    max_pos_index = max(max_pos_index, pos[0])
                    if pos in lemmas_pos_index:
                        if lemmas_pos_index[pos].m_Weight < index_item.m_Weight:
                            lemmas_pos_index[pos] = index_item
                    else:
                        lemmas_pos_index[pos] = index_item

            if entity_name == 'HDR':
                last_hdr_position = entity_pos_info[-1:][0][0]

        chosen_lemmas = set([item.m_Name for pos, item in lemmas_pos_index.items()])

        for entity in entity_list:
            if entity.m_Type == 'SENT' and entity.m_Name.isdigit():
                if entity.m_PosInfo[0][1] < 3:
                    continue
                document.add_sentence(int(entity.m_Name), entity.m_PosInfo)
            elif entity.m_Type in index_data_types:
                document.add_pos_index_item(entity)
                if entity.m_Type == 'ЛЕММА' and entity.m_Name not in chosen_lemmas:
                    continue
                entity.m_PosInfo = [pos_data for pos_data in entity.m_PosInfo if pos_data[0] > last_hdr_position]
                if len(entity.m_PosInfo) == 0:
                    continue
                if entity.m_Name not in self.m_StopWords:
                    document.add_index_item(entity)

        table_start = doc_content.rfind('<table')
        table_end = doc_content.rfind('</table>')
        if table_start < 0 or table_end < 0:
            raise Exception('ERROR: table_start < 0 or table_end < 0')

        lemmas2byte_map_table = doc_content[table_start:table_end + len('</table>')]
        lemmas2byte_map_table_root = ElementTree.fromstring(lemmas2byte_map_table)
        lemmas2byte_map = dict()
        doc_pos_index = document.get_pos_index()
        for tr in lemmas2byte_map_table_root.iter('tr'):
            td_list = tr.findall('td')
            if len(td_list) != 2:
                continue
            word_pos = int(td_list[0].text)
            byte_shift = int(td_list[1].text)

            if word_pos not in doc_pos_index:
                continue

            label = 'СЛОВО'
            if label not in doc_pos_index[word_pos]:
                continue
            word_name = doc_pos_index[word_pos]['СЛОВО'].m_Name
            word_len = len(word_name)
            lemmas2byte_map[word_pos] = (word_name, byte_shift, word_len)

        first_id = min([elem for elem in lemmas2byte_map])
        last_id = max([elem for elem in lemmas2byte_map])

        reg_cut = re.compile('</TITLE>(.*?)</HEAD>', re.DOTALL)
        doc_content = re.sub(reg_cut, '</TITLE>\r\n\r\n</HEAD>', doc_content)
        doc_start_byte = lemmas2byte_map[first_id][1] + 1
        doc_end_byte = lemmas2byte_map[last_id][1] + lemmas2byte_map[last_id][2] + 1
        document.add_meta_data('raw_view', doc_content[doc_start_byte: doc_end_byte])
        for sent in document.sentence_iter():
            sentence_meta = sent.get_meta_data_dict()
            start_sent_word = sentence_meta['sentence_start_word_num']
            sentence_len = sentence_meta['sentence_word_len']
            last_sent_word = start_sent_word + sentence_len - 1

            while start_sent_word not in lemmas2byte_map and start_sent_word <= max_pos_index:
                start_sent_word += 1

            while last_sent_word not in lemmas2byte_map and last_sent_word >= start_sent_word:
                last_sent_word -= 1

            if start_sent_word not in lemmas2byte_map:
                pass
            sent_start_byte = lemmas2byte_map[start_sent_word][1] + 1
            sent_end_byte = lemmas2byte_map[last_sent_word][1] + lemmas2byte_map[last_sent_word][2] + 1
            sent_raw_view = doc_content[sent_start_byte: sent_end_byte].replace('\n', '').replace('\r', '')
            sent_raw_view = re.sub('(.*)</CIR_HDR>', '', sent_raw_view)
            sent_raw_view = re.sub('<cirpar(.*?)>', '', sent_raw_view).replace('</cirpar>', '').replace('cirpar>', '')

            sentence_meta['raw_view'] = sent_raw_view

    def _create_doc(self, doc_id, doc_content):
        document = TSDocument(doc_id)
        self._extract_meta_data(document, doc_content)
        self._extract_index_data(document, doc_content)
        return document

    def _parse_pos_info(self, pos_info):
        results = []
        pos_datas = pos_info.replace('\n', '').replace('\r', '').split(',')
        for pos_data in pos_datas:
            entity_pos = int(pos_data[:pos_data.find('(')])
            entity_len = int(pos_data[pos_data.find('(') + 1:pos_data.find(')')])
            entity_sent = int(pos_data[pos_data.find('[') + 1:pos_data.find(']')])
            '''
            pos_reg_data = re.search('(.*?)\((.*?)\)\[(.*?)\]', pos_data)
            if pos_reg_data is None:
                raise Exception('pos_reg_data is None')

            entity_pos = int(pos_reg_data.group(1))
            entity_len = int(pos_reg_data.group(2))
            entity_sent = int(pos_reg_data.group(3))
            '''
            results.append((entity_pos, entity_len, entity_sent))

        return results


