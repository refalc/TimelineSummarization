import math
import json
from scipy.spatial import distance
import numpy as np


class TSIndexItem:
    def __init__(self, item_type=None, item_name=None, item_weight=None, item_pos_info=None):
        self.m_Type = item_type
        self.m_Name = item_name
        self.m_Weight = round(item_weight, 2) if item_weight is not None else None
        self.m_PosInfo = item_pos_info

    def to_dict(self):
        return {'type': self.m_Type, 'name': self.m_Name, 'weight': self.m_Weight, 'pos_info': self.m_PosInfo}

    def from_dict(self, repr_dict):
        self.m_Type = repr_dict['type']
        self.m_Name = repr_dict['name']
        self.m_Weight = repr_dict['weight']
        self.m_PosInfo = repr_dict['pos_info']

    def to_saved(self):
        str_pos_info = json.dumps(self.m_PosInfo)
        saved_str = '{} {} {} {} {} {} {} {}'.format(
            len(self.m_Type), self.m_Type, len(self.m_Name), self.m_Name, len(str(self.m_Weight)), str(self.m_Weight),
            len(str_pos_info), str_pos_info)
        saved_str = '{} {}'.format(len(saved_str), saved_str)
        return saved_str

    def to_str(self):
        data_list = [self.m_Type, self.m_Name, self.m_Weight, self.m_PosInfo]
        data_list_str = json.dumps(data_list, ensure_ascii=False)
        return data_list_str

    def from_str(self, saved_str):
        data_list = json.loads(saved_str)
        self.m_Type = data_list[0]
        self.m_Name = data_list[1]
        self.m_Weight = data_list[2]
        self.m_PosInfo = data_list[3]

    def to_list(self):
        data_list = [self.m_Type, self.m_Name, self.m_Weight, self.m_PosInfo]
        return data_list

    def from_list(self, data_list):
        self.m_Type = data_list[0]
        self.m_Name = data_list[1]
        self.m_Weight = data_list[2]
        self.m_PosInfo = data_list[3]

    def __str__(self):
        return '{}:{}:{}:{}'.format(self.m_Type, self.m_Name, self.m_Weight, self.m_PosInfo)


class TSIndex:
    def __init__(self, index_type):
        self.m_IndexType = index_type
        self.m_IndexData = dict()
        self.m_Len = None
        self.m_IndexEmbedding = None

    def add_to_index(self, index_item):
        if self.m_Len is not None:
            self.m_Len = None
        self.m_IndexData[index_item.m_Name] = index_item

    def get_index_data(self):
        return self.m_IndexData

    def get_sorted_index_data_list(self):
        items_list = sorted([(item, item.m_Weight) for item_name, item in self.m_IndexData.items()],
                            key=lambda x: (-x[1], x[0].m_Name))
        return [item[0] for item in items_list]

    def get_type(self):
        return self.m_IndexType

    def sim(self, other):
        current_keys = set(self.m_IndexData.keys())
        other_keys = set(other.m_IndexData.keys())
        intersect_keys = current_keys.intersection(other_keys)
        dot_product = 0.0
        for key in intersect_keys:
            dot_product += self.m_IndexData[key].m_Weight * other.m_IndexData[key].m_Weight

        current_len = self.get_len()
        other_len = other.get_len()
        if current_len * other_len == 0:
            return 0.0
        return round(dot_product / (current_len * other_len), 3)

    def get_len(self):
        if self.m_Len is not None:
            return self.m_Len

        vector_len = 0.0
        for item_name, item in self.m_IndexData.items():
            vector_len += item.m_Weight * item.m_Weight

        self.m_Len = math.sqrt(vector_len)
        return self.m_Len

    def __str__(self):
        items_list = sorted([(item.m_Name, item.m_Weight) for item_name, item in self.m_IndexData.items()],
                            key=lambda x: (-x[1], x[0]))
        item_names_list = [item[0] for item in items_list]
        return ' '.join(item_names_list)

    def to_dict(self):
        index_dict = {'index_type': self.m_IndexType, 'ts_index': list()}
        for index_item_name in self.m_IndexData:
            index_dict['ts_index'].append(self.m_IndexData[index_item_name].to_list())
        return index_dict

    def from_dict(self, repr_dict):
        self.m_IndexType = repr_dict['index_type']
        self.m_IndexData = dict()
        for index_item_data in repr_dict['ts_index']:
            ts_index_item = TSIndexItem()
            ts_index_item.from_list(index_item_data)
            self.m_IndexData[ts_index_item.m_Name] = ts_index_item

    def construct_index_embedding(self, w2v_model):
        embedding_vector = None
        for item_name in self.m_IndexData:
            word = item_name.lower()
            weight = self.m_IndexData[item_name].m_Weight
            if word in w2v_model:
                if embedding_vector is None:
                    embedding_vector = weight * w2v_model[word]
                else:
                    embedding_vector += weight * w2v_model[word]

        norm = np.linalg.norm(embedding_vector)
        if norm > 0:
            embedding_vector /= norm
        self.m_IndexEmbedding = embedding_vector

    def sim_embedding(self, other):
        if self.m_IndexEmbedding is None or other.m_IndexEmbedding is None:
            return 0.0
        return np.dot(self.m_IndexEmbedding, other.m_IndexEmbedding)


class TSIndexiesHolder:
    def __init__(self):
        self.m_Indexies = dict()

        # todo del
        self.m_PosIndex = dict()

    def add_index_item(self, index_item):
        if index_item.m_Type not in self.m_Indexies:
            self.m_Indexies[index_item.m_Type] = TSIndex(index_item.m_Type)
        self.m_Indexies[index_item.m_Type].add_to_index(index_item)

    # todo del
    def add_pos_index_item(self, index_item):
        for pos_data in index_item.m_PosInfo:
            item_pos = pos_data[0]
            if item_pos not in self.m_PosIndex:
                self.m_PosIndex[item_pos] = dict()
            self.m_PosIndex[item_pos][index_item.m_Type] = index_item

    def get_index(self, index_type):
        return self.m_Indexies.get(index_type, None)

    # todo del
    def get_pos_index(self):
        return self.m_PosIndex

    def get_indexies(self):
        return self.m_Indexies

    def index_iter(self):
        return (self.m_Indexies[index] for index in self.m_Indexies)

    def sim(self, other, modality='lemma'):
        if modality not in self.m_Indexies or modality not in other.m_Indexies:
            return 0.0
        return self.m_Indexies[modality].sim(other.m_Indexies[modality])

    def sim_embedding(self, other, modality='lemma'):
        if modality not in self.m_Indexies or modality not in other.m_Indexies:
            return 0.0
        return self.m_Indexies[modality].sim_embedding(other.m_Indexies[modality])

    def to_dict(self):
        return {'ts_indexies_holder': {index_type: index.to_dict() for index_type, index in self.m_Indexies.items()}}

    def from_dict(self, repr_dict):
        self.m_Indexies = dict()
        for index_type in repr_dict['ts_indexies_holder']:
            ts_index = TSIndex(index_type)
            ts_index.from_dict(repr_dict['ts_indexies_holder'][index_type])
            self.m_Indexies[index_type] = ts_index


class TSMeta:
    def __init__(self):
        self.m_MetaData = dict()

    def add_meta_data(self, meta_tag, meta_data):
        self.m_MetaData[meta_tag] = meta_data

    def get_meta_data(self, meta_tag):
        return self.m_MetaData.get(meta_tag, None)

    def get_meta_data_dict(self):
        return self.m_MetaData

    def to_dict(self):
        return {'ts_meta': self.m_MetaData}

    def from_dict(self, repr_dict):
        self.m_MetaData = repr_dict['ts_meta']


class TSQuery(TSIndexiesHolder, TSMeta):
    def __init__(self):
        TSIndexiesHolder.__init__(self)
        TSMeta.__init__(self)

    def get_label(self):
        return str(self.get_meta_data('INT_DATE'))


class TSSentence(TSIndexiesHolder, TSMeta):
    def __init__(self, parent_document, sentence_id):
        TSIndexiesHolder.__init__(self)
        TSMeta.__init__(self)
        self.m_SentenceID = sentence_id
        self.m_ParentDoc = parent_document
        self.m_MetaData = dict()
        self.m_Label = None
        self._calc_label()

    def __eq__(self, other):
        return self.get_label() == other.get_label()

    def _calc_label(self):
        self.m_Label = '{}|{}'.format(self.m_ParentDoc.get_doc_id(), self.m_SentenceID)

    def get_label(self):
        return self.m_Label

    def get_sentence_id(self):
        return self.m_SentenceID

    def get_parent_doc(self):
        return self.m_ParentDoc

    def to_dict(self):
        dict_repr = {'sent_id': self.m_SentenceID,
                     'sent_meta_data': TSMeta.to_dict(self),
                     'sent_indexies': TSIndexiesHolder.to_dict(self)}
        return dict_repr

    def from_dict(self, repr_dict):
        self.m_SentenceID = repr_dict['sent_id']
        TSMeta.from_dict(self, repr_dict['sent_meta_data'])
        TSIndexiesHolder.from_dict(self, repr_dict['sent_indexies'])
        self._calc_label()

    def add_pos_info(self, pos_info):
        self.m_MetaData['sentence_start_word_num'] = pos_info[0][0]
        self.m_MetaData['sentence_word_len'] = pos_info[0][1]

    def __hash__(self):
        return hash('{}:{}'.format(self.m_SentenceID, self.m_ParentDoc.get_doc_id()))

    def get_sent_len(self, modality='lemma'):
        return len(self.get_index(modality).get_index_data())


class TSDocument(TSIndexiesHolder, TSMeta):
    def __init__(self, doc_id):
        TSIndexiesHolder.__init__(self)
        TSMeta.__init__(self)
        self.m_DocID = doc_id
        self.m_Sentences = dict()

        # not serialized
        self.m_DocImportance = 0.0

    def add_sentence(self, sentence_id, pos_info):
        self.m_Sentences[sentence_id] = TSSentence(self, sentence_id)
        self.m_Sentences[sentence_id].add_pos_info(pos_info)

    def add_index_item(self, index_item):
        TSIndexiesHolder.add_index_item(self, index_item)

        sentences_ids = [pos_info[2] for pos_info in index_item.m_PosInfo]
        for sent_id in sentences_ids:
            if sent_id not in self.m_Sentences:
                continue

            self.m_Sentences[sent_id].add_index_item(index_item)

    def add_index_item_to_sentence(self, index_item, sent_id):
        TSIndexiesHolder.add_index_item(self, index_item)
        if sent_id in self.m_Sentences:
            self.m_Sentences[sent_id].add_index_item(index_item)

    def sentence_iter(self):
        sentences = [self.m_Sentences[sent] for sent in self.m_Sentences]
        sentences = sorted(sentences, key=lambda sent: sent.get_sentence_id())
        return (sent for sent in sentences)

    def get_doc_id(self):
        return self.m_DocID

    def get_sentences_num(self):
        return len(self.m_Sentences)

    def to_dict(self):
        dict_repr = {'doc_id': self.m_DocID,
                     'meta_data': TSMeta.to_dict(self),
                     'indexies': TSIndexiesHolder.to_dict(self),
                     'sentences': {str(sent_id): sent.to_dict() for sent_id, sent in self.m_Sentences.items()}}
        return dict_repr

    def from_dict(self, dict_repr):
        self.m_DocID = int(dict_repr['doc_id'])
        TSMeta.from_dict(self, dict_repr['meta_data'])
        TSIndexiesHolder.from_dict(self, dict_repr['indexies'])

        self.m_Sentences = dict()
        for sent_id, sent_dict_data in dict_repr['sentences'].items():
            sent_id = int(sent_id)
            sentence = TSSentence(self, sent_id)
            sentence.from_dict(sent_dict_data)
            self.m_Sentences[sent_id] = sentence

    def from_saved(self, saved_data):
        loaded_dict = json.loads(saved_data)
        self.from_dict(loaded_dict)

    def to_saved(self):
        #return self.to_dict()
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def set_importance(self, importance):
        self.m_DocImportance = importance

    def get_importance(self):
        return self.m_DocImportance


class TSCollection:
    def __init__(self):
        self.m_Documents = dict()

    def add_doc(self, doc):
        self.m_Documents[doc.get_doc_id()] = doc

    def get_docs(self):
        return self.m_Documents

    def get_len(self):
        return len(self.m_Documents)

    def iterate_docs(self):
        return (self.m_Documents[doc_id] for doc_id in self.m_Documents)


class TSTimeLineCollections:
    def __init__(self):
        self.m_Collections = dict()
        self.m_TopDocs = None

    def add_doc(self, document, time):
        if time not in self.m_Collections:
            self.m_Collections[time] = TSCollection()
        self.m_Collections[time].add_doc(document)

    def get_doc(self, doc_id):
        for __, coll in self.m_Collections.items():
            for doc in coll.iterate_docs():
                if doc.get_doc_id() == doc_id:
                    return doc
        return None

    def add_collection(self, collection, time):
        if time in self.m_Collections:
            print('ERROR: TSTimeLineCollections:add_collection Not implemented')
            # merge
        else:
            self.m_Collections[time] = collection

    def iterate_collections(self):
        collections = [(time, coll) for time, coll in self.m_Collections.items()]
        return (val for val in collections)

    def iterate_collections_by_time(self):
        sorted_collections = sorted([(time, coll) for time, coll in self.m_Collections.items()], key=lambda x: x[0])
        return (val for val in sorted_collections)

    def set_importance_data(self, docid2importance, top_docs):
        for __, coll in self.m_Collections.items():
            for doc in coll.iterate_docs():
                doc.set_importance(docid2importance[doc.get_doc_id()])

        self.m_TopDocs = top_docs

    def get_top_docs(self):
        return self.m_TopDocs

    def del_coll_by_time(self, time):
        if time in self.m_Collections:
            del self.m_Collections[time]


class TSTimeLineQueries:
    def __init__(self):
        self.m_Queries = dict()

    def check_time(self, time):
        return time in self.m_Queries

    def add_query(self, query, time):
        self.m_Queries[time] = query

    def get_query_for_time(self, time):
        min_time_dif = 3600 * 24 * 365 * 10
        chosen_time = -1
        for query_time in sorted([time for time in self.m_Queries]):
            time_dif = abs(query_time - time)
            if time_dif < min_time_dif:
                chosen_time = query_time
                min_time_dif = time_dif
        if chosen_time == -1:
            print(time)
            print(self.m_Queries)
        return self.m_Queries[chosen_time]

    def iterate_queries(self):
        queries_list = sorted([(time, query) for time, query in self.m_Queries.items()], key=lambda x: x[0])
        return (val for val in queries_list)