from .ts_primitives import *
from ..utils import utils
import numpy as np


class TSQueryConstructor:
    def __init__(self, config, data_extractor):
        self.m_Config = config
        self.m_DataExtractor = data_extractor

    def construct_query(self, doc_id):
        query = self._construct_query_l1(doc_id)
        print('doc_id = {}'.format(doc_id))
        print(query.get_index('ЛЕММА'))
        print(query.get_index('ТЕРМИН'))
        int_date = query.get_meta_data('INT_DATE')
        query_constr_ext = self.m_Config['query_constr_ext']
        if query_constr_ext:
            query = self._construct_query_l2(query)
            query_constr_double_ext = self.m_Config['query_constr_double_ext']
            if query_constr_double_ext:
                query = self._construct_query_l3(query)
        print(query.get_index('ЛЕММА'))
        print(query.get_index('ТЕРМИН'))
        query.add_meta_data('INT_DATE', int_date)
        return query

    @staticmethod
    def _get_index_top_k_items(index_data, top_k):
        if top_k == 0:
            return []
        weight_list = [val.m_Weight for key, val in index_data.items()]
        top_k = min(top_k, len(weight_list))
        top_k_weight = -np.partition(-np.asarray(weight_list), top_k - 1)[top_k - 1]
        top_k_item_list = []
        for item_name, item in index_data.items():
            if item.m_Weight >= top_k_weight:
                top_k_item_list.append(item)

        #if len(top_k_item_list) > top_k:
            #top_k_item_list = sorted(top_k_item_list, key=lambda x: (-x.m_Weight, x.m_Name))[:top_k]

        return top_k_item_list

    @staticmethod
    def _get_coll_stat_top_k_items(coll_stat, top_k):
        if top_k == 0:
            return []

        weight_list = [val for key, val in coll_stat.items()]
        top_k = min(top_k, len(weight_list))
        top_k_weight = -np.partition(-np.asarray(weight_list), top_k - 1)[top_k - 1]
        top_k_item_list = []

        all_count = 0
        for name, count in coll_stat.items():
            all_count += count
        for name, count in coll_stat.items():
            if count > top_k_weight:
                top_k_item_list.append((name, count / all_count))

        if len(top_k_item_list) < top_k:
            equal_list = [(name, count) for name, count in coll_stat.items() if count == top_k_weight]
            equal_list = sorted(equal_list, key=lambda x: x[0])
            for name, count in equal_list:
                if len(top_k_item_list) == top_k:
                    break
                top_k_item_list.append((name, count / all_count))

        return top_k_item_list

    def _construct_query_l1(self, doc_id):
        query_l1 = TSQuery()
        document = self.m_DataExtractor.retrieve_doc(doc_id)
        if document is None:
            raise Exception('document is None')
        modality_list = self.m_Config['query_l1_modality_list']
        for modality in modality_list:
            top_k = modality_list[modality]['result_query_size']
            top_k_item_list = self._get_index_top_k_items(document.get_index(modality).get_index_data(), top_k)
            for item in top_k_item_list:
                query_l1.add_index_item(item)

        init_doc_int_time = utils.get_document_int_time(document)
        query_l1.add_meta_data('INT_DATE', init_doc_int_time)

        return query_l1

    def _construct_query_l2(self, query_l1):
        query_ext_doccount = self.m_Config['query_ext_doccount']
        query_ext_softor = self.m_Config['query_ext_softor']
        query_ext_min_doc_rank = self.m_Config['query_ext_min_doc_rank']
        call_params = {'doccnt': query_ext_doccount, 'soft_or_coef': query_ext_softor,
                       'min_doc_rank': query_ext_min_doc_rank}

        modality_list = self.m_Config['query_l2_modality_list']
        query_l2 = self._query_extension_process(query_l1, call_params, modality_list)

        return query_l2

    def _construct_query_l3(self, query_l2):
        query_ext_doccount = self.m_Config['query_ext_doccount']
        query_ext_softor = self.m_Config['query_ext_softor']
        query_ext_min_doc_rank = self.m_Config['query_ext_min_doc_rank']
        call_params = {'doccnt': query_ext_doccount, 'soft_or_coef': query_ext_softor,
                       'min_doc_rank': query_ext_min_doc_rank}

        modality_list = self.m_Config['query_l3_modality_list']
        query_l3 = self._query_extension_process(query_l2, call_params, modality_list)

        return query_l3

    def _query_extension_process(self, query, call_params, modality_list):
        query_ext= TSQuery()
        collection = self.m_DataExtractor.retrieve_docs_coll(query, call_params)
        for modality in modality_list:
            modality_coll_stat = dict()
            top_k = modality_list[modality]['top_items_count']
            for document in collection.iterate_docs():
                if document.get_index(modality) is None:
                    continue
                top_k_item_list = self._get_index_top_k_items(document.get_index(modality).get_index_data(), top_k)
                for item in top_k_item_list:
                    if item.m_Name not in modality_coll_stat:
                        modality_coll_stat[item.m_Name] = 0
                    modality_coll_stat[item.m_Name] += 1

            result_size = modality_list[modality]['result_query_size']
            top_k_stat_items = self._get_coll_stat_top_k_items(modality_coll_stat, result_size)
            for stat_item in top_k_stat_items:
                index_item = TSIndexItem(modality, stat_item[0], stat_item[1], [])
                query_ext.add_index_item(index_item)

        return query_ext