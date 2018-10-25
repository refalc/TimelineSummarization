from .ts_primitives import *
from ..utils import utils
from .ts_query_constructor import TSQueryConstructor
from .ts_doc_importance import TSDocImportanceSolver


class TSCollectionConstructor:
    def __init__(self, config):
        self.m_Config = config
        self.m_DataExtractor = None
        self.m_DocImportanceSolver = TSDocImportanceSolver(config)

    def init_data_extractor(self, data_extractor):
        self.m_DataExtractor = data_extractor

    def init_w2v_model(self, w2v_model):
        self.m_DocImportanceSolver.init_w2v_model(w2v_model)

    def construct_collection(self, query):
        constr_coll_doccount = self.m_Config['constr_coll_doccount']
        constr_coll_softor = self.m_Config['constr_coll_softor']
        constr_coll_min_doc_rank = self.m_Config['constr_coll_min_doc_rank']
        call_params = {'doccnt': constr_coll_doccount,
                       'soft_or_coef': constr_coll_softor,
                       'min_doc_rank': constr_coll_min_doc_rank}

        index_type_list = ['ЛЕММА', 'СЛОВО', 'ТЕРМИН']

        '''
        call_query = TSQuery()
        for index_type in index_type_list:
            modality_list = self.m_Config['query_l1_modality_list']
            if index_type not in modality_list:
                continue
            top_k = modality_list[index_type]['result_query_size']
            index_data = query.get_index(index_type).get_index_data()
            top_k_items = TSQueryConstructor._get_index_top_k_items(index_data, top_k)
            for item in top_k_items:
                call_query.add_index_item(item)

        collection = self.m_DataExtractor.retrieve_docs_coll(call_query, call_params)
        '''
        collection = self.m_DataExtractor.retrieve_docs_coll(query, call_params)
        timeline_collection = TSTimeLineCollections()
        temporal_mode = self.m_Config['temporal']
        importance_mode = self.m_Config['importance']
        if temporal_mode:
            for doc in collection.iterate_docs():
                int_time = utils.get_document_int_time(doc, min_val='day')
                timeline_collection.add_doc(doc, int_time)
        else:
            timeline_collection.add_collection(collection, 0)

        self._cut_days_with_small_pubs_num(timeline_collection)
        if importance_mode:
            docid2importance, top_docs = self.m_DocImportanceSolver.construct_doc_importance(timeline_collection)
            timeline_collection.set_importance_data(docid2importance, top_docs)

        return timeline_collection

    @staticmethod
    def _cut_days_with_small_pubs_num(timeline_collection, top_k=3, threshold=0.2):
        sorted_colls = sorted([(time, coll) for time, coll in timeline_collection.iterate_collections_by_time()],
                              reverse=True)
        sorted_coll_lens = sorted([coll.get_len() for time, coll in sorted_colls],
                                  reverse=True)
        if len(sorted_coll_lens) == 0:
            return
        top3_mean_lean = sum(sorted_coll_lens[:top_k]) / len(sorted_coll_lens[:top_k])
        len_threshold = top3_mean_lean * threshold

        candidates_for_delete = []
        for time, coll in sorted_colls:
            if coll.get_len() < len_threshold:
                candidates_for_delete.append(time)

        for time in candidates_for_delete:
            timeline_collection.del_coll_by_time(time)