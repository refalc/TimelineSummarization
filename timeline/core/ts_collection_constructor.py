from .ts_primitives import *
from ..utils import utils
from .ts_query_constructor import TSQueryConstructor
from .ts_doc_importance import TSDocImportanceSolver


class TSCollectionConstructor:
    def __init__(self, config, data_extractor):
        self.m_Config = config
        self.m_DataExtractor = data_extractor
        self.m_DocImportanceSolver = TSDocImportanceSolver(config)

    def construct_collection(self, query):
        constr_coll_doccount = self.m_Config['constr_coll_doccount']
        constr_coll_softor = self.m_Config['constr_coll_softor']
        constr_coll_min_doc_rank = self.m_Config['constr_coll_min_doc_rank']
        call_params = {'doccnt': constr_coll_doccount,
                       'soft_or_coef': constr_coll_softor,
                       'min_doc_rank': constr_coll_min_doc_rank}

        index_type_list = ['ЛЕММА', 'СЛОВО', 'ТЕРМИН']

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
        timeline_collection = TSTimeLineCollections()
        temporal_mode = self.m_Config['temporal']
        importance_mode = self.m_Config['importance']
        if temporal_mode:
            for doc in collection.iterate_docs():
                int_time = utils.get_document_int_time(doc)
                timeline_collection.add_doc(doc, int_time)
        else:
            timeline_collection.add_collection(collection, 0)

        if importance_mode:
            docid2importance, top_docs = self.m_DocImportanceSolver.construct_doc_importance(collection)
            timeline_collection.set_importance_data(docid2importance, top_docs)

        return timeline_collection
