from .ts_nldx_bridge import NldxSearchEngineBridge
from .ts_collection_constructor import TSCollectionConstructor
from .ts_query_constructor import TSQueryConstructor
from .ts_solver import TSSolver
from .ts_primitives import TSTimeLineQueries
from ..utils.utils import ConfigReader, SimpleTimer, get_document_int_time

from threading import BoundedSemaphore
import codecs


class TSController:
    def __init__(self, path_to_config):
        self.m_DataExtractor = None
        self.m_Config = ConfigReader(path_to_config)
        self.m_QueryConstructor = TSQueryConstructor(self.m_Config)
        self.m_CollectionConstructor = TSCollectionConstructor(self.m_Config)
        self.m_Solver = TSSolver(self.m_Config)
        self.m_SaveMutex = BoundedSemaphore()

    def run_queries(self, doc_id_list, answer_file):
        timer = SimpleTimer('TSController.run_queries')
        with codecs.open(answer_file, 'w', 'windows-1251') as file_descr:
            file_descr.write('')

        for story_id, doc_id in enumerate(doc_id_list):
            self.run_query(doc_id, answer_file, story_id)

        '''
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executer:
            futures = [executer.submit(TSController.run_query, self, doc_id, answer_file, story_id)
                       for story_id, doc_id in enumerate(doc_id_list)]
        '''

    def run_query(self, doc_id, answer_file, story_id=0):
        #timer = SimpleTimer('TSController.run_query')
        self.m_DataExtractor = NldxSearchEngineBridge('127.0.0.1', '2062')
        self.m_QueryConstructor.init_data_extractor(self.m_DataExtractor)
        self.m_CollectionConstructor.init_data_extractor(self.m_DataExtractor)

        query = self._construct_query(doc_id)
        timeline_collection = self._construct_collection(query)
        timeline_queries = self._construct_timeline_queries(query, timeline_collection)
        timeline_summary = self._construct_timeline_summary(timeline_queries, timeline_collection)
        self._save_summary(timeline_summary, timeline_queries, doc_id, story_id, answer_file)

    def _construct_query(self, doc_id):
        #timer = SimpleTimer('TSController._construct_query')
        return self.m_QueryConstructor.construct_query(doc_id)

    def _construct_collection(self, query):
        #timer = SimpleTimer('TSController._construct_collection')
        return self.m_CollectionConstructor.construct_collection(query)

    def _construct_timeline_queries(self, query, timeline_collection):
        #timer = SimpleTimer('TSController._construct_timeline_queries')
        timeline_queries = TSTimeLineQueries()
        query_int_date = query.get_meta_data('INT_DATE')
        timeline_queries.add_query(query, query_int_date)

        temporal_mode = self.m_Config['temporal']
        importance_mode = self.m_Config['importance']
        if temporal_mode and importance_mode:
            for doc_id in timeline_collection.get_top_docs():
                doc_int_date = get_document_int_time(timeline_collection.get_doc(doc_id), min_val='day')
                if timeline_queries.check_time(doc_int_date):
                    continue
                top_doc_query = self.m_QueryConstructor.construct_query(doc_id)
                timeline_queries.add_query(top_doc_query, doc_int_date)

        return timeline_queries

    def _construct_timeline_summary(self, timeline_queries, timeline_collection):
        #timer = SimpleTimer('TSController._construct_timeline_summary')
        return self.m_Solver.construct_timeline_summary(timeline_queries, timeline_collection)

    def _save_summary(self, timeline_summary, timeline_queries, doc_id, story_id, answer_file):
        #timer = SimpleTimer('TSController._save_summary')
        summary_text = ''
        summary_text += '<story id={} init_doc_id={}>\r\n'.format(story_id, doc_id)
        summary_text += '<queries>\r\n'
        for time, query in timeline_queries.iterate_queries():
            summary_text += '<query int_date={}>\r\n'.format(time)
            summary_text += '<lemmas>\r\n'
            summary_text += str(query.get_index('ЛЕММА')) + '\r\n'
            summary_text += '</lemmas>\r\n'
            summary_text += '<termins>\r\n'
            summary_text += str(query.get_index('ТЕРМИН')) + '\r\n'
            summary_text += '</termins>\r\n'
            summary_text += '</query>\r\n'
        summary_text += '</queries>\r\n'

        summary_text += '</summary>\r\n'
        for i in range(0, len(timeline_summary)):
            summary_text += '<sentence id={} mmr={}>\r\n'.format(i + 1, timeline_summary[i][1])
            sentence = timeline_summary[i][0]
            parent_doc = sentence.get_parent_doc()
            summary_text += '<metadata date={} site={} title={} doc_id={} sent_num={}\\>\r\n'.format(
                parent_doc.get_meta_data('DATE'), parent_doc.get_meta_data('SITE'), parent_doc.get_meta_data('TITLE'),
                parent_doc.get_doc_id(), sentence.get_sentence_id()
            )
            summary_text += sentence.get_meta_data('raw_view') + '\r\n'
            summary_text += '</sentence>\r\n'
        summary_text += '</summary>\r\n'
        summary_text += '</story>\r\n'

        with self.m_SaveMutex:
            with codecs.open(answer_file, 'a', 'windows-1251') as file_descr:
                file_descr.write(summary_text)
