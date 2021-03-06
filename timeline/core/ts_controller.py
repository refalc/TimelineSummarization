from .ts_nldx_bridge import NldxSearchEngineBridge
from .ts_elastic_bridge import ElasticSearchBridge
from .ts_collection_constructor import TSCollectionConstructor
from .ts_query_constructor import TSQueryConstructor
from .ts_solver import TSSolver
from .ts_primitives import TSTimeLineQueries
from ..utils.utils import ConfigReader, SimpleTimer, get_document_int_time, TSLogger
from gensim.models import KeyedVectors
import multiprocessing
import threading
import codecs
from logging.handlers import QueueHandler
import logging


def init_process(log_queue, nldx_lock=None):
    if nldx_lock is not None:
        global __nldx_lock
        __nldx_lock = nldx_lock

    logger = logging.getLogger('timeline_file_logger')
    if len(logger.handlers) == 0:
        logger.setLevel(logging.DEBUG)
        logger_queue_handler = QueueHandler(log_queue)
        logger.addHandler(logger_queue_handler)

        logger = logging.getLogger('timeline_console_logger')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logger_queue_handler)


class TSController:
    def __init__(self, path_to_config, log_file='./ts_log.txt'):
        self.m_DataExtractor = None
        self.m_Config = ConfigReader(path_to_config)
        if self.m_Config['di_w2v'] or self.m_Config['slv_w2v']:
            self.m_W2V_model = KeyedVectors.load(self.m_Config['w2v_path'])
        self.m_QueryConstructor = TSQueryConstructor(self.m_Config)
        self.m_CollectionConstructor = TSCollectionConstructor(self.m_Config)
        self.m_Solver = TSSolver(self.m_Config)

        if self.m_Config['di_w2v']:
            self.m_CollectionConstructor.init_w2v_model(self.m_W2V_model)
        if self.m_Config['slv_w2v']:
            self.m_Solver.init_w2v_model(self.m_W2V_model)

        self.m_LogFile = log_file

    def run_queries(self, doc_id_list, answer_file, processes=1, search_engine_name='elastic'):
        logging_queue = TSLogger.get_logger_queue()
        init_process(logging_queue)

        logger = TSLogger(self.m_LogFile)
        logger.run_logger()

        process_pool = None
        try:
            timer = SimpleTimer('TSController.run_queries')

            summaries = []
            error_results = []

            run_queries_lock = multiprocessing.Lock()
            process_pool = multiprocessing.Pool(processes=processes, initializer=init_process,
                                                initargs=(logging_queue, run_queries_lock))
            run_query_args = [(self, doc_id, story_id, search_engine_name) for story_id, doc_id in enumerate(doc_id_list)]
            run_queries_results = process_pool.starmap(TSController.run_query, run_query_args)
            for query_result in run_queries_results:
                if query_result[0] == 'OK':
                    summaries.append((query_result[3], query_result[1]))
                else:
                    error_results.append(query_result)

            if len(error_results) != 0:
                print('Completed with ERRORS:')
                for error_info in error_results:
                    print(error_info)

            summaries = sorted(summaries, key=lambda summ_info: summ_info[0])
            with codecs.open(answer_file, 'w', 'windows-1251') as file_descr:
                for story_id, summary_text in summaries:
                    file_descr.write(summary_text)

            return len(summaries) == len(doc_id_list) and len(error_results) == 0
        finally:
            process_pool.close()
            process_pool.join()

            logger.stop_logger()

    def run_query(self, doc_id, story_id=0, search_engine_name='elastic'):
        # timer = SimpleTimer('TSController.run_query')
        try:
            #logging.getLogger('timeline_console_logger').info('Start doc_id={} story_id={}'.format(doc_id, story_id))
            if search_engine_name == 'elastic':
                self.m_DataExtractor = ElasticSearchBridge('127.0.0.1', '9200', db_name=search_engine_name)
            else:
                self.m_DataExtractor = NldxSearchEngineBridge('127.0.0.1', '2062', db_name=search_engine_name)

            self.m_QueryConstructor.init_data_extractor(self.m_DataExtractor)
            self.m_CollectionConstructor.init_data_extractor(self.m_DataExtractor)

            query = self._construct_query(doc_id)
            timeline_collection = self._construct_collection(query)
            timeline_queries = self._construct_timeline_queries(query, timeline_collection)
            timeline_summary = self._construct_timeline_summary(timeline_queries, timeline_collection)
            summary_text = self._gen_summary_text(timeline_summary, timeline_queries, doc_id, story_id)
            run_result = ('OK', summary_text, doc_id, story_id)
        except Exception as e:
            run_result = ('ERROR', e, doc_id, story_id)

        #logging.getLogger('timeline_console_logger').info(
            #'End doc_id={} story_id={} with code={}'.format(doc_id, story_id, run_result[0]))

        return run_result

    def get_config(self):
        return self.m_Config

    def _construct_query(self, doc_id):
        # timer = SimpleTimer('TSController._construct_query')
        return self.m_QueryConstructor.construct_query(doc_id)

    def _construct_collection(self, query):
        # timer = SimpleTimer('TSController._construct_collection')
        return self.m_CollectionConstructor.construct_collection(query)

    def _construct_timeline_queries(self, query, timeline_collection):
        # timer = SimpleTimer('TSController._construct_timeline_queries')
        timeline_queries = TSTimeLineQueries()
        query_int_date = query.get_meta_data('INT_DATE')
        timeline_queries.add_query(query, query_int_date)

        temporal_mode = self.m_Config['temporal']
        importance_mode = self.m_Config['importance']
        if temporal_mode and importance_mode:
            for doc_id in timeline_collection.get_top_docs():
                doc_int_date = get_document_int_time(timeline_collection.get_doc(doc_id), min_val_tag='day')
                if timeline_queries.check_time(doc_int_date):
                    continue
                top_doc_query = self.m_QueryConstructor.construct_query(doc_id)
                timeline_queries.add_query(top_doc_query, doc_int_date)

        return timeline_queries

    def _construct_timeline_summary(self, timeline_queries, timeline_collection):
        # timer = SimpleTimer('TSController._construct_timeline_summary')
        return self.m_Solver.construct_timeline_summary(timeline_queries, timeline_collection)

    @staticmethod
    def _gen_summary_text(timeline_summary, timeline_queries, doc_id, story_id):
        summary_text = ''
        summary_text += '<story id={} init_doc_id={}>\r\n'.format(story_id, doc_id)
        summary_text += '<queries>\r\n'
        for time, query in timeline_queries.iterate_queries():
            summary_text += '<query int_date={}>\r\n'.format(time)
            summary_text += '<lemmas>\r\n'
            summary_text += str(query.get_index('lemma')) + '\r\n'
            summary_text += '</lemmas>\r\n'
            summary_text += '<termins>\r\n'
            summary_text += str(query.get_index('termin')) + '\r\n'
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
        return summary_text
