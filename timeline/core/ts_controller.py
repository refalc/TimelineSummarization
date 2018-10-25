from .ts_nldx_bridge import NldxSearchEngineBridge
from .ts_collection_constructor import TSCollectionConstructor
from .ts_query_constructor import TSQueryConstructor
from .ts_solver import TSSolver
from .ts_primitives import TSTimeLineQueries
from ..utils.utils import ConfigReader, SimpleTimer, get_document_int_time
from gensim.models import KeyedVectors
import multiprocessing
import codecs


def init_pool(my_lock):
    global lock
    lock = my_lock


class TSController:
    def __init__(self, path_to_config):
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

    def run_queries(self, doc_id_list, answer_file, processes=1):
        timer = SimpleTimer('TSController.run_queries')

        summaries = []
        error_results = []

        single_process = True if processes <= 1 else False
        if single_process:
            for story_id, doc_id in enumerate(doc_id_list):
                query_result = self.run_query(doc_id, story_id)
                if query_result[0] == 'OK':
                    summaries.append((query_result[3], query_result[1]))
                else:
                    error_results.append(query_result)

            if len(error_results) != 0:
                print('Completed with ERRORS:')
                for error_info in error_results:
                    print(error_info)
            else:
                print('Completed without ERRORS')
        else:
            run_queries_lock = multiprocessing.Lock()
            process_pool = multiprocessing.Pool(processes=processes, initializer=init_pool, initargs=(run_queries_lock, ))
            run_query_args = [[self]*len(doc_id_list), doc_id_list,
                              [story_id for story_id in range(0, len(doc_id_list))]]

            run_query_args = [(self, doc_id, story_id) for story_id, doc_id in enumerate(doc_id_list)]
            run_queries_results = process_pool.starmap(TSController.run_query, run_query_args)
            for query_result in run_queries_results:
                if query_result[0] == 'OK':
                    summaries.append((query_result[3], query_result[1]))
                else:
                    error_results.append(query_result)

        summaries = sorted(summaries, key=lambda summ_info: summ_info[0])
        with codecs.open(answer_file, 'w', 'windows-1251') as file_descr:
            for story_id, summary_text in summaries:
                file_descr.write(summary_text)


        '''
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executer:
            futures = [executer.submit(TSController.run_query, self, doc_id, answer_file, story_id)
                       for story_id, doc_id in enumerate(doc_id_list)]
        '''

    def run_query(self, doc_id, story_id=0):
        #timer = SimpleTimer('TSController.run_query')
        try:
            self.m_DataExtractor = NldxSearchEngineBridge('127.0.0.1', '2062')
            self.m_QueryConstructor.init_data_extractor(self.m_DataExtractor)
            self.m_CollectionConstructor.init_data_extractor(self.m_DataExtractor)

            query = self._construct_query(doc_id)
            timeline_collection = self._construct_collection(query)
            timeline_queries = self._construct_timeline_queries(query, timeline_collection)
            timeline_summary = self._construct_timeline_summary(timeline_queries, timeline_collection)
            #self._save_summary(timeline_summary, timeline_queries, doc_id, story_id, answer_file)
            summary_text = self._gen_summary_text(timeline_summary, timeline_queries, doc_id, story_id)
            return 'OK', summary_text, doc_id, story_id
        except Exception as e:
            return 'ERROR', e, doc_id, story_id

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

    def _gen_summary_text(self, timeline_summary, timeline_queries, doc_id, story_id):
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
        return summary_text

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

        with codecs.open(answer_file, 'a', 'windows-1251') as file_descr:
            file_descr.write(summary_text)
