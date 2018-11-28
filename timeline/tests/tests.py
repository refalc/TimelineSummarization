from ..core.ts_controller import TSController
import tempfile
import os
import codecs


class RegTest:
    def __init__(self, ref_answer_file, config_file, doc_ids, process_num=2, log_file='./ts_log.txt'):
        self.m_RefFilePath = ref_answer_file
        self.m_GenFilePath = os.path.join(tempfile.gettempdir(), 'temp_answer.xml')
        self.m_Controller = TSController(config_file, log_file=log_file)
        self.m_DocIds = doc_ids
        self.m_ProcessNum = process_num

    def run_reg_test(self):
        search_engine_name = 'nldx'
        res = self.m_Controller.run_queries(self.m_DocIds, self.m_GenFilePath, self.m_ProcessNum, search_engine_name)
        if not res:
            return 'ERROR', 'run_queries'

        res = self._cmp_results()
        if not res:
            return 'ERROR', '_cmp_results'

        return 'OK', ''

    def _cmp_results(self):
        try:
            with codecs.open(self.m_RefFilePath, 'r', 'windows-1251') as file_descr:
                ref_data = file_descr.read()
            with codecs.open(self.m_GenFilePath, 'r', 'windows-1251') as file_descr:
                hypo_data = file_descr.read()
        except Exception:
            return False

        return ref_data == hypo_data
