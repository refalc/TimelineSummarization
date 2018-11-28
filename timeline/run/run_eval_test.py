from timeline.eval.ts_evaluator import TSEvaluator
from timeline.utils.utils import ConfigReader
import os


if __name__ == '__main__':
    stop_words_path = r'..\data\GoldSummary\stop_words.txt'
    ref_data_path = r'..\data\GoldSummary\gold1.xml'
    mapping_data_path = r'..\data\GoldSummary\id_to_querry.xml'
    evaluator = TSEvaluator(lemmatization=True)
    evaluator.init_stop_words(stop_words_path)
    evaluator.init_reference_data(ref_data_path, mapping_data_path)

    eval_out_file = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\29_10_18\test\eval_data.xml'
    docs_ids = ['10366234', '13394061', '10822682', '10482437', '10105996', '12171411', '12483331', '13197524',
                '11872175', '11768974', '11092876', '11136413', '11155970', '13142685', '12521721']

    process_num = 2

    configs_root = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\29_10_18\test\test_conf'
    answers_root = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\29_10_18\test\test_answer'
    #configurations = ['no_ext', 'one_ext', 'double_ext', 'double_ext_temp', 'double_ext_temp_imp',
                      #'double_ext_temp_imp_w2v']

    configurations = ['double_ext']
    for i in range(0, len(configurations)):
        conf = configurations[i]
        path_to_config = os.path.join(configs_root, 'config_{}.xml'.format(conf))
        path_to_answer = os.path.join(answers_root, 'answer_{}.xml'.format(conf))
        evaluator.run_and_evaluate(path_to_config, docs_ids, path_to_answer, eval_out_file, process_num)

    '''
    mode = 'overall'
    eval_res = evaluator.evaluate(answer_file, mode=mode)
    evaluator._save_results(eval_res, mode, ConfigReader(path_to_config), eval_out_file)

    mode = 'sent_by_sent'
    eval_res = evaluator.evaluate(answer_file, mode=mode)
    evaluator._save_results(eval_res, mode, ConfigReader(path_to_config), eval_out_file)
    '''
