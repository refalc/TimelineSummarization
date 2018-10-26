from timeline.eval.ts_evaluator import TSEvaluator
from timeline.utils.utils import ConfigReader


if __name__ == '__main__':
    stop_words_path = 'C:\\!DEV\\C++\\Diplom\\GoldSummary\\stop_words.txt'
    ref_data_path = 'C:\\!DEV\\C++\\Diplom\\GoldSummary\\gold1.xml'
    mapping_data_path = 'C:\\!DEV\\C++\\Diplom\\GoldSummary\\id_to_querry.xml'
    hypo_data_path = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\answer_double_ext_temp_imp_w2v.xml'
    evaluator = TSEvaluator(lemmatization=True)
    evaluator.init_stop_words(stop_words_path)
    evaluator.init_reference_data(ref_data_path, mapping_data_path)

    path_to_config = r'C:\Users\MishaDEV\Data\config_double_ext_temp_imp_w2v.xml'
    answer_file = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\answer_double_ext_temp_imp_w2v.xml'
    eval_out_file = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\eval_data.xml'
    docs_ids = ['10366234', '13394061', '10822682', '10482437', '10105996', '12171411', '12483331', '13197524',
                '11872175', '11768974', '11092876', '11136413', '11155970', '13142685', '12521721']

    docs_ids = ['11136413', '11155970']
    process_num = 2

    '''
    for i in range(0, 10):
        answer_name = answer_file[:-4] + str(i) + '.xml'
        eval_res = evaluator.run_and_evaluate(path_to_config, docs_ids, answer_name, eval_out_file, process_num)
    '''
    eval_res = evaluator.run_and_evaluate(path_to_config, docs_ids, answer_file, eval_out_file, process_num)
    '''
    mode = 'overall'
    eval_res = evaluator.evaluate(answer_file, mode=mode)
    evaluator._save_results(eval_res, mode, ConfigReader(path_to_config), eval_out_file)

    mode = 'sent_by_sent'
    eval_res = evaluator.evaluate(answer_file, mode=mode)
    evaluator._save_results(eval_res, mode, ConfigReader(path_to_config), eval_out_file)
    '''
