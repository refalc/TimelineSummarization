import pickle
import codecs
import json
from hyperopt import fmin, tpe, hp, Trials
from timeline.core.ts_controller import TSController
from timeline.eval.ts_evaluator import TSEvaluator


def generate_config(space, path_to_config):
    with codecs.open(path_to_config, 'w', 'windows-1251') as file_descr:
        for param_name in space:
            tag_name = param_name
            tag_type = None
            tag_val = None
            if type(space[param_name]) != dict:
                tag_type = type(space[param_name]).__name__
                tag_val = space[param_name]
            else:
                tag_type = 'json_dict'
                tag_val = json.dumps(space[param_name], ensure_ascii=False)
            file_descr.write('<{} type={}>{}</{}>\n'.format(tag_name, tag_type, tag_val, tag_name))


space_config_no_ext = {
    'query_l1_modality_list': {'lemma': {'result_query_size': hp.choice('result_query_size', range(2, 8))}},
    'query_constr_ext': 0,
    'constr_coll_doccount': hp.choice('constr_coll_doccount', [x for x in range(100, 501) if x % 50 == 0]),
    'constr_coll_softor': 0.5,
    'constr_coll_min_doc_rank': 0.0,
    'temporal': 0,
    'max_daily_answer_size': 15,
    'timeline_summary_size': 15,
    'max_sentence_len': 50,
    'min_sentence_len': 4,
    'min_mmr': 0.0,
    'lambda': hp.uniform('lambda', 0.4, 1.0),
    'importance': 0,
    'slv_w2v': 0,
}


def func_to_minimize(space):
    print("Start func...")
    print(space)
    docs_ids = ['10366234', '13394061', '10822682', '10482437', '10105996', '12171411', '12483331', '13197524',
                '11872175', '11768974', '11092876', '11136413', '11155970', '13142685', '12521721']
    process_num = 2
    path_to_config = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\01_11_18\config.xml'
    path_to_answer = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\01_11_18\answer.xml'
    log_file = r'C:\!DEV\C++\TemporalSummarization\TemporalSummarizationVS\Data\01_11_18\ts_log.txt'

    generate_config(space, path_to_config)
    controller = TSController(path_to_config, log_file)
    res = controller.run_queries(docs_ids, path_to_answer, process_num)

    stop_words_path = r'C:\!DEV\C++\Diplom\GoldSummary\stop_words.txt'
    ref_data_path = r'C:\!DEV\C++\Diplom\GoldSummary\gold1.xml'
    mapping_data_path = r'C:\!DEV\C++\Diplom\GoldSummary\id_to_querry.xml'
    evaluator = TSEvaluator(lemmatization=True)
    evaluator.init_stop_words(stop_words_path)
    evaluator.init_reference_data(ref_data_path, mapping_data_path)
    evaluate_results = evaluator.evaluate(path_to_answer, mode='sent_by_sent')
    r1 = evaluate_results['mean']['rouge-1']['r']
    r2 = evaluate_results['mean']['rouge-2']['r']

    score = r2 + r1
    print("End func. Score = " + str(score))
    return 2 - score


if __name__ == '__main__':
    trials = Trials()  # pickle.load(open("myfile.p", "rb"))

    for i in range(0, 1):
        best = fmin(
            fn=func_to_minimize,
            space=space_config_no_ext,
            algo=tpe.suggest,
            max_evals=len(trials.trials) + 1,
            trials=trials
        )

        pickle.dump(trials, open("trial_dump_no_ext.p", "wb"))

        print(len(trials.trials))
        print(best)
        # func_to_minimize(best)
