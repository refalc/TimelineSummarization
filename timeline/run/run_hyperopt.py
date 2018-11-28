import pickle
import codecs
import json
from hyperopt.pyll.base import Apply
from hyperopt import fmin, tpe, hp, Trials
import os
import sys


def generate_config(space, path_to_config):
    # todo refactor this
    with codecs.open(path_to_config, 'w', 'windows-1251') as file_descr:
        for param_name in space:
            tag_name = param_name
            if type(space[param_name]) != dict:
                tag_type = type(space[param_name]).__name__
                tag_val = space[param_name]
            else:
                tag_type = 'json_dict'
                if param_name == 'query_l2_modality_list' or param_name == 'query_l3_modality_list':
                    for modal in space[param_name]:
                        if 'result_query_size_l2' in space[param_name][modal]:
                            space[param_name][modal]['result_query_size'] = space[param_name][modal].pop(
                                'result_query_size_l2')
                        if 'top_items_count_l2' in space[param_name][modal]:
                            space[param_name][modal]['top_items_count'] = space[param_name][modal].pop(
                                'top_items_count_l2')
                        if 'result_query_size_l3' in space[param_name][modal]:
                            space[param_name][modal]['result_query_size'] = space[param_name][modal].pop(
                                'result_query_size_l3')
                        if 'top_items_count_l3' in space[param_name][modal]:
                            space[param_name][modal]['top_items_count'] = space[param_name][modal].pop(
                                'top_items_count_l3')

                tag_val = json.dumps(space[param_name], ensure_ascii=False)
            file_descr.write('<{} type={}>{}</{}>\n'.format(tag_name, tag_type, tag_val, tag_name))


special_space = {
    'query_l1_modality_list': {'lemma': {'result_query_size': hp.choice('result_query_size', [7])}},
    'query_constr_ext': 0,
    'constr_coll_doccount': hp.choice('constr_coll_doccount', [100]),
    'constr_coll_softor': 0.5,
    'constr_coll_min_doc_rank': 0.0,
    'temporal': 0,
    'max_daily_answer_size': 15,
    'timeline_summary_size': 15,
    'max_sentence_len': 50,
    'min_sentence_len': 4,
    'min_mmr': 0.0,
    'lambda': 0.63,
    'importance': 0,
    'slv_w2v': 0,
}

space_config_no_ext= {
    'query_l1_modality_list': {'lemma': {'result_query_size': hp.choice('result_query_size', range(5, 15))}},
    'query_constr_ext': 0,
    'constr_coll_doccount': hp.choice('constr_coll_doccount', [x for x in range(50, 501) if x % 50 == 0]),
    'constr_coll_softor': 0.5,
    'constr_coll_min_doc_rank': 0.0,
    'temporal': 0,
    'max_daily_answer_size': 15,
    'timeline_summary_size': 15,
    'max_sentence_len': 50,
    'min_sentence_len': 4,
    'min_mmr': 0.0,
    'lambda': hp.choice('lambda', [0.4 + x * 0.01 for x in range(0, 61)]),
    'importance': 0,
    'slv_w2v': 0,
}

space_config_ext= {
    'query_l1_modality_list': {'lemma': {'result_query_size': hp.choice('result_query_size', range(5, 15))}},
    'query_l2_modality_list': {'lemma': {'result_query_size_l2': hp.choice('result_query_size_l2', range(5, 15)),
                                         'top_items_count_l2': hp.choice('top_items_count_l2', range(6, 20))}},
    'query_constr_ext': 1,
    'constr_coll_doccount': hp.choice('constr_coll_doccount', [x for x in range(50, 501) if x % 50 == 0]),
    'constr_coll_softor': 0.5,
    'constr_coll_min_doc_rank': 0.0,
    'query_ext_doccount': hp.choice('query_ext_doccount', [x for x in range(50, 151) if x % 10 == 0]),
    'query_ext_softor': 0.5,
    'query_ext_min_doc_rank': 0.0,
    'temporal': 0,
    'max_daily_answer_size': 15,
    'timeline_summary_size': 15,
    'max_sentence_len': 50,
    'min_sentence_len': 4,
    'min_mmr': 0.0,
    'lambda': hp.choice('lambda', [0.4 + x * 0.01 for x in range(0, 61)]),
    'importance': 0,
    'slv_w2v': 0,
}

space_config_double_ext= {
    'query_l1_modality_list': {'lemma': {'result_query_size': hp.choice('result_query_size', range(5, 15))}},
    'query_l2_modality_list': {'lemma': {'result_query_size_l2': hp.choice('result_query_size_l2', range(5, 15)),
                                         'top_items_count_l2': hp.choice('top_items_count_l2', range(6, 20))}},
    'query_l3_modality_list': {'lemma': {'result_query_size_l3': hp.choice('result_query_size_l3', range(7, 15)),
                                         'top_items_count_l3': hp.choice('top_items_count_l3', range(6, 20))}},
    'query_constr_ext': 1,
    'query_constr_double_ext': 1,
    'constr_coll_doccount': hp.choice('constr_coll_doccount', [x for x in range(50, 501) if x % 50 == 0]),
    'constr_coll_softor': 0.5,
    'constr_coll_min_doc_rank': 0.0,
    'query_ext_doccount': hp.choice('query_ext_doccount', [x for x in range(50, 151) if x % 10 == 0]),
    'query_ext_softor': 0.5,
    'query_ext_min_doc_rank': 0.0,
    'temporal': 0,
    'max_daily_answer_size': 15,
    'timeline_summary_size': 15,
    'max_sentence_len': 50,
    'min_sentence_len': 4,
    'min_mmr': 0.0,
    'lambda': hp.choice('lambda', [0.4 + x * 0.01 for x in range(0, 61)]),
    'importance': 0,
    'slv_w2v': 0,
}


def func_to_minimize(space):
    print("Start func...")
    print(space)

    cur_file_file_dir = os.path.dirname(__file__)

    stop_words_path = os.path.join(cur_file_file_dir, r'../data/GoldSummary/stop_words.txt')
    ref_data_path = os.path.join(cur_file_file_dir, r'../data/GoldSummary/gold1.xml')

    search_engine_name = 'elastic'
    if search_engine_name == 'elastic':
        mapping_data_path = os.path.join(cur_file_file_dir, r'../data/GoldSummary/id_to_querry_elastic.xml')
    elif search_engine_name == 'nldx':
        mapping_data_path = os.path.join(cur_file_file_dir, r'../data/GoldSummary/id_to_querry.xml')
    else:
        raise Exception('Incorrect search_engine_name')

    evaluator = TSEvaluator(lemmatization=True)
    evaluator.init_stop_words(stop_words_path)
    evaluator.init_reference_data(ref_data_path, mapping_data_path)

    eval_out_file = os.path.join(cur_file_file_dir, r'../data/temp/eval_data.xml')
    doc_partition_file_path = os.path.join(cur_file_file_dir, r'../data/GoldSummary/docs_partition.json')

    with codecs.open(doc_partition_file_path, 'r', 'utf-8') as file_descr:
        docs_ids = json.load(file_descr)[search_engine_name]['train']

    process_num = 8

    path_to_config = os.path.join(cur_file_file_dir, r'../data/temp/config.xml')
    path_to_answer = os.path.join(cur_file_file_dir, r'../data/temp/answer.xml')
    log_file = os.path.join(cur_file_file_dir, r'../data/temp/log.xml')

    generate_config(space, path_to_config)
    controller = TSController(path_to_config, log_file)
    controller.run_queries(docs_ids, path_to_answer, process_num, search_engine_name=search_engine_name)

    evaluator = TSEvaluator(lemmatization=True)
    evaluator.init_stop_words(stop_words_path)
    evaluator.init_reference_data(ref_data_path, mapping_data_path)
    evaluate_results = evaluator.evaluate(path_to_answer, mode='sent_by_sent')
    evaluator._save_results(evaluate_results, 'sent_by_sent', controller.get_config(), eval_out_file)

    r1 = evaluate_results['mean']['rouge-1']['r']
    r2 = evaluate_results['mean']['rouge-2']['r']

    score = r2 + r1
    print("End func. Score = {} r1 = {} r2 = {}".format(2 - score, r1, r2))
    return 2 - score


def my_custom_best_vals_applier(space, best):
    def _my_custom_best_vals_applier(space, best):
        for name in space:
            if isinstance(space[name], Apply):
                space[name] = [val.obj for val in space[name].pos_args[1:]][best[name]]
            elif isinstance(space[name], dict) or isinstance(space[name], list) or isinstance(space[name], tuple):
                _my_custom_best_vals_applier(space[name], best)

    best_space = space.copy()
    _my_custom_best_vals_applier(best_space, best)
    return best_space


if __name__ == '__main__':
    project_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if project_path not in sys.path:
        sys.path.append(project_path)
    from timeline.core.ts_controller import TSController
    from timeline.eval.ts_evaluator import TSEvaluator

    cur_file_file_dir = os.path.dirname(__file__)
    trial_dump = os.path.join(cur_file_file_dir, r'../data/temp/trial_dump_no_ext_28_11_18.p')
    if os.path.exists(trial_dump):
        trials = pickle.load(open(trial_dump, 'rb'))
    else:
        trials = Trials()

    for i in range(0, 1000):
        using_space = space_config_no_ext
        best = fmin(
            fn=func_to_minimize,
            space=using_space,
            algo=tpe.suggest,
            max_evals=len(trials.trials) + 1,
            trials=trials
        )

        pickle.dump(trials, open(trial_dump, "wb"))

        best_vals = my_custom_best_vals_applier(using_space, best)
        print(using_space)
        print('best result with param = {} loss = {} fmin calls = {}'.format(best_vals, min(trials.losses()), len(trials.trials)))