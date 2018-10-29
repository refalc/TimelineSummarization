import codecs
import string
import nltk
import pymorphy2
import re
from rouge import Rouge
from ..core.ts_controller import TSController
import datetime


class TSEvaluator:
    def __init__(self, lemmatization=True):
        self.m_StopWords = set()
        self.m_Morph = pymorphy2.MorphAnalyzer()
        self.m_HashedMorph = dict()
        self.m_Query2Story = None
        self.m_RefData = None
        self.m_Lemmatization = lemmatization

    def init_reference_data(self, ref_file_path, story2query_mapping_file):
        self.m_Query2Story = self._read_mapping_file(story2query_mapping_file)
        self.m_RefData = self._parse_file(ref_file_path, lemmatization=self.m_Lemmatization)

    def init_stop_words(self, path_to_stop_word_file=None):
        self.m_StopWords = set(nltk.corpus.stopwords.words('russian'))
        if path_to_stop_word_file is not None:
            with codecs.open(path_to_stop_word_file, 'r', 'utf-8') as file_descr:
                lines = file_descr.readlines()
                for line in lines:
                    word = line.replace('\n', '').replace('\r', '').lower()
                    self.m_StopWords.add(word)

    def run_and_evaluate(self, path_to_config, doc_ids, answer_path, evaluate_out_path, process_num=1,
                         mode='sent_by_sent'):
        controller = TSController(path_to_config)
        res = controller.run_queries(doc_ids, answer_path, process_num)
        if res:
            evaluate_results = self.evaluate(answer_path, mode=mode)
            self._save_results(evaluate_results, mode, controller.get_config(), evaluate_out_path)

    @staticmethod
    def _save_results(evaluate_results, mode, config, output_file):
        evaluate_results_list = sorted([[key, value] for key, value in evaluate_results.items()], reverse=True)
        for i in range(0, len(evaluate_results_list)):
            metrics_data = evaluate_results_list[i][1]
            if metrics_data is None:
                continue
            evaluate_results_list[i][1] = sorted([[key, value] for key, value in metrics_data.items()])
            for j in range(0, len(evaluate_results_list[i][1])):
                metrics_mode_data = evaluate_results_list[i][1][j][1]
                evaluate_results_list[i][1][j][1] = sorted([[key, value] for key, value in metrics_mode_data.items()])

        empty_result = '\t<rouge-1>\n\t\t<f>0.000</f>\n\t\t<p>0.000</p>\n\t\t<r>0.000</r>\n\t</rouge-1>\n' \
                       '\t<rouge-2>\n\t\t<f>0.000</f>\n\t\t<p>0.000</p>\n\t\t<r>0.000</r>\n\t</rouge-2>\n'

        with codecs.open(output_file, 'a', 'utf-8') as file_descr:
            file_descr.write('<run time={} mode={}>\n'.format(datetime.datetime.now(), mode))
            file_descr.write('<params>\n{}</params>\n'.format(config.config_to_str()))
            file_descr.write('<results>\n')
            for tag, eval_res_data in evaluate_results_list:
                file_descr.write('<tag={}>\n'.format(tag))
                if eval_res_data is None:
                    file_descr.write(empty_result)
                    continue

                for metric, metric_data in eval_res_data:
                    file_descr.write('\t<{}>\n'.format(metric))
                    for metric_mode, val in metric_data:
                        file_descr.write('\t\t<{}>{}</{}>\n'.format(metric_mode, val, metric_mode))
                    file_descr.write('\t</{}>\n'.format(metric))
                file_descr.write('</tag>\n')
            file_descr.write('</results>\n')
            file_descr.write('</run>\n')

    def _parse_file(self, path_to_file, lemmatization=True):
        answer_data = dict()
        try:
            with codecs.open(path_to_file, 'r', 'utf-8') as file_descr:
                translator = str.maketrans('', '', '\r\n')
                text = file_descr.read().translate(translator)
        except UnicodeDecodeError:
            with codecs.open(path_to_file, 'r', 'windows-1251') as file_descr:
                translator = str.maketrans('', '', '\r\n')
                text = file_descr.read().translate(translator)

        # del metadata
        text = re.sub(r'<metadata(.*?)>', '', text)

        # del querry data
        text = re.sub(r'<querries>(.*?)</querries>', '', text)
        stories = re.findall(r'(<story.*?)</story>', text)
        for story in stories:
            if re.search(r'init_doc_id=(\d*)', story):
                story_id = re.search(r'init_doc_id=(\d*)', story).group(1)
            else:
                story_id = re.search(r'story id=(\d*)', story).group(1)

            sentences = re.findall(r'(<sentence.*?)</sentence>', story)
            answer_data[story_id] = []
            for sentence in sentences:
                sent_data = re.search(r'<sentence id=(\d*)(.*?)>(.*)', sentence)
                sent_text = self._clean_text_data(sent_data.group(3), lemmatization=lemmatization)
                answer_data[story_id].append(sent_text)

        return answer_data

    @staticmethod
    def _read_mapping_file(story2query_mapping_file):
        query2story = dict()
        with codecs.open(story2query_mapping_file, "r", "utf_8") as file_descr:
            translator = str.maketrans('', '', '\r\n')
            text = file_descr.read().translate(translator)

        pairs = re.findall(r"<pair>(.*?)</pair>", text)
        for pair in pairs:
            story_id = re.search(r"<id>(\d*)</id>", pair).group(1)
            queries = re.findall(r"<doc_id>(.*?)</doc_id>", pair)
            for query in queries:
                query2story[query] = story_id

        return query2story

    @staticmethod
    def get_metric_results(ref, hypo, rouge_calculator, available_metrics):
        metric_results = rouge_calculator.get_scores(hypo, ref)[0]
        metric_results = {metric: metric_data for metric, metric_data in metric_results.items()
                          if metric in available_metrics}

        return metric_results

    def evaluate(self, hypo_file_path, mode='sent_by_sent', available_metrics={'rouge-1', 'rouge-2'}):
        hypo_file_data = self._parse_file(hypo_file_path, lemmatization=self.m_Lemmatization)

        all_metric_results = dict()
        mean_metrics = None
        rouge_calculator = Rouge()
        for query in hypo_file_data:
            metric_results = None
            hypo_sentences = hypo_file_data[query]
            ref_sentences = self.m_RefData[self.m_Query2Story[query]]
            if mode == 'overall':
                hypo_sentences = ' '.join(hypo_sentences)
                ref_sentences = ' '.join(ref_sentences)
                metric_results = self.get_metric_results(ref_sentences, hypo_sentences, rouge_calculator,
                                                         available_metrics)
            elif mode == 'sent_by_sent':
                recall_metric_results = None
                for ref_sent in ref_sentences:
                    ref_best_metric_result = None
                    for hypo_sent in hypo_sentences:
                        cur_metric_results = self.get_metric_results(ref_sent, hypo_sent, rouge_calculator,
                                                                     available_metrics)
                        ref_best_metric_result = self._get_best_metric(cur_metric_results, ref_best_metric_result)
                    recall_metric_results = self._merge_metric_results(recall_metric_results, ref_best_metric_result)
                recall_metric_results = self._devide_metric_results_by(recall_metric_results, len(ref_sentences))

                precision_metric_results = None
                for hypo_sent in hypo_sentences:
                    hypo_best_metric_result = None
                    for ref_sent in ref_sentences:
                        cur_metric_results = self.get_metric_results(ref_sent, hypo_sent, rouge_calculator,
                                                                     available_metrics)
                        hypo_best_metric_result = self._get_best_metric(cur_metric_results, hypo_best_metric_result)
                    precision_metric_results = self._merge_metric_results(precision_metric_results,
                                                                          hypo_best_metric_result)
                precision_metric_results = self._devide_metric_results_by(precision_metric_results, len(hypo_sentences))
                metric_results = {metric: {'r': 0.0, 'p': 0.0, 'f': 0.0} for metric in available_metrics}
                for metric in available_metrics:
                    recall_value = 0.0
                    precision_value = 0.0
                    f1_value = 0.0
                    if recall_metric_results is not None:
                        recall_value = recall_metric_results[metric]['r']
                    if precision_metric_results is not None:
                        precision_value = precision_metric_results[metric]['p']
                    if precision_value + recall_value > 0:
                        f1_value = 2.0 * recall_value * precision_value / (precision_value + recall_value)
                    metric_results[metric]['r'] = recall_value
                    metric_results[metric]['p'] = precision_value
                    metric_results[metric]['f'] = f1_value
            else:
                raise Exception('ERROR: incorrect mode')

            all_metric_results[query] = metric_results
            mean_metrics = self._merge_metric_results(mean_metrics, metric_results)

        mean_metrics = self._devide_metric_results_by(mean_metrics, len(hypo_file_data))
        all_metric_results['mean'] = mean_metrics

        for query in all_metric_results:
            if all_metric_results[query] is None:
                continue
            for metric in all_metric_results[query]:
                for metric_mode in all_metric_results[query][metric]:
                    rounded_val = round(all_metric_results[query][metric][metric_mode], 3)
                    all_metric_results[query][metric][metric_mode] = rounded_val

        return all_metric_results

    @staticmethod
    def _merge_metric_results(lhs, rhs):
        if lhs is None and rhs is None:
            return None
        if lhs is None:
            return rhs
        if rhs is None:
            return lhs

        merged_metric_results = lhs.copy()
        for metric in merged_metric_results:
            for mode in merged_metric_results[metric]:
                merged_metric_results[metric][mode] += rhs[metric][mode]

        return merged_metric_results

    @staticmethod
    def _devide_metric_results_by(metric_results, denominator):
        if metric_results is None:
            return None
        dev_metric_results = metric_results.copy()
        for metric in dev_metric_results:
            for mode in dev_metric_results[metric]:
                dev_metric_results[metric][mode] = dev_metric_results[metric][mode] / denominator

        return dev_metric_results

    @staticmethod
    def _get_best_metric(lhs, rhs, axis=['rouge-1', 'f']):
        if lhs is None and rhs is None:
            return None
        if lhs is None:
            return rhs
        if rhs is None:
            return lhs

        lhs_val = lhs[axis[0]][axis[1]]
        rhs_val = rhs[axis[0]][axis[1]]

        return lhs.copy() if lhs_val > rhs_val else rhs.copy()

    def _clean_text_data(self, text, lemmatization=True):
        punct_set = string.punctuation + '»«“„'
        translator = str.maketrans('', '', punct_set)
        text = text.translate(translator).lower()

        splited_text = text.split()
        splited_text = [word for word in splited_text if word not in self.m_StopWords]
        if lemmatization:
            splited_text = list(map(self._get_morph, splited_text))

        clean_text = ' '.join(splited_text)
        return clean_text

    def _get_morph(self, word):
        nword = self.m_HashedMorph.get(word, None)
        if nword is None:
            nword = self.m_Morph.parse(word)[0].normal_form
            self.m_HashedMorph[word] = nword
        return nword
