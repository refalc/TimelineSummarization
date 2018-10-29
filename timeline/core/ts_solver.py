from .ts_primitives import *
from ..utils import utils


class TSSolver:
    def __init__(self, config):
        self.m_Config = config
        self.m_SavedSim = dict()
        self.m_W2V_model = None

    def init_w2v_model(self, w2v_model):
        self.m_W2V_model = w2v_model

    def construct_timeline_summary(self, timeline_queries, timeline_collection, sort_by_time=True):
        w2v_enable = bool(self.m_Config['slv_w2v'])
        if w2v_enable:
            for time, query in timeline_queries.iterate_queries():
                query.get_index('ЛЕММА').construct_index_embedding(self.m_W2V_model)

        max_daily_answer_size = self.m_Config['max_daily_answer_size']
        all_extracted = []
        for time, collection in timeline_collection.iterate_collections_by_time():
            current_query = timeline_queries.get_query_for_time(time)
            sentences = self._get_sentences_from_collection(collection, w2v_enable=w2v_enable)

            today_extracted = []
            max_today_sentences = min(max_daily_answer_size, len(sentences))
            while len(today_extracted) < max_today_sentences:
                top_sentence_info = self._extract_top_sentence(sentences, current_query, all_extracted, today_extracted,
                                                               w2v_enable=w2v_enable)
                if top_sentence_info is None:
                    break
                sentences = top_sentence_info[1]
                today_extracted.append(top_sentence_info[0])
                if len(sentences) == 0:
                    break

            all_extracted += today_extracted

        all_extracted = sorted(all_extracted, key=lambda x: -x[1])
        timeline_summary_size = self.m_Config['timeline_summary_size']
        result_summary_size = min(len(all_extracted), timeline_summary_size)

        result_summary_sentences = all_extracted[:result_summary_size]
        if sort_by_time:
            result_summary_sentences = sorted(result_summary_sentences, key=lambda x: (utils.get_sentence_int_time(
                x[0], min_val='minute'), x[0].get_label(), x[0].get_sentence_id()))
        return result_summary_sentences

    def _extract_top_sentence(self, sentences, query, all_extracted, today_extracted, w2v_enable=False):
        min_mmr = self.m_Config['min_mmr']

        # 1) rank sentences
        ranked_sentences = []
        for sent in sentences:
            sent_score = self._rank_one_sentence(sent, query, all_extracted, today_extracted, w2v_enable=w2v_enable)
            ranked_sentences.append((sent, sent_score))

        # 2) extract top sentence
        ranked_sentences = sorted(ranked_sentences,
                                  key=lambda x: (-x[1], utils.get_sentence_int_time(x[0], 'minute'),
                                                 x[0].get_label(), x[0].get_sentence_id()))

        #print(ranked_sentences)
        top_sentence_pair = ranked_sentences[0]
        if top_sentence_pair[1] < min_mmr:
            return None

        # 3) erase sentences with < min_mmr and = top_sentence
        erase_sentence_set = set()
        erase_sentence_set.add(top_sentence_pair[0])
        for sent_pair in ranked_sentences[1:]:
            if sent_pair[1] < min_mmr:
                erase_sentence_set.add(sent_pair[0])

        sentences = [sent for sent in sentences if sent not in erase_sentence_set]
        return top_sentence_pair, sentences

    def _calc_sim(self, lhs, rhs, w2v_enable=False):
        key = (lhs.get_label(), rhs.get_label())
        sim = self.m_SavedSim.get(key, None)
        if sim is None:
            sim = lhs.sim_embedding(rhs) if w2v_enable else lhs.sim(rhs)
            self.m_SavedSim[key] = sim
        return sim

    def _rank_one_sentence(self, sentence, query, all_extracted, today_extracted, w2v_enable=False):
        lambda_val = self.m_Config['lambda']
        sim_to_query = self._calc_sim(query, sentence, w2v_enable)# query.sim(sentence)
        if self.m_Config['importance']:
            doc_importance = sentence.get_parent_doc().get_importance()
            sim_to_query *= (1.0 + self.m_Config['di_alpha'] * doc_importance)

        sim_to_extracted = 0.0
        sim_to_extracted_today = 0.0

        for sent_pair in all_extracted:
            #sim_to_extracted = max(sim_to_extracted, sentence.sim(sent_pair[0]))
            sim_to_extracted = max(sim_to_extracted, self._calc_sim(sentence, sent_pair[0], w2v_enable))
        for sent_pair in today_extracted:
            #sim_to_extracted_today = max(sim_to_extracted_today, sentence.sim(sent_pair[0]))
            sim_to_extracted = max(sim_to_extracted, self._calc_sim(sentence, sent_pair[0], w2v_enable))

        sim_to_other = max(sim_to_extracted, sim_to_extracted_today)
        sentence_num_penality = 1.0 - 0.25 * math.sin(
            sentence.get_sentence_id() / sentence.get_parent_doc().get_sentences_num())
        score = sentence_num_penality * lambda_val * sim_to_query - (1.0 - lambda_val) * sim_to_other

        return round(score, 3)

    def _get_sentences_from_collection(self, collection, w2v_enable=False):
        sentences = []
        for doc in collection.iterate_docs():
            for sent in doc.sentence_iter():
                if sent.get_index('ЛЕММА') is None:
                    continue
                if self.m_Config['max_sentence_len'] > len(sent.get_index('ЛЕММА').get_index_data()) > \
                   self.m_Config['min_sentence_len']:
                    if w2v_enable:
                        sent.get_index('ЛЕММА').construct_index_embedding(self.m_W2V_model)
                    sentences.append(sent)
        return sentences