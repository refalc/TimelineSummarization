import numpy as np
import sys
from ..utils import utils
from ..utils.pagerank import powerIteration


class TSDocRepr:
    def __init__(self, init_doc):
        self.m_Doc = init_doc
        self.m_HeadSentences = None
        self.m_TailSentences = None
        self.m_Importance = 0.0
        self.m_FloatDate = self._calc_date()

    def set_importance(self, importance):
        self.m_Importance = importance

    def get_doc_id(self):
        return self.m_Doc.get_doc_id()

    def get_importance(self):
        return self.m_Importance

    def _calc_date(self):
        return utils.get_document_int_time(self.m_Doc, min_val='minute')

    def get_date(self):
        return self.m_FloatDate

    def assign_head_sentences(self, sentences):
        self.m_HeadSentences = sentences

    def assign_tail_sentences(self, sentences):
        self.m_TailSentences = sentences

    def sim(self, other_repr, w2v_enable=False):
        weight = 0.0
        for tail_sent in self.m_TailSentences:
            for head_sent in other_repr.m_HeadSentences:
                sim = tail_sent.sim_embedding(head_sent) if w2v_enable else tail_sent.sim(head_sent)
                weight = max(weight, round(sim, 3))

        return weight


class TSDocImportanceSolver:
    def __init__(self, config):
        self.m_Config = config
        self.m_W2V_model = None

    def init_w2v_model(self, w2v_model):
        self.m_W2V_model = w2v_model

    def construct_doc_importance(self, timeline_collection):
        docs_reprs = self._construct_docs_reprs(timeline_collection)
        if len(docs_reprs) == 0:
            return dict(), []

        sim_doc_matrix = self._create_sim_doc_matrix(docs_reprs)
        scores = powerIteration(sim_doc_matrix)
        max_val = max(scores)
        min_val = 1.0
        for score in scores:
            if score > sys.float_info.epsilon:
                min_val = min(min_val, score)

        diff_val = max_val - min_val
        if diff_val > 0:
            for i in range(0, len(docs_reprs)):
                scores[i] = (scores[i] - min_val) / diff_val
                docs_reprs[i].set_importance(scores[i])

            docs_reprs = sorted(docs_reprs, key=lambda doc_repr: -doc_repr.get_importance())

        top_docs = [top_doc_repr.get_doc_id() for top_doc_repr in docs_reprs
                    if top_doc_repr.get_importance() > self.m_Config['di_boundary']]
        docid2importance = {doc_repr.get_doc_id(): doc_repr.get_importance() for doc_repr in docs_reprs}

        return docid2importance, top_docs

    def _construct_docs_reprs(self, timeline_collection):
        docs_reprs = []
        for time, coll in timeline_collection.iterate_collections():
            for doc in coll.iterate_docs():
                docs_reprs.append(self._construct_doc_repr(doc))

        docs_reprs = sorted(docs_reprs, key=lambda d: (d.get_date(), d.get_doc_id()))
        return docs_reprs

    def _construct_doc_repr(self, document):
        doc_repr = TSDocRepr(document)

        head_docs_sent = self.m_Config['di_head_sent_num_doc_repr']
        tail_docs_sent = self.m_Config['di_tail_sent_num_doc_repr']

        doc_sentences = [sent for sent in document.sentence_iter()]

        head_sentences = []
        tail_sentence = []
        sent_min_len = self.m_Config['min_sentence_len']
        sent_max_len = self.m_Config['max_sentence_len']
        w2v_enable = bool(self.m_Config['di_w2v'])
        i = 0
        while len(head_sentences) < head_docs_sent and i < len(doc_sentences):
            sent = doc_sentences[i]
            if sent.get_index('ЛЕММА') is not None and sent_min_len < sent.get_sent_len() < sent_max_len:
                if w2v_enable:
                    sent.get_index('ЛЕММА').construct_index_embedding(self.m_W2V_model)
                head_sentences.append(sent)
            i += 1

        i = len(doc_sentences) - 1
        while len(tail_sentence) < tail_docs_sent and i >= 0:
            sent = doc_sentences[i]
            if sent.get_index('ЛЕММА') is not None and sent_min_len < sent.get_sent_len() < sent_max_len:
                if w2v_enable:
                    sent.get_index('ЛЕММА').construct_index_embedding(self.m_W2V_model)
                tail_sentence.append(sent)
            i -= 1

        doc_repr.assign_head_sentences(head_sentences)
        doc_repr.assign_tail_sentences(tail_sentence)

        return doc_repr

    def _create_sim_doc_matrix(self, coll_doc_reprs):
        docs_reprs_size = len(coll_doc_reprs)
        sim_doc_matrix = np.zeros([docs_reprs_size] * 2)
        w2v_enable = bool(self.m_Config['di_w2v'])
        min_link_score = self.m_Config['di_min_link_score']
        for i in range(1, docs_reprs_size):
            for j in range(0, i):
                sim = coll_doc_reprs[i].sim(coll_doc_reprs[j], w2v_enable)
                if sim > min_link_score:
                    sim_doc_matrix[i][j] = sim
        return sim_doc_matrix




