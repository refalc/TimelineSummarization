from timeline.core.ts_controller import TSController


if __name__ == '__main__':
    path_to_config = r'C:\Users\MishaDEV\Data\config_double_ext_temp_imp.xml'
    controller = TSController(path_to_config)
    docs_ids = ['10366234', '13394061', '10822682', '10482437', '10105996', '12171411', '12483331', '13197524',
                '11872175', '11768974', '11092876', '11136413', '11155970', '13142685', '12521721']
    #docs_ids = ['10366234', '10822682']
    #docs_ids = ['11092876']
    answer_file = 'C:\\!DEV\\C++\\TemporalSummarization\\TemporalSummarizationVS\\Data\\answer_2.xml'
    controller.run_queries(docs_ids, answer_file)
    '''
    answer_files = ['C:\\!DEV\\C++\\TemporalSummarization\\TemporalSummarizationVS\\Data\\answer_4.xml',
                    'C:\\!DEV\\C++\\TemporalSummarization\\TemporalSummarizationVS\\Data\\answer_5.xml',
                    'C:\\!DEV\\C++\\TemporalSummarization\\TemporalSummarizationVS\\Data\\answer_6.xml']
    for answer in answer_files:
        controller.run_queries(docs_ids, answer)
    '''
    #query = controller._construct_query(doc_id)
    #timeline_collection = controller._construct_collection(query)
    #timeline_queries = controller._construct_timeline_queries(query, timeline_collection)
    #timeline_summary = controller._construct_timeline_summary(timeline_queries, timeline_collection)
    #for sent_pair in timeline_summary:
        #print('mmr={}\n\t{}'.format(sent_pair[1], sent_pair[0].get_meta_data('raw_view')))
    #print(timeline_summary)
