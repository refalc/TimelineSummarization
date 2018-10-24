from timeline.core.ts_controller import TSController


if __name__ == '__main__':
    path_to_config = r'C:\Users\Misha\source\repos\TS_scripts\config_double_ext_temp_imp.xml'
    controller = TSController(path_to_config)
    docs_ids = ['10366234', '13394061', '10822682', '10482437', '10105996', '12171411', '12483331', '13197524',
                '11872175', '11768974', '11092876', '11136413', '11155970', '13142685', '12521721']

    #docs_ids = ['10822682']
    answer_file = r'C:\Users\Misha\source\repos\TemporalSummarizationVS\Data\answer.xml'
    controller.run_queries(docs_ids, answer_file, 4)