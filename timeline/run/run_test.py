from timeline.tests.tests import RegTest
import os

if __name__ == '__main__':
    path_to_configs = '../tests/test_data/28_10_18/configs'
    path_to_refs = '../tests/test_data/28_10_18/answers'
    docs_ids = ['10366234', '13394061', '10822682', '10482437', '10105996', '12171411', '12483331', '13197524',
                '11872175', '11768974', '11092876', '11136413', '11155970', '13142685', '12521721']

    configs = []
    for file in os.listdir(path_to_configs):
        configs.append(os.path.join(path_to_configs, file))

    answers = []
    for file in os.listdir(path_to_refs):
        answers.append(os.path.join(path_to_refs, file))

    if len(answers) != len(configs):
        raise Exception('ERROR: len(answers) != len(configs)')

    for i in range(0, len(configs)):
        reg_test = RegTest(answers[i], configs[i], docs_ids, process_num=2)
        if not reg_test.run_reg_test():
            raise Exception('ERROR: reg test {} failed'.format(configs[i]))
        else:
            print('Test {} is OK'.format(configs[i]))
