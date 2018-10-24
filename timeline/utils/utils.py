import codecs
import re
import json
import datetime


class ConfigReader:
    def __init__(self, path_to_config):
        self.m_ConfigPath = path_to_config
        self.m_Params = dict()
        self._read_config(self.m_ConfigPath)

    def print_config(self):
        print('---------------------')
        print('config_len={}'.format(len(self.m_Params)))
        for elem in self.m_Params:
            print('key={} val={}'.format(elem, self.m_Params[elem]))
        print('---------------------')

    def _read_config(self, path):
        with codecs.open(path, 'r') as file_descr:
            params_lines = file_descr.readlines()

        re_tag = re.compile(r'<(.+?) type=(.+?)>(.*?)</(.+?)>', re.DOTALL)
        for line in params_lines:
            searh_res = re.search(re_tag, line)
            if searh_res is not None and searh_res.group(1) == searh_res.group(4):
                tag = searh_res.group(1)
                val_type = searh_res.group(2)
                str_val = searh_res.group(3)
                if val_type == 'int':
                    val = int(str_val)
                elif val_type == 'float':
                    val = float(str_val)
                elif val_type == 'str':
                    val = str_val
                elif val_type == 'json_dict':
                    val = json.loads(str_val)
                else:
                    continue

                self.m_Params[tag] = val

    def __getitem__(self, item):
        return self.m_Params.get(item, None)


def get_document_int_time(document, min_val='day'):
    date_str = document.get_meta_data('DATE')
    day = date_str[0:2]
    month = date_str[3:5]
    year = date_str[6:10]
    hour = date_str[11:13]
    minute = date_str[14:16]
    sec = date_str[17:19]

    time_data = [('sec', 1, sec), ('minute', 60, minute), ('hour', 60, hour), ('day', 24, day),
                 ('month', 31, month), ('year', 365, year)]

    mult = -1
    full_val = 0
    for val in time_data:
        if mult > 0:
            mult *= val[1]
            full_val += mult * int(val[2])
        if val[0] == min_val:
            mult = 1
            full_val += mult * int(val[2])

    if mult < 0:
        raise Exception('ERROR: time info gen error')

    return full_val


def get_sentence_int_time(sentence, min_val='sec'):
    sentence_doc = sentence.get_parent_doc()
    return get_document_int_time(sentence_doc, min_val)


class SimpleTimer:
    def __init__(self, func_name):
        self.m_FuncName = func_name
        self.m_StartTime = datetime.datetime.now()

    def __del__(self):
        self.m_EndTime = datetime.datetime.now()
        total_second = (self.m_EndTime - self.m_StartTime).total_seconds()
        print('Process function={} for {}s'.format(self.m_FuncName, total_second))