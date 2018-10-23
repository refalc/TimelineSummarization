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


def get_document_int_time(document):
    date_str = document.get_meta_data('DATE')
    day = date_str[0:2]
    month = date_str[3:5]
    year = date_str[6:10]
    int_time = int(day) + int(month) * 31 + int(year) * 365;
    return int_time


def get_document_float_time(document):
    date_str = document.get_meta_data('DATE')
    '''27.04.2015 12:02:17'''
    day = date_str[0:2]
    month = date_str[3:5]
    year = date_str[6:10]
    hour = date_str[11:13]
    minute = date_str[14:16]
    sec = date_str[17:19]

    float_time = int(sec) + int(minute) * 60 + int(hour) * 60 * 60 + int(day) * 60 * 60 * 24 + \
                 int(month) * 60 * 60 * 24 * 31 + int(year) * 60 * 60 * 24 * 31 * 365
    return float_time


def get_sentence_float_time(sentence):
    sentence_doc = sentence.get_parent_doc()
    return get_document_float_time(sentence_doc)


class SimpleTimer:
    def __init__(self, func_name):
        self.m_FuncName = func_name
        self.m_StartTime = datetime.datetime.now()

    def __del__(self):
        self.m_EndTime = datetime.datetime.now()
        total_second = (self.m_EndTime - self.m_StartTime).total_seconds()
        print('Process function={} for {}s'.format(self.m_FuncName, total_second))