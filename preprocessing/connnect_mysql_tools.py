# -*- coding: utf-8 -*-

# Copyright 2018-2019 Leiming Du Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

import pymysql
import time
import pandas as pd

def split_tables_by_code(tablename='30m_stock_price_unique',
    column_name=['time', 'code', 'open', 'close', 'high', 'low', 'volumn', 'money'], file_tip='_30m'):
    db = pymysql.connect(host='localhost', user='root', password='duleiming', database='stock')

    cursor = db.cursor()

    # Let's get stock name first
    cursor.execute('select distinct code from {}'.format(tablename))
    code_set = [x[0] for x in cursor.fetchall()]

    for single_code in code_set:
        if single_code is None or single_code == 'None':
            continue
        start_time = time.time()
        print('processing {}'.format(single_code))
        filename = '../data/data_by_code/{}.csv'.format(single_code+file_tip)
        sql_sentence = 'select * from {} where code=\'{}\''.format(tablename, single_code)
        cursor.execute(sql_sentence)
        single_code_list = cursor.fetchall()

        single_code_df = pd.DataFrame(data=single_code_list, columns=column_name).drop_duplicates()
        single_code_df.to_csv(filename, index=False, header=True)
        print('Have already written {}...'.format(filename))

if __name__ == '__main__':
    # # split 30m data
    # split_tables_by_code()
    #
    # # split 5m data
    # split_tables_by_code(tablename='5m_stock_price_unique',
    #                      column_name=['time', 'code', 'open', 'close', 'high', 'low', 'volumn', 'money'],
    #                      file_tip='_5m')
    #
    # split 1d data
    split_tables_by_code(tablename='daily_stock_price_unique',
                         column_name=['index', 'time', 'code', 'open', 'close', 'high', 'low', 'volumn', 'money',
                                      'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused', 'open_interest'],
                         file_tip='_1d')

