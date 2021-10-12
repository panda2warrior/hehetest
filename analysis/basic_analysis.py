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
import os
import pandas as pd
from preprocessing.general_tools import multiprocessing_execute_func


def filter_positive_samples():
    '''
    basic strategy: 1. filter 5m data, by percent of last 5 days.
    '''
    file_dir = '../data/data_by_code/'
    m5_filenames = [x if '5m' in x for x in os.listdir(file_dir)]
    m5_filepaths = [file_dir + x for x in m5_filenames]


def statistic_analysis():
    '''
    Some statistic experiments.
    '''
    datadir = '../data/data_by_code/'
    filepaths = [datadir + x for x in os.listdir(datadir) if '_1d' in x]

    def analysis_func(filename):
        '''returned from dict {name: output}'''
        from ..preprocessing.normalize_module import normalize_daily_price_dataframe
        from ..preprocessing.normalize_module import adding_pct_change_columns
        original_df = normalize_daily_price_dataframe(filename, if_write=False)
        adding_pct_df = adding_pct_change_columns(original_df, interval_lists=[1])
        adding_pct_df.fillna(1e-10)
        #todo




if __name__ == '__main__':
    statistic_analysis()

