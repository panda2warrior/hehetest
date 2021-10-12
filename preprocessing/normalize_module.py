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
import time
import os
os.environ["MODIN_ENGINE"] = "ray"
import pandas as pd
import numpy as np
import pickle
import logging


def normalize_daily_price_dataframe(raw_df_filepath, if_write=False):
    '''
    read file from mysql, drop repeated and nas, preprocess and output normalized dataframe.
    '''
    daily_stock_price_columns = ['index', 'time', 'code', 'open', 'close', 'low', 'high', 'volume', 'money', 'factor',
                                 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused', 'open_interest']
    daily_stock_price_column_dtype = {
        'index': float, 'time': str, 'code': str, 'open': float, 'close': float, 'low': float, 'high': float,
        'volume': float, 'money': float, 'factor': float, 'high_limit': float, 'low_limit': float, 'avg': float,
        'pre_close': float, 'paused': str, 'open_interest': str
    }

    df = pd.read_csv(raw_df_filepath, header=None, parse_dates=['time'], na_values='\\N',
                     names=daily_stock_price_columns, dtype=daily_stock_price_column_dtype)
    df = df.fillna(1e-10)
    drop_list = ['index']
    df = df.drop(labels=drop_list, axis=1)  # drop unrelated columns

    df = df.drop_duplicates() # drop repeated rows

    if if_write:
        df.to_csv(raw_df_filepath+'.normalized.csv.gzip', index=False, header=True)
        logging.info('successfully normalized file {}...'.format(raw_df_filepath))
    return df


def adding_pct_change(df_filepath):
    '''
    add pct change to the dataframe.
    '''
    df = pd.read_csv(df_filepath, header=0, low_memory=False)
    # print(df)
    interval_list = [int(i) for i in range(1, 10)]  # get change in one month
    df = df.groupby('code').apply(lambda x: adding_pct_change_columns(x, interval_list)).reset_index(drop=True)
    df.to_csv(df_filepath+'.add_pct.gzip', index=False, header=True)
    return df


def adding_pct_change_columns(original_df, interval_lists=[1]):
    original_df = original_df.sort_values(by='time', ascending=True)
    for x in interval_lists:
        original_df['{}_pct'.format(x)] = original_df['close'].pct_change(periods=int(x)) * 100
    return original_df.drop('code', axis=1)

