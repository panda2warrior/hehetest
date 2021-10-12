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
import multiprocessing
import pickle
import logging


def load_pickle_obj(filepath):
    with open(filepath, 'rb') as fr:
        data = pickle.load(fr)
    return data


def dump_obj_to_file(obj, filepath):
    with open(filepath, 'wb') as fw:
        pickle.dump(obj, fw)
    logging.info('successfully dumped to {}'.format(filepath))


def multiprocessing_execute_func(func, params_list):
    ''' execute one func with multiprocessing for different params, func should return True when executed.'''
    pool = multiprocessing.Pool(processes=8)
    pool_outputs = pool.map(func, params_list)
    return pool_outputs
