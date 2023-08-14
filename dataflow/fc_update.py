# -*- coding: utf-8 -*-

from pyspark.sql import functions as F
from utils import flatList, read_json, save_json, show_cosed_time
import os
import json
import copy
import logging

# 配置logger并设置等级为DEBUG
logger = logging.getLogger('logging_debug')
logger.setLevel(logging.INFO)

# 配置控制台Handler并设置等级为DEBUG
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)

logger.addHandler(consoleHandler)


def load_tfrecord(spark, tfrecode_save_path, dt_range: list):
    data_df = None
    for dt in dt_range:
        path_tmp = os.path.join(tfrecode_save_path, dt)
        data_tmp = spark.read.format("tfrecord").option("codec", "org.apache.hadoop.io.compress.GzipCodec").load(
            path_tmp)
        if data_df:
            data_df = data_df.union(data_tmp)
        else:
            data_df = data_tmp
    return data_df


def collect_vocab(partitions, args):
    tmpresult = {}
    for index_, rows in enumerate(partitions):
        for column_name, group_name, sep, maxlen, _ in args:
            if sep is None:
                key = rows[column_name]
                if key is not None:
                    tkey = (group_name, key)
                    if tkey in tmpresult:
                        tmpresult[tkey] += 1
                    else:
                        tmpresult[tkey] = 1
            else:
                key_sep = rows[column_name]
                if key_sep is not None:
                    if isinstance(key_sep, str):
                        key_list = key_sep.split(sep)
                    elif isinstance(key_sep, list):
                        key_list = key_sep
                    else:
                        raise Exception('column_name:{} type must be str/list'.format(column_name))
                    if len(key_list) > maxlen:
                        key_list = key_list[:maxlen]
                    for key in key_list:
                        tkey = (group_name, key)
                        if tkey in tmpresult:
                            tmpresult[tkey] += 1
                        else:
                            tmpresult[tkey] = 1

    for kk, vv in tmpresult.items():
        yield (kk, vv)


def filter_min_frequency(xx, args):
    name = xx[0][0]
    for _, group_name, _, _, min_frequency in args:
        if name == group_name:
            if xx[1] >= min_frequency:
                return True
            return False
    return False


class FCUpdate:
    def __init__(self, config_map):

        if isinstance(config_map, dict):
            conf = config_map
        else:
            conf = read_json(config_map)
            assert conf

        # self.default_sparse_embedding_dim =
        # self.default_dense_dimension =
        self.default_sep = ','
        # self.default_date_format =
        self.default_varlen_maxlen = 10
        self.default_vocab_filter_min_freq = 5

        self.schema = conf.get('schema')
        self.label = conf.get('label', [])
        self.features = conf.get('features')
        self.field = conf.get('field', [])

    def get_feat_names(self, features_dict):
        return list(flatList([[n for n in x.get('inputName').split(',')] for x in features_dict]))

    # 主要是找最小值，最大值，平均数等
    @show_cosed_time
    def update_dense(self, spark_df):
        keys = []
        values = []
        dense_names = {}
        for js in self.features:
            featureType = js.get('feature_column', {}).get('featureType')
            if featureType.lower() == 'dense':
                name = js.get('out')
                dense_names[name] = js
                keys.extend([name + x for x in ['_min', '_median', '_quantile95', '_max', '_mean', '_std']])
                values.append(F.expr('percentile_approx({}, array(0,0.5,0.95,1))'.format(name)))
                values.append(F.mean(name))
                values.append(F.stddev(name))

        print('dense collecting, you will be waiting a long time')
        rows = spark_df.select(values).collect()
        statistics = dict(zip(list(flatList(keys)), list(flatList(rows[0]))))

        dense_res = {}
        for name, js in dense_names.items():
            feature_column = js.get('feature_column')
            max_ = statistics.get('{}_max'.format(name))
            std_ = statistics.get('{}_std'.format(name))
            if max_ == 0 or std_ == 0:
                logger.info('warning!!!{} statistics max is {}, std_ is {}'.format(name, max_, std_))
            feature_column['statistics'] = {
                'min': statistics.get('{}_min'.format(name)),
                'median': statistics.get('{}_median'.format(name)),
                'quantile95': statistics.get('{}_quantile95'.format(name)),
                'max': max_,
                'mean': statistics.get('{}_mean'.format(name)),
                'std': std_,
            }
            dense_res[name] = feature_column
        return dense_res

    @show_cosed_time
    def update_sparse(self, spark_df):
        sparse_names = {}
        rdd_args = []
        for js in self.features:
            feature_column = js.get('feature_column', {})
            featureType = feature_column.get('featureType')
            if 'sparse' in featureType.lower():
                freeze = feature_column.get('freeze')
                if freeze:
                    continue

                name = js.get('out')

                js_copy = copy.copy(js)
                embedding_name = feature_column.get('embedding_name', name)
                sep = feature_column.get('sep', self.default_sep)
                maxlen = feature_column.get('maxlen', self.default_varlen_maxlen)
                min_frequency = feature_column.get('vocab_filter', self.default_vocab_filter_min_freq)
                rdd_args.append((name, embedding_name, sep, maxlen, min_frequency))

                feature_column = js_copy.get('feature_column')
                feature_column['embedding_name'] = embedding_name
                feature_column['sep'] = sep
                feature_column['maxlen'] = maxlen
                feature_column['min_frequency'] = min_frequency
                sparse_names[name] = feature_column
        # print('sparse_names',sparse_names)
        print('sparse collecting, you will be waiting a long time')
        frequency_tuple = spark_df.rdd.mapPartitions(lambda x: collect_vocab(x, rdd_args)) \
            .reduceByKey(lambda x, y: x + y) \
            .filter(lambda x: filter_min_frequency(x, rdd_args)) \
            .map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(lambda x, y: x + y) \
            .collect()

        sparse_res = {}
        for name, feature_column in sparse_names.items():
            embedding_name = feature_column.get('embedding_name', name)
            vocab_txt = feature_column.get('vocab_txt', None)
            vocab = feature_column.get('vocab', None)
            weights = feature_column.get('weights', None)
            # 适配不同特征emb时的的group name，默认为default group
            group_name = feature_column.get('group_name', 'default_group')

            feature_column['group_name'] = group_name

            if not vocab_txt:
                vocab = []
                for key, vocab_tuple in dict(frequency_tuple).items():
                    if key == embedding_name:
                        vocab = sorted(list(dict(vocab_tuple).keys()))
            if vocab and not vocab_txt:
                feature_column['vocab'] = vocab
                feature_column['vocabulary_size'] = len(vocab) + 1
            elif vocab_txt:
                feature_column['vocab'] = vocab_txt
            else:
                raise Exception('check {} ,{} vocab is empty'.format(name, embedding_name))

            if weights and isinstance(weights, str):
                feature_column['weights'] = weights

            sparse_res[name] = feature_column
        return sparse_res

    def update(self, spark_df):
        # 暂使用 sparse sparseVarlen dense 特征
        sparse_dict = self.update_sparse(spark_df)
        dense_dict = self.update_dense(spark_df)
        # print('sparse_dict', json.dumps(sparse_dict, indent=2))
        # print('dense_dict', json.dumps(dense_dict, indent=2))
        feature_copy = copy.copy(self.features)
        res = []
        for js in feature_copy:
            name = js.get('out')
            if name in sparse_dict:
                js['feature_column'] = sparse_dict.get(name)
                res.append(js)
            elif name in dense_dict:
                js['feature_column'] = dense_dict.get(name)
                res.append(js)
            else:
                res.append(js)
        # print(json.dumps(res, indent=2))
        return {
            "version": 2,
            "schema": self.schema,
            "label": self.label,
            "features": res,
            "field": self.field,
        }

    def save(self, spark_df, save_path):
        if os.path.exists(os.path.join(os.getcwd(), save_path)) is False:
            os.mkdir(os.path.join(os.getcwd(), save_path))
        feature_js = self.update(spark_df)
        save_json(save_path, feature_js)
        print('save done in {}'.format(save_path))


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    # 本地设置
    os.environ['SPARK_HOME'] = "C:\spark-2.3.4-bin-hadoop2.7"
    os.environ["PYSPARK_PYTHON"] = "D://anaconda//python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "D://anaconda//python.exe"

    spark = SparkSession \
        .builder \
        .appName("tes") \
        .master('local[*]') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    sc = spark.sparkContext
    # sc.setLogLevel('INFO')
    sc.setLogLevel('ERROR')

    dt = '20230801'
    spark_df = spark.read.parquet('./data/parquet/')

    # pre_params = PreParams(spark_df, config_map='./cache/push_update.json', token_map='./cache/common_map.json')
    pre_params = FCUpdate(config_map='./data/feature.json')
    pre_params.update(spark_df)

    # pre_params.update_normalization()
    # pre_params.update_map()

    # dense_res = pre_params.update_dense()
    # sparse_res = pre_params.update_sparse()
    # print(sparse_res)

    # print(pre_params.spark_df.take(1))
