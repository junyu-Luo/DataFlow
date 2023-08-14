# -*- coding: utf-8 -*-
import os, json
import logging
import zipfile
from collections import Counter
import ast
import re
from ast_parse import get_all_name

# 配置logger并设置等级
log_level = logging.INFO
logger = logging.getLogger('logging_debug')
logger.setLevel(log_level)

# 配置控制台Handler并设置等级
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(log_level)
logger.addHandler(consoleHandler)


def show_cosed_time(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        logger.info('{} done,took {} seconds'.format(f.__name__, round(end - start, 3)))
        return result

    return wrapper


def read_json(file, encoding='utf-8', path=''):
    data = open(os.path.join(path, file), encoding=encoding).read()
    return json.loads(data)


def process_func(partitions, func, columnJs_list):
    for row in partitions:
        res = []
        for columnJs in columnJs_list:
            res.append(func(row, columnJs))
            # dtype = columnJs.get('dtype')
            # assert dtype, '{} dtype not exist'.format(columnJs.get('out'))
            # if dtype == 'float32':
            #     tmp = func(row, columnJs)
            #     assert isinstance(tmp, int) or isinstance(tmp, float), 'cheak {}'.format(columnJs.get('out'))
            #     check_str2float(tmp)
            #     res.append(float(tmp))
            # elif dtype == 'int32':
            #     tmp = func(row, columnJs)
            #     assert isinstance(tmp, int) or isinstance(tmp, float), 'cheak {}'.format(columnJs.get('out'))
            #     check_str2float(tmp)
            #     res.append(int(float(tmp)))
            # elif dtype == 'string':
            #     tmp = func(row, columnJs)
            #     res.append(str(tmp))
            # else:
            #     raise Exception('{} dtype {} not in [int32/float32/string]'.format(columnJs.get('out'), dtype))
            #     # res.append(func(row, columnJs))

        yield res


class Dataprocessor:
    def __init__(self, verbose=False):
        '''
        # 逻辑是先对rename_expr替换，再执行special_expr把整个expr更新
        Args:
            verbose: 略
        '''
        # hive/ef/py 的类型统一
        hive_ef_default_typemap = (
            ("int", "Int", 0, None),
            ("bigint", "Int", 0, None),
            ("float", "Float32", 0.0, None),
            ("double", "Float32", 0.0, None),
            ("string", "String", "", None),
            ("array<bigint>", "IntVec", [], None),
            ("array<double>", "Float32Vec", [], None),
            ("array<string>", "StringVec", [], None),
            ("map<bigint,bigint>", "MapIntInt", {}, 0),
            ("map<bigint,float>", "MapIntFloat32", {}, 0.0),
            ("map<bigint,double>", "MapIntFloat32", {}, 0.0),
            ("map<bigint,string>", "MapIntString", {}, ""),
            ("map<string,bigint>", "MapStringInt", {}, 0),
            ("map<string,float>", "MapStringFloat32", {}, 0.0),
            ("map<string,double>", "MapStringFloat32", {}, 0.0),
            ("map<string,string>", "MapStringString", {}, ""),
        )
        self.map_hive_ef = {x[0]: x[1] for x in hive_ef_default_typemap}
        self.map_ef_default = {x[1]: x[2] for x in hive_ef_default_typemap}
        self.map_hive_default = {x[0]: x[2] for x in hive_ef_default_typemap}
        self.map_hive_get_default = {x[0]: x[3] for x in hive_ef_default_typemap}
        self.map_hive_py = {x[0]: type(x[2]).__name__ for x in hive_ef_default_typemap}

        self.verbose = verbose

    def _init_config_map(self, config_map):
        if isinstance(config_map, dict):
            config_map = config_map
        else:
            config_map = read_json(config_map)
            assert config_map, 'check config_map,is None or {}?'
        if self.verbose:
            logger.info('\nconfig_map:{}'.format(json.dumps(config_map, indent=2)))
        return config_map

    def _init_token_map(self, token_map):
        if token_map is None:
            token_map = {}
        elif isinstance(token_map, dict):
            token_map = token_map
        else:
            token_map = read_json(token_map)
        if self.verbose:
            logger.info('\ntoken_map:{}'.format(json.dumps(token_map, indent=2)))
        return token_map

    def _init_dataset_js(self, rename_expr, special_expr, ignore_features):

        label = self.config_map.get('label', [])
        field = self.config_map.get('field', [])
        features = self.config_map.get('features')

        dataset = []
        # py语法不能def if，使用If替换代替，c用不上
        replace_map = {
            "if(": "If(",
            "or(": "Or(",
            "and(": "And(",
            "not(": "Not(",
            "get(": "Get(",
            "max(": "Max(",
        }
        # label + features + field 不能重复
        for js in label + features + field:
            out = js.get('out')
            # 根据 ignore_features 忽略掉想不执行的特征
            if out in ignore_features:
                continue
            expr = js.get('expr')
            # rename_expr 先替换
            for k, v in rename_expr.items():
                expr = expr.replace(k, v)

            # special_expr 后替换
            for k, v in special_expr.items():
                if k == out:
                    expr = v

            # py不能def 关键词，只在py中替换，c忽略
            for k, v in replace_map.items():
                expr = expr.replace(k, v)

            # py不支持查看 map的val类型，所以对应的默认先用这不优雅但简单暴力先解决
            # 针对get算子进行的操作

            def get_args_name(string, start_numb):
                args_name = ''
                for char in string[start_numb + 4:]:
                    if char == ',':
                        break
                    args_name += char
                return args_name

            if 'Get(' in expr:
                index_list = [(substr.start(), substr.end()) for substr in re.finditer("Get\(", expr)]
                index_list.reverse()
                for start, end in index_list:
                    args = get_args_name(expr, start)
                    default = self.map_hive_get_default.get(self.dtypes_map.get(args))
                    expr = expr[:start] + 'Get({},'.format(default) + expr[end:]

            js.update({"expr": expr})
            dataset.append(js)
        if self.verbose:
            logger.info('\ndataset:{}'.format(json.dumps(dataset, indent=2)))
        return dataset

    def _init_expr_map(self):
        expr_map = {}
        all_func = []
        all_args = []
        for js in self.dataset:
            expr = js.get('expr')
            func_name, args_name = get_all_name(expr)
            expr_map[expr] = args_name
            all_func.extend(func_name)
            all_args.extend(args_name)
        all_func_name = list(set(all_func))
        all_args_name = list(set(all_args))
        if self.verbose:
            logger.info('\nexpr_map:{}'.format(json.dumps(expr_map, indent=2)))
        return expr_map, all_func_name, all_args_name

    def _init_op_str(self, token_map, custom_func):
        # 读算子的py文件
        abs_dir = os.path.dirname(os.path.abspath(__file__))
        if len(abs_dir.split('rcmd_utils.zip')) == 2:
            # 在k8s内路径是'/data/BDP/spark/data/spark-e177c230-9dfa-475e-9085-0dbd083df3b9/userFiles-9af5d410-5d87-4eb4-a680-b465a40c055c/rcmd_utils.zip
            current_dir, zip_dir = os.path.dirname(os.path.abspath(__file__)).split('rcmd_utils.zip')
            zip_file = zipfile.ZipFile(os.path.join(current_dir, 'rcmd_utils.zip'))
            op_dir = os.path.join(zip_dir[1:], 'Operator.py')
            file_content = zip_file.read(op_dir).decode('utf-8')
            op_str = file_content
        else:
            op_dir = os.path.join(abs_dir, 'Operator.py')
            op_str = open(op_dir, encoding='utf-8').read()

        if token_map:
            op_str = op_str.replace('token_map = {}', 'token_map = {}'.format(token_map))
        else:
            op_str = op_str

        if custom_func:
            op_str += '\n\n\n' + custom_func

        try:
            exec(op_str)
        except Exception as e:
            raise Exception('init op str error:{}'.format(e))

        return op_str, op_dir

    def _check_op(self, expr_func, out):
        # 校验算子是否存在
        # if set(expr_func) < self.all_operator is False:
        #     for error_func in set(expr_func).difference(self.all_operator):
        #         raise Exception('check {},{} is not exist'.format(out, error_func))
        pass

    def _check_columns(self):
        # 校验columns是否重复
        columns = self.get_column()
        if len(columns) != len(set(columns)):
            for duplicate_column in [key for key, value in dict(Counter(columns)).items() if value > 1]:
                raise Exception('out: {} is duplicate'.format(duplicate_column))

    def get_column(self):
        return [x.get('out') for x in self.dataset]

    def get_dtypes_map(self, spark_df):
        return dict(spark_df.dtypes)

    def _type_format(self, k, v):
        if isinstance(v, str):
            try:
                if '"' not in v and "'" not in v:
                    return "{}=b'{}'.decode('utf-8')".format(k, str(v.encode())[2:-1])
                elif '"' in v and "'" not in v:
                    return "{}=b'{}'.decode('utf-8')".format(k, str(v.encode())[2:-1])
                elif "'" in v and '"' not in v:
                    return '{}=b"{}".decode("utf-8")'.format(k, str(v.encode())[2:-1])
                else:
                    return "{}=b'{}'.decode('utf-8')".format(k, str(v.replace("'", '"').encode())[2:-1])
            except:
                return ''
        else:
            return "{}={}".format(k, v)

    def Conf2Func(self, row, js):

        out = js.get('out')
        expr = js.get('expr')
        consts = js.get('consts', {})
        dtype = js.get('feature_column').get('dtype')
        assert dtype is not None

        exec(self.compile_obj)

        if self.verbose:
            print('out:{}'.format(out))
            print('row:{}'.format(row.asDict()))
            print('consts:{}'.format(consts))
            print('expr:{}'.format(expr))

        for args in self.expr_map.get(expr):
            if args in row:
                val = row[args]
                if val is None:
                    if args in self.schema_default.keys():
                        val = self.schema_default.get(args)
                    else:
                        type_ = self.dtypes_map.get(args)
                        default = self.map_hive_default.get(type_)
                        val = default
                exec(self._type_format(args, val))

            elif args in consts:
                val = consts[args]
                exec(self._type_format(args, val.get('value')))

            else:
                # init 自检，应该不会到这里
                print('{},{} not found'.format(out, args))

        try:
            res = eval("{}".format(expr))
        except Exception as e:
            raise Exception("{}:{}\nexpr:{}".format(out, e, expr))
        if self.verbose:
            print('result:{}'.format(out))
            print('*' * 50)

        # print(out,res,type(res),dtype)
        if isinstance(res, int) and dtype == 'float32':
            if self.verbose:
                print('{}强转float类型'.format(out))
            res = float(res)

        if isinstance(res, float) and dtype == 'int':
            if self.verbose:
                print('{}强转int类型'.format(out))
            res = int(res)

        if isinstance(res, int) and dtype == 'string':
            if self.verbose:
                print('{}强转了string类型'.format(out))
            res = str(res)

        return res

    def fit(self, persona_df, config_map, token_map={}, rename_expr={}, special_expr={}, schema_default={},
            ignore_features=[], custom_func="", repartition_numb=256):
        '''
        Args:
            persona_df: spark.DataFrame 输入数据

            config_map: feature.json

            token_map: token_map.json

            rename_expr:
            对expr内容进行替换，有先后顺序，比如
            1.在实时取曝光时间为 now_ts()，离线hive中数据为exp_timestamp。可用{"now_ts()":"exp_timestamp"}
            2.离线跟tml注册的名称不一致，
                hive中字段名为 i_room_kaihei_female_user_cnt。
                但tml注册了i_p_enter_female_cnt_now，
                可用{"i_p_enter_female_cnt_now":"i_room_kaihei_female_user_cnt"}

            special_expr:
            用于兼容线上线下不一致的情况，
                如线上的expr为div(sub(now_ts(),u_user_kaihei_age),x)，
                hive直接拿到字段 u_user_age，
            在rename_expr配置{"div(sub(now_ts(),u_user_kaihei_age),x)":"u_user_age"}也是ok的，但有顺序要求，
            若{"now_ts()":"exp_timestamp"}在此之前，就会失效，因此这个参数是把expr更新
            注：写法是{"out":"expr"}，例：{"u_user_kaihei_age":"u_user_age"}

            schema_default:string类型默认值为 "" 空字符串，若想修改可在这设置

            ignore_features:
            离线开发会有部分特征已经算好（如：历史遗留，开发新模型等原因），需要暂时忽略feature.json中features的一些expr

            custom_func:
                py自定义函数，str输入，例如
                def my_func(x):
                    return 0

            repartition_numb: exactly numPartitions partitions
        Returns:
            返回包含所有特征列和回填的无关列（不包括画像列）
        '''
        self.dtypes_map = self.get_dtypes_map(persona_df)
        self.config_map = self._init_config_map(config_map)
        self.token_map = self._init_token_map(token_map)
        self.schema_default = schema_default
        self.dataset = self._init_dataset_js(rename_expr, special_expr, ignore_features)
        self.expr_map, self.all_func_name, self.all_args_name = self._init_expr_map()
        self._check_columns()

        # 读算子的py文件
        self.op_str, self.op_dir = self._init_op_str(token_map, custom_func)
        self.compile_obj = compile(ast.parse(self.op_str), self.op_dir, 'exec')
        try:
            # 测试下编译是否成功
            exec(self.compile_obj)
        except Exception as e:
            raise Exception(e)

        spark_RDD = persona_df.rdd.repartition(repartition_numb).mapPartitions(
            lambda x: process_func(x, self.Conf2Func, self.dataset))
        return spark_RDD

    def gen_schema(self):
        from pyspark.sql.types import StructType, StructField
        from pyspark.sql.types import StringType, IntegerType, FloatType, ArrayType

        schemas = []
        for js in self.dataset:
            name = js.get('out')
            feature_dtype = js.get('feature_column').get('dtype')
            assert feature_dtype is not None
            dtype = feature_dtype.lower()

            if dtype == 'int' or dtype == 'int32':
                dataType = IntegerType()
            elif dtype == 'str' or dtype == 'string':
                dataType = StringType()
            elif dtype == 'float' or dtype == 'float32':
                dataType = FloatType()
            elif dtype == 'vector' or dtype == 'array' or dtype == 'list':
                dataType = ArrayType(FloatType())

            else:
                raise Exception('{} dtype must in [int/float/string/list]'.format(name))
            try:
                schemas.append(
                    StructField(
                        name=name,
                        dataType=dataType,
                        nullable=False
                    )
                )
            except Exception as e:
                raise Exception('{} dtype can not accept,info: {}'.format(name, e))
        return StructType(schemas)

    def createDF(self, spark, spark_df, config_map, token_map={}, rename_expr={}, special_expr={}, schema_default={},
                 ignore_features=[], custom_func="", repartition_numb=256):
        data = self.fit(spark_df, config_map=config_map, token_map=token_map,
                        rename_expr=rename_expr, special_expr=special_expr, schema_default=schema_default,
                        ignore_features=ignore_features, custom_func=custom_func, repartition_numb=repartition_numb)
        itemschema = self.gen_schema()
        data = spark.createDataFrame(data, schema=itemschema)
        return data

    @show_cosed_time
    def saveAsParquet(self, data_df, savepath):
        data_df.write.parquet(savepath, 'overwrite')
        logger.info('save hdfs dir done,path is {}'.format(savepath))

    @show_cosed_time
    def saveAsTFrecord(self, data_df, savepath, use_gzip=True):
        if use_gzip:
            data_df.write.format("tfrecord").option("codec", "org.apache.hadoop.io.compress.GzipCodec").mode(
                "overwrite").save(savepath)
        else:
            data_df.write.format("tfrecords").option("recordType", "Example").mode("overwrite").save(savepath)
        logger.info('save hdfs dir done,path is {}'.format(savepath))

    @show_cosed_time
    def saveAsText(self, spark_df, savepath, repartition_numb=256, ):
        # 数据集 可从配置文件中 找header、类型
        # 若path已经存在会报错
        self.fit(spark_df, repartition_numb).saveAsTextFile(savepath)
        logger.info('save hdfs dir done,path is {}'.format(savepath))

    def get_df(self, spark_df, repartition_numb=1):
        # 适合较小数据集，测试
        return self.fit(spark_df, repartition_numb).toPandas()  # , del_column=False


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    # 本地设置
    os.environ['SPARK_HOME'] = "C:\spark-2.3.4-bin-hadoop2.7"
    os.environ["PYSPARK_PYTHON"] = "D://anaconda//python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "D://anaconda//python.exe"

    # os.environ['SPARK_HOME'] = "G:\environment\spark-3.1.2-bin-hadoop3.2"
    # os.environ["PYSPARK_PYTHON"] = "D:\Anaconda3\python.exe"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda3\python.exe"

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

    dt = '20230420'
    spark_df = spark.read.parquet('./data/dt=20230420/')

    config_map = './data/feature.json'

    rename_expr = {
        # 当前时间
        "div(sub(now_ts(),u_user_birthday),YEAR2SEC)": "u_user_age",
        "div(sub(now_ts(),i_user_birthday),YEAR2SEC)": "i_user_age",
        "now_ts()": "e_algo_kaihei_exp_timestamp",
        "weekday()": "e_algo_kaihei_day_of_week",

        # 其他
        "i_room_kaihei_publish_time": "i_room_kaihei_publish_timestamp",
        "u_room_kaihei_click_cnt_3d": "u_room_kaihei_click_count_3d",
        "ii_room_kaihei_game_id_browse_cnt_3d": "ii_p_room_kaihei_game_id_browse_cnt_3d",
        "ii_room_kaihei_game_id_click_cnt_3d": "ii_p_room_kaihei_game_id_click_cnt_3d",
        "ii_room_kaihei_game_id_enter_cnt_7d": "ii_p_room_kaihei_game_id_enter_cnt_7d",
        "ii_room_kaihei_game_id_gangup_cnt_7d": "ii_p_room_kaihei_game_id_gangup_cnt_7d",
        "ii_room_kaihei_game_id_mic_cnt_7d": "ii_p_room_kaihei_game_id_mic_cnt_7d",
        "ui_algo_kaihei_pref_tag_id_tfidf_map_2w": "iu_algo_kaihei_pref_tag_id_tfidf_2w",
        "ui_room_kaihei_tag_tfidf_28d": "ui_room_kaihei_tag_tfidf_map_28d",

    }

    special_expr = {
        "u_user_kaihei_roomname_tfidf_sum": "sum(gets(ui_room_kaihei_tag_tfidf_map_28d,i_algo_kaihei_room_name_tag_list))",
        "u_user_kaihei_roompara_tfidf_sum": "sum(gets(ui_room_kaihei_tag_tfidf_map_28d,i_algo_kaihei_room_para_tag_list))",
        # find_map_values
        "ui_room_kaihei_roomname_tfidf_cnt": "len(find_map_values(ui_room_kaihei_tag_tfidf_map_28d,i_algo_kaihei_room_name_tag_list))",
        "ui_room_kaihei_roomname_tfidf_max": "max(gets(ui_room_kaihei_tag_tfidf_map_28d,i_algo_kaihei_room_name_tag_list))",
        "u_user_kaihei_select_gender": "str(if(if_equal(get_dflt(select_info,'性别','不限'),'不限性别'),'不限',get_dflt(select_info,'性别','不限')))",
        "ui_user_gangup_roompara_tag_seq": "ui_algo_kaihei_room_para_tag_seq",
        "ui_user_gangup_roomname_tag_seq": "ui_algo_kaihei_room_name_tag_seq",
        "ui_user_kaihei_enter_roompara_tag_list": "ui_user_enter_roompara_tag_seq",
        "ui_user_kaihei_enter_roomname_tag_list": "ui_user_enter_roomname_tag_seq",
        "i_room_kaihei_roompara_topic": "str(get_dflt(js2dict(get_dflt(js2dict(i_room_kaihei_roompara_info),'主题',{})),'扩列聊天',''))",
        "ui_algo_kaihei_topic_select_pref_3d": "float(max(gets(iu_algo_kaihei_topic_select_pref,i_algo_kaihei_room_para_tag_list)))",
        "ui_algo_kaihei_topic_pref_3d": "float(max(gets(iu_algo_kaihei_topic_pref,i_algo_kaihei_room_para_tag_list)))",

        "i_algo_kaihei_room_para_tag_list": "join(i_algo_kaihei_room_para_tag_list,',')",  # 也可用 ",".join()
        "i_algo_kaihei_room_name_tag_list": "join(i_algo_kaihei_room_name_tag_list,',')",
        "i_room_kaihei_publish_timestamp": "i_room_kaihei_publish_timestamp",
    }

    schema_default = {"u_algo_kaihei_duration": "0"}

    ignore_features = []
    import time

    start = time.time()

    custom_func = '''
    def myfunc(x: [int, float], y: [int, float]) -> [int, float]:
        return (x + y)/(x + y)
    '''

    ef = Dataprocessor(verbose=False)

    res = ef.fit(spark_df, repartition_numb=4, config_map=config_map, token_map={},
                 rename_expr=rename_expr, schema_default=schema_default, special_expr=special_expr,
                 ignore_features=ignore_features, custom_func=custom_func).collect()

    # df = ef.createDF(spark, spark_df, repartition_numb=4, config_map=config_map, token_map={},
    #                  rename_expr=rename_expr, special_expr=special_expr,
    #                  ignore_features=ignore_features, custom_func=custom_func)
    # savepath = 'tes'
    # df.write.parquet(savepath, 'overwrite')

    end = time.time()
    print(end - start)  # 1个文件耗时4.7
