# -*- coding: utf-8 -*-
import sys, gc, subprocess, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from dataflow.DataProcess import Dataprocessor
from dataflow.fc_update import load_tfrecord, FCUpdate
from dataflow.utils import date_range, get_date, save_json

def adjust_data_ratio(df: DataFrame, duration_threshold: int, desired_ratio: int) -> DataFrame:
    '''
    抽样，desired_ratio=10，正负样本1:10
    正样本：duration>=duration_threshold
    '''
    df_with_category = df.withColumn('is_gangup',
                                     F.when(F.col('duration') >= duration_threshold, 'gangup').otherwise('no_gangup'))
    greater_count = df_with_category.filter(F.col('is_gangup') == 'gangup').count()
    less_count = df_with_category.filter(F.col('is_gangup') == 'no_gangup').count()
    desired_less_count = greater_count * desired_ratio
    sampled_greater_df = df_with_category.filter(F.col('is_gangup') == 'gangup').sample(fraction=1.0)
    sampled_less_df = df_with_category.filter(F.col('is_gangup') == 'no_gangup').sample(
        fraction=desired_less_count / less_count)
    final_df = sampled_greater_df.union(sampled_less_df)
    return final_df


if __name__ == "__main__":
    dt = str(sys.argv[1])
    dt1 = get_date(-1, dt)
    save_tfrecord = int(sys.argv[2])
    update_dt = int(sys.argv[3])
    is_update = int(sys.argv[4])
    save_parquet = int(sys.argv[5])

    spark = SparkSession.builder \
        .appName('home_page_dataset') \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext

    # 样本实时数据
    real_time = spark.read.table("rcmd_tablename.mt_homepage_user_profile_realtime_di") \
        .filter((col('dt') == f"{dt}") & ((col('filter_tab') == '扩列聊天') | (col('filter_tab') == '扩列')))

    # 用户静态画像，user_id是主键
    cols = ['user_id', 'platform_name', 'reg_time', 'gender', 'age', 'game_tagname', 'province', 'city']
    user_static = spark.read.table("rcmd_tablename.mt_user_profile_static_di") \
        .filter((col('dt') == f"{dt}")) \
        .select(*cols)
    rename_dict = {i: 'u_algo_mt_' + i for i in user_static.columns}
    user_static = user_static.select([col(c).alias(rename_dict[c]) for c in user_static.columns])

    # 房主静态画像
    cols = ['user_id', 'platform_name', 'reg_time', 'gender', 'age', 'game_tagname', 'province', 'city']
    owner_static = spark.read.table("rcmd_tablename.mt_user_profile_static_di") \
        .filter((col('dt') == f"{dt}")) \
        .select(*cols)
    rename_dict = {i: 'o_algo_mt_' + i for i in owner_static.columns}
    owner_static = owner_static.select([col(c).alias(rename_dict[c]) for c in owner_static.columns])

    # 房间侧画像
    cols = ['room_id', 'publish_tag_cnt_3w', 'exposure_tag_cnt_3w', 'click_tag_cnt_3w',
            'stay_tag_dur_3w', 'gang_up_tag_cnt_3w', 'on_mic_tag_cnt_3w',
            'publish_tag_cnt_1w', 'exposure_tag_cnt_1w', 'click_tag_cnt_1w',
            'stay_tag_dur_1w', 'gang_up_tag_cnt_1w', 'on_mic_tag_cnt_1w',
            'publish_tag_cnt_3d', 'exposure_tag_cnt_3d', 'click_tag_cnt_3d',
            'stay_tag_dur_3d', 'gang_up_tag_cnt_3d', 'on_mic_tag_cnt_3d']

    room_profile = spark.read.table("rcmd_tablename.mt_homepage_room_profile_di") \
        .filter((col('dt') == f"{dt1}")) \
        .select(*cols)
    rename_dict = {i: 'i_algo_mt_' + i for i in room_profile.columns}
    room_profile = room_profile.select([col(c).alias(rename_dict[c]) for c in room_profile.columns])

    # 用户侧画像
    cols = ['user_id', 'ui_room_mt_list_tag_exp_cnt_7d', 'ui_room_mt_list_tag_enter_cnt_7d',
            'ui_room_mt_list_tag_gangup_cnt_7d', 'ui_room_mt_list_tag_duration_7d',
            'ui_room_mt_list_tag_mic_cnt_7d', 'ui_room_mt_list_tag_exp_room_cnt_7d',
            'ui_room_mt_list_tag_enter_room_cnt_7d', 'ui_room_mt_list_tag_gangup_room_cnt_7d',
            'ui_room_mt_list_tag_mic_room_cnt_7d', 'ui_room_mt_tag_enter_cnt_7d',
            'ui_room_mt_tag_gangup_cnt_7d', 'ui_room_mt_tag_duration_7d', 'ui_room_mt_tag_mic_cnt_7d',
            'ui_room_mt_tag_enter_room_cnt_7d', 'ui_room_mt_tag_gangup_room_cnt_7d',
            'ui_room_mt_tag_mic_room_cnt_7d', 'ui_room_mt_list_tag_exp_cnt_21d',
            'ui_room_mt_list_tag_enter_cnt_21d', 'ui_room_mt_list_tag_gangup_cnt_21d',
            'ui_room_mt_list_tag_duration_21d', 'ui_room_mt_list_tag_mic_cnt_21d',
            'ui_room_mt_list_tag_exp_room_cnt_21d', 'ui_room_mt_list_tag_enter_room_cnt_21d',
            'ui_room_mt_list_tag_gangup_room_cnt_21d', 'ui_room_mt_list_tag_mic_room_cnt_21d']

    user_profile = spark.read.table("rcmd_tablename.dwd_tt_algo_mt_homepage_user_profile_di") \
        .filter((col('dt') == f"{dt1}")) \
        .select(*cols)

    rename_dict = {i: 'u_algo_mt_' + i.replace('ui_room_mt_', '').replace('7d', '1w').replace('21d', '3w') \
                   for i in user_profile.columns}
    user_profile = user_profile.select([col(c).alias(rename_dict[c]) for c in user_profile.columns])

    # 正负样本比例1:x
    key = adjust_data_ratio(df=real_time, duration_threshold=600, desired_ratio=10)
    tmp = key.join(user_static, (key.user_id == user_static.u_algo_mt_user_id), how='left') \
        .join(owner_static, (key.room_owner_user_id == owner_static.o_algo_mt_user_id), how='left') \
        .join(room_profile, (key.room_id == room_profile.i_algo_mt_room_id), how='left') \
        .join(user_profile, (key.user_id == user_profile.u_algo_mt_user_id), how='left') \
        .drop('u_algo_mt_user_id', 'o_algo_mt_user_id', 'i_algo_mt_room_id', 'u_algo_mt_user_id')

    root_dir = './'
    config_local_dir = os.path.join(root_dir, 'feature.json')

    ef = Dataprocessor(verbose=False)

    schema_default = {"duration": "0", 'i_algo_mt_publish_tag_cnt_3w': {}, 'i_algo_mt_exposure_tag_cnt_3w': {},
                      'i_algo_mt_click_tag_cnt_3w': {}, 'i_algo_mt_stay_tag_dur_3w': {},
                      'i_algo_mt_gang_up_tag_cnt_3w': {}, 'i_algo_mt_on_mic_tag_cnt_3w': {},
                      'i_algo_mt_publish_tag_cnt_1w': {}, 'i_algo_mt_exposure_tag_cnt_1w': {},
                      'i_algo_mt_click_tag_cnt_1w': {}, 'i_algo_mt_stay_tag_dur_1w': {},
                      'i_algo_mt_gang_up_tag_cnt_1w': {}, 'i_algo_mt_on_mic_tag_cnt_1w': {},
                      'i_algo_mt_publish_tag_cnt_3d': {}, 'i_algo_mt_exposure_tag_cnt_3d': {},
                      'i_algo_mt_click_tag_cnt_3d': {}, 'i_algo_mt_stay_tag_dur_3d': {},
                      'i_algo_mt_gang_up_tag_cnt_3d': {}, 'i_algo_mt_on_mic_tag_cnt_3d': {}}
    df = ef.createDF(spark, tmp, config_map=config_local_dir, token_map={},
                     special_expr={},
                     schema_default=schema_default,
                     custom_func='', repartition_numb=200)

    tfrecord_savepath = os.path.join(root_dir, 'tfrecord')
    if save_tfrecord:
        df.write.format("tfrecord").option("codec", "org.apache.hadoop.io.compress.GzipCodec").mode("overwrite").save(
            os.path.join(tfrecord_savepath, dt))

    if save_parquet:
        parquet_savepath = os.path.join(root_dir, 'parquet')
        df.write.format("parquet").option("compression", "gzip").mode("overwrite").save(
            os.path.join(parquet_savepath, dt))

    if is_update:
        start = get_date(-update_dt, dt)
        end = dt
        # date_range 不包含结束日期
        dt_df = load_tfrecord(spark, tfrecord_savepath, date_range(start=start, end=end))

        pre_params = FCUpdate(config_map=config_local_dir)
        update_featjs = pre_params.update(dt_df)

        save_name = 'feature_{}_{}.json'.format(start, get_date(-1, dt))
        save_path = '/tmp'
        save_path_name = os.path.join(save_path, save_name)

        if not os.path.exists(save_path):  # 判断是否存在文件夹如果不存在则创建为文件夹
            os.makedirs(save_path)  # makedirs 创建文件时如果路径不存在会创建这个路径
            print('makedirs {}'.format(save_path))

        save_json(save_path_name, update_featjs)
        assert os.path.exists(save_path_name), '{} not exists'.format(save_path_name)

        print('save path: {}'.format(os.path.join(root_dir, 'update_featjs')))
        # put_file_to_hdfs_or_obs(save_path_name, os.path.join(root_dir, 'update_featjs'))
        print('save done')

        os.remove(save_path_name)
