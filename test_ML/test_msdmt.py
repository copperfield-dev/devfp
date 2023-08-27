import os
import shutil
import sys

import networkx as nx
import numpy as np
import pandas as pd
import spektral
import pyspark
import tensorflow as tf
from sklearn.model_selection import StratifiedKFold, train_test_split
from keras.models import load_model
from model_msdmt import MSDMT
from elasticsearch import Elasticsearch


def get_f1_measure(precision, recall):
    f1 = 2 * precision * recall / (precision + recall)
    return round(f1, 4)


def get_diff_day(date, delta):
    sdate = datetime.datetime.strptime(date, '%Y-%m-%d')
    delta_date = datetime.timedelta(days=delta)
    new_date = sdate + delta_date
    return new_date.strftime('%Y-%m-%d')


def write_json_to_es(json, index):
    es_url = ["longzhou-lkv1.lz.dscc.99.com",
              "longzhou-lkv2.lz.dscc.99.com", "longzhou-lkv3.lz.dscc.99.com"]
    #es = Elasticsearch(hosts=es_url, port=7000, http_auth=('logstash_writer', 'writer0702'))
    es = Elasticsearch(hosts=es_url, port=7000,
                       http_auth=('eng_my_writer', 'ywmy1012'))
    if es.indices.exists(index=index):
        es.indices.delete(index=index)
    res = es.index(index=index,  document=json)
    return res


def data_process(timestep=7, maxlen=64):

    df_portrait = pd.read_csv('../data/output/data_player_portrait_7_to_7.csv')
    df_behavior = pd.read_csv(
        '../data/output/data_behavior_sequence_7_to_7.csv')
    df_social = pd.read_csv('../data/output/data_social_network_7_to_7.csv')
    df_label = pd.read_csv('../data/output/data_label_7_to_7.csv')

    portrait = df_portrait.drop(['uid', 'ds'], axis=1).values
    portrait = portrait.reshape(-1, timestep, portrait.shape[-1])

    behavior = df_behavior['seq'].apply(
        lambda x: x.split(',') if pd.notna(x) else []).values
    behavior = tf.keras.preprocessing.sequence.pad_sequences(sequences=behavior,
                                                             maxlen=maxlen,
                                                             padding='post')
    behavior = behavior.reshape(-1, timestep, maxlen)

    graph_social = nx.from_pandas_edgelist(df=df_social,
                                           source='src_uid',
                                           target='dst_uid',
                                           edge_attr=['weight'])
    adj_social = nx.adjacency_matrix(graph_social)
    adj_social = spektral.layers.GCNConv.preprocess(adj_social).astype('f4')

    churn = df_label['churn_label'].values.reshape(-1, 1)

    return portrait, behavior, adj_social, churn


if __name__ == '__main__':

    # * 超参数
    seed_value = 20220428
    lr = 0.0001
    epochs = 5000
    alpha = 0.5
    beta = 0.5
    timestep = 7
    maxlen = 64
    split_num = 5

    if len(sys.argv) < 3:
        sys.exit(-1)

    # 测试集第一天
    first_test_simple_day = sys.argv[1]
    # 训练集的时间天数
    train_d = int(sys.argv[2])
    # 测试集的时间天数
    test_d = int(sys.argv[3])
    timestep = train_d
    predict_type = str(train_d) + "d_pred_" + str(test_d) + "d"
    first_test_day = datetime.datetime.strptime(
        first_test_simple_day, '%Y%m%d').strftime('%Y-%m-%d')
    first_train_day = get_diff_day(first_test_day, -1 * timestep)
    last_train_day = get_diff_day(first_test_day, -1)
    last_test_day = get_diff_day(first_test_day, 6)

    es_index = "ywmy-ml-" + predict_type + "-" + first_test_simple_day

    # * 创建SparkConf
    conf = SparkConf().setAppName("ywmy-model-predict")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    sc = spark.sparkContext

    # 分别读取四份数据

    # 读取data_behavior_sequence
    data_behavior_sequence_rdd = sc.textFile(
        "/user/nd_rdg/result-data/offline/data_behavior_sequence/" + predict_type + "/" + first_test_day)
    data_behavior_sequence_rdd = data_behavior_sequence_rdd.map(lambda l: l.split(",")).map(
        lambda p: Row(uid=p[0], ds=p[1], seq=p[2].replace("\"", "")))

    # 读取data_label
    data_label_rdd = sc.textFile(
        "/user/nd_rdg/result-data/offline/data_label/" + predict_type + "/" + first_test_day)
    data_label_rdd = data_label_rdd.map(lambda l: l.split(",")).map(
        lambda p: Row(uid=p[0], churn_label=int(p[1])))

    # 读取data_player_portrait
    data_player_portrait_rdd = sc.textFile(
        "/user/nd_rdg/result-data/offline/data_player_portrait/" + predict_type + "/" + first_test_day)
    data_player_portrait_rdd = data_player_portrait_rdd.map(lambda l: l.split(",")).map(
        lambda p: Row(uid=p[0], ds=p[1], prof=int(p[2]), level=int(p[3]), vip=int(p[4]), battle=int(p[5])))

    # 读取data_social_network
    data_social_network_rdd = sc.textFile(
        "/user/nd_rdg/result-data/offline/data_social_network/" + predict_type + "/" + first_test_day)
    data_social_network_rdd = data_social_network_rdd.map(lambda l: l.split(",")).map(
        lambda p: Row(src_uid=p[0], dst_uid=p[1], weight=float(p[2])))

    # 转成Spark的DataFrame
    data_behavior_sequence_spark_df = spark.createDataFrame(
        data_behavior_sequence_rdd)
    data_label_spark_df = spark.createDataFrame(data_label_rdd)
    data_player_portrait_spark_df = spark.createDataFrame(
        data_player_portrait_rdd)
    data_social_network_spark_df = spark.createDataFrame(
        data_social_network_rdd)

    portrait, behavior, adj_social, churn = data_process(data_behavior_sequence_spark_df,
                                                         data_label_spark_df,
                                                         data_player_portrait_spark_df,
                                                         data_social_network_spark_df,
                                                         timestep=timestep,
                                                         maxlen=maxlen)
    print("==============input data's shape info===================")
    print(portrait.shape)
    print(behavior.shape)
    print(churn.shape)
    print(adj_social.shape)

    N = adj_social.shape[0]
    kfold = StratifiedKFold(n_splits=split_num, shuffle=True,
                            random_state=round(time.time()))

    i = 0
    nowTime = time.time()
    for train_index, test_index in kfold.split(portrait, churn):
        i += 1
        beginTime = time.time()

        train_index, val_index = train_test_split(
            train_index, test_size=0.1, random_state=round(time.time()))

        mask_train = np.zeros(N, dtype=bool)
        mask_val = np.zeros(N, dtype=bool)
        mask_test = np.zeros(N, dtype=bool)
        mask_train[train_index] = True
        mask_val[val_index] = True
        mask_test[test_index] = True

        # 构建训练集和测试集所需的神经网络矩阵
        adj_social_train = np.transpose(
            np.transpose(adj_social[mask_train])[mask_train])
        adj_social_val = np.transpose(
            np.transpose(adj_social[mask_val])[mask_val])
        adj_social_test = np.transpose(
            np.transpose(adj_social[mask_test])[mask_test])

        N_train = adj_social_train.shape[0]
        N_val = adj_social_val.shape[0]
        N_test = adj_social_test.shape[0]

        ### 对整个数组一次性赋值 ###

        # checkpoint_path = './model/checkpoint-{epoch:04d}.ckpt'
        # checkpoint_dir = os.path.dirname(checkpoint_path)
        #
        # if os.path.exists(checkpoint_dir):
        #     shutil.rmtree(checkpoint_dir)

        early_stopping = tf.keras.callbacks.EarlyStopping(monitor='val_loss',
                                                          patience=50,
                                                          mode='min')
        ### 最近5 epochs，val_loss不再减小###

        # best_checkpoint = tf.keras.callbacks.ModelCheckpoint(filepath=checkpoint_path,
        #                                                      monitor='val_loss',
        #                                                      verbose=1,
        #                                                      save_best_only=True,
        #                                                      save_weights_only=True,
        #                                                      mode='auto')

        model = MSDMT(timestep=timestep, behavior_maxlen=maxlen)

        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr),
                      loss={'output_1': tf.keras.losses.BinaryCrossentropy()},
                      loss_weights={'output_1': alpha},
                      metrics={'output_1': [tf.keras.metrics.BinaryAccuracy(), tf.keras.metrics.AUC(),
                                            tf.keras.metrics.Precision(), tf.keras.metrics.Recall(),
                                            tf.keras.metrics.TrueNegatives(), tf.keras.metrics.TruePositives()]})

        history = model.fit([portrait[mask_train], behavior[mask_train], adj_social_train], [churn[mask_train]],
                            validation_data=(
                            [portrait[mask_val], behavior[mask_val], adj_social_val], [churn[mask_val]]),
                            batch_size=N_train,
                            epochs=epochs,
                            shuffle=False,
                            callbacks=[early_stopping],
                            verbose=2)

        test_loss = model.evaluate([portrait[mask_test], behavior[mask_test], adj_social_test], [churn[mask_test]],
                                   batch_size=N_test,
                                   verbose=2)

        precision = round(test_loss[3], 4)
        recall = round(test_loss[4], 4)
        F1 = get_f1_measure(precision, recall)
        # precision = TP/(TP+FP) ===> FP = (TP/precision) - TP
        # recall = TP/(TP+FN) ===> FN = (TP/recall) - TP
        TN = round(test_loss[5], 0)
        TP = round(test_loss[6], 0)
        FN = round(TP / recall, 0) - TP
        FP = round(TP/precision, 0) - TP

        print(model.metrics_names)
        print('\n********', i, '******Test loss:', test_loss)

        # 开始将结果数据组织成JSON格式并发送给ES
        history_data = history.history
        model_info = {"lr": lr, "epochs": epochs, "alpha": alpha,
                      "max_len": maxlen, "split_num": split_num}
        test_info = {"loss": round(test_loss[0], 6), "binary_accuracy": round(test_loss[1], 4), "auc": round(test_loss[2], 4),
                     "precision": round(test_loss[3], 4), "recall": round(test_loss[4], 4), "f1": F1,
                     "TN": TN, "TP": TP, "FN": FN, "FP": FP, "N": N_test}
        train_info = {"loss": round(history_data['loss'][-1], 6),
                      "binary_accuracy": round(history_data['binary_accuracy'][-1], 4),
                      "auc": round(history_data['auc'][-1], 4),
                      "precision": round(history_data['precision'][-1], 4),
                      "recall": round(history_data['recall'][-1], 4),
                      "val_loss": round(history_data['val_loss'][-1], 6),
                      "val_binary_accuracy": round(history_data['val_binary_accuracy'][-1], 4),
                      "val_auc": round(history_data['val_auc'][-1], 4),
                      "val_precision": round(history_data['val_precision'][-1], 4),
                      "val_recall": round(history_data['val_recall'][-1], 4),
                      }
        epochs_used = len(history_data['loss'])
        users_cnt = churn.shape[0]
        observe_time = first_train_day + "~" + last_train_day
        predict_time = first_test_day + "~" + last_test_day
        create_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        cost_time = round(time.time() - beginTime)
        es_time = first_test_day + "T00:00:00.000+0800"
        json = {"time": es_time, "predict_type": predict_type, "train_id": i, "cost_time_seconds": cost_time,
                "first_test_day": first_test_day, "last_train_day": last_train_day, "observe_time": observe_time, "predict_time": predict_time,
                "users_cnt": users_cnt, "epochs_used": epochs_used, "create_time": create_time,
                "train_info": train_info, "test_info": test_info, "model_info": model_info}
        print("==================json result====================")
        print(json)
        write_json_to_es(json, es_index)

        break

    print("cost time: ", round(time.time() - nowTime), "s")

    # 关闭
    sc.stop()
