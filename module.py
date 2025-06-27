import warnings
import ruptures as rpt
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm
from scipy.stats import pearsonr
import os
import time
from tqdm import tqdm
import re
import json
import trino
from settings import *
from datetime import timedelta
import datetime
from requests import Session
import queue
import concurrent.futures
import threading
import matplotlib.dates as mdate
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib
from dateutil.relativedelta import relativedelta
matplotlib.use("Agg")
warnings.filterwarnings("ignore")
from mloutputs import MLCustomOutputs

# 紀錄下載整理的資料
data_que = queue.Queue()
# 鎖
lock = threading.Lock()


class Program:
    def __init__(self, equipment, chamber, recipe, starttime, endtime, e):
        if e.key == True:
            print("登入成功")
            self.eqip = equipment
            self.chamber = chamber
            self.recipe = recipe  # list
            self.starttime = starttime
            self.endtime = endtime
            self.e = e
        else:
            print("登入失敗")

    def gen_month(self):
        """
        月生成器
        """
        starttime = datetime.datetime.strptime(self.starttime[:7], "%Y-%m")
        endtime = datetime.datetime.strptime(self.endtime[:7], "%Y-%m")
        
        while starttime <= endtime:
            yield datetime.datetime.strftime(starttime, "%Y%m")
            # 增加一個月
            next_month = starttime.month + 1
            if next_month > 12:
                starttime = starttime.replace(year=starttime.year + 1, month=1)
            else:
                starttime = starttime.replace(month=next_month)

    @staticmethod
    def handle_tube_wafers(x):
        lst = []
        for i in x:
            lst += json.loads(i)
        return lst

    @staticmethod
    def data_process(dataset):
        # recipe step 若為"" 則以TIME取代
        #dataset["recipe_step"] = dataset["recipe_step"].apply(lambda x: "TIME" if x == "" else str(x))
        dataset["feature_name"] = dataset["parameter"] + "__" + dataset["recipe_step"] + "__" + dataset[
            "statistics_name"]
        dataset = dataset[["start_time", "wafers", "recipe", "feature_name", "statistics_value"]]
        dataset.columns = ["DateTime", "Alias", "Recipe", "feature_name", ""]
        dataset = dataset.drop_duplicates(
            subset=["DateTime", "Alias", "Recipe", "feature_name"], keep="first")
        dataset = dataset.set_index(
            ["DateTime", "Alias", "Recipe", "feature_name"])
        dataset = dataset.unstack().reset_index()
        dataset.columns = [i[0] if i[1] == "" else i[1] for i in dataset.columns]
        dataset["Alias"] = dataset["Alias"].apply(lambda x: json.loads(x)[0]["alias_id"])
        return dataset

    def query_one_month(self, month):
        """查詢一個月的資料，按天拆分並使用線程池"""
        print(f"\n處理月份: {month}")
        
        # 轉換月份格式為datetime對象
        month_start = datetime.datetime.strptime(month, "%Y%m")
        # 計算下個月的第一天
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)
        
        # 生成該月的每一天
        current_day = month_start
        days = []
        while current_day < next_month:
            days.append(current_day)
            current_day += datetime.timedelta(days=1)

        # 使用線程池處理每天的查詢
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            progress_bar = tqdm(total=len(days), desc="下載資料中")
            
            for day in days:
                futures.append(
                    executor.submit(self.query_one_day, month, day))
        
            # 等待所有查詢完成
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                    progress_bar.update(1)
                except Exception as e:
                    print(f"Error querying data: {str(e)}")
            
            progress_bar.close()

    def query_one_day(self, month, day):
        """查詢一天的資料"""
        session = Session()
        cacert_path = r'\\10.9.205.78\hm10_phm\cacert\cacert.pem'
        session.verify = cacert_path
        conn = trino.dbapi.connect(
            host=HOST,
            port=PORT,
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(USER, PASSWD),
            http_session=session
        )
        
        day_start = day.strftime("%Y-%m-%d 00:00:00")
        day_end = day.strftime("%Y-%m-%d 23:59:59")
        
        for recipe_name in self.recipe:
            if "%" in recipe_name:
                sql = '''SELECT start_time, wafers, recipe, recipe_step, parameter, statistics_name, statistics_value 
                         FROM hadoop.{}.fdc_hispcdata_new_view
                         WHERE eqp_id = '{}'
                         AND chamber = '{}'
                         AND recipe LIKE '{}'
                         AND month = {}
                         AND start_time >= TIMESTAMP '{}'
                         AND start_time <= TIMESTAMP '{}' '''.format(
                    SCHEME, self.eqip, self.chamber, recipe_name, month, day_start, day_end)
            else:
                sql = '''SELECT start_time, wafers, recipe, recipe_step, parameter, statistics_name, statistics_value 
                         FROM hadoop.{}.fdc_hispcdata_new_view
                         WHERE eqp_id = '{}'
                         AND chamber = '{}'
                         AND recipe = '{}'
                         AND month = {}
                         AND start_time >= TIMESTAMP '{}'
                         AND start_time <= TIMESTAMP '{}' '''.format(
                    SCHEME, self.eqip, self.chamber, recipe_name, month, day_start, day_end)

            cur = conn.cursor()
            try:
                rows = self.execute_query(cur, sql)
                if rows and len(rows) > 0:
                    columns = [desc[0] for desc in cur.description]
                    dataset = pd.DataFrame(rows, columns=columns)
                    data = Program.data_process(dataset)
                    with lock:
                        data_que.put(data)
            except Exception as e:
                print(f"Error querying data for {day_start}: {str(e)}")
            finally:
                cur.close()
        
        conn.close()

    def execute_query(self, cur, sql, max_retries=5):
        """執行查詢並處理重試邏輯"""
        retry_count = 0
        while retry_count < max_retries:
            try:
                cur.execute(sql)
                return cur.fetchall()
            except Exception as e:
                retry_count += 1
                print(f"exceeds 10mins, retry again... ({retry_count}/{max_retries})")
                if retry_count >= max_retries:
                    print(f"Maximum retries ({max_retries}) reached. Aborting query.")
                    raise Exception(f"Query failed after {max_retries} retries")
                time.sleep(1)  # 添加短暫延遲避免立即重試

    @staticmethod
    def lam_process(data_all):
        rm = []
        for feature in data_all.columns[3:]:
            if data_all[feature].isnull().sum() > 0.8 * len(data_all):
                rm.append(feature)
        data_all = data_all.drop(rm, axis=1)

        def lam_combind(x):
            for i in x:
                if not np.isnan(i):
                    return i
            return i

        def lam_datetime_combind(x):
            return list(x)[0]

        data1 = data_all.groupby(["Alias", "Recipe"])[data_all.columns[3:]].aggregate([lam_combind]).reset_index()
        data1.columns = [i[0] for i in data1.columns]
        data2 = data_all.groupby(["Alias", "Recipe"])["DateTime"].aggregate([lam_datetime_combind]).reset_index()
        data2.columns = ["Alias", "Recipe", "DateTime"]
        data2 = data2[["DateTime", "Alias", "Recipe"]]

        data_all = pd.merge(data2, data1, on=["Alias", "Recipe"], how="inner")
        data_all["DateTime"] = pd.to_datetime(data_all["DateTime"])
        data_all = data_all.sort_values("DateTime", ascending=True).reset_index(drop=True)
        return data_all

    @staticmethod
    def merge_list():
        # 合併share_list -> dataframe
        data_all = pd.DataFrame()
        while not data_que.empty():
            sub = data_que.get()
            data_all = pd.concat([data_all, sub], ignore_index=True)
        data_all["DateTime"] = pd.to_datetime(data_all["DateTime"])
        data_all = data_all.sort_values(
            "DateTime", ascending=True).reset_index(
            drop=True)
        if data_all["Alias"].value_counts().quantile(0.1) > 1:
            print("Alias重複過多，判斷為TF WCVD 同資料結構case，啟動合併程式")
            data_all = Program.lam_process(data_all)
        return data_all

    @staticmethod
    def clean(data_all):
        # 資料清理
        # a 移除std~=0的特徵
        rm = data_all.iloc[:, 3:].std(
        )[data_all.iloc[:, 3:].std() < 0.00000001].index
        data_all = data_all.drop(rm, axis=1)

        # 最多允許刪除 5% wafer, 在此前題可以留的最大feature數量
        thresh = len(data_all) * 0.05
        x = data_all.isnull().sum()
        for q in np.arange(0, 1, 0.01):
            if x.quantile(q) > thresh:
                break
        q -= 0.01
        rm = data_all.iloc[:, 3:].isnull().sum(
        )[data_all.iloc[:, 3:].isnull().sum() > x.quantile(q)].index
        data_all = data_all.drop(rm, axis=1)
        data_all = data_all.dropna(axis=0)
        return data_all

    def query(self):
        # self.map
        self.query_child_system() # get self.map
        if len(self.map) == 0:
            print("沒有建立子系統")
            return

        # 按月份串行執行
        total_month = list(self.gen_month())
        self.total = len(total_month)
        
        for month in total_month:
            try:
                self.query_one_month(month)
            except Exception as e:
                print(f"Error processing month {month}: {str(e)}")
        
        data_all = Program.merge_list()
        self.fdc = Program.clean(data_all)  # fdc
        self.fdc["DateTime"] = pd.to_datetime(self.fdc["DateTime"])
        self.fdc = self.fdc.sort_values(
            "DateTime", ascending=True).reset_index(drop=True)
        print("資料準備完成")

    def show_all_recipe_steps(self):
        print(sorted(list(set([i.split("__")[1]
              for i in self.fdc.columns[3:]]))))

    def use_all_recipe_steps(self):
        self.fdc_process = self.fdc.copy()
        print("使用all recipe steps")

    def use_filtered_recipe_steps(self, recipe_step):
        self.fdc_process = self.fdc.copy()
        feature_selected = [i for i in self.fdc.columns[3:]
                            if i.split("__")[1] in recipe_step]
        self.fdc_process = self.fdc_process[[
            "DateTime", "Alias", "Recipe"] + feature_selected]

    def output_raw_data(self):
        self.fdc_process.to_csv("raw.csv", index=False)
        print("資料下載完成")

    def input_raw_data(self):
        """從raw.csv讀取資料"""
        try:
            if os.path.exists("raw.csv"):
                self.fdc_process = pd.read_csv("raw.csv")
                # 確保DateTime欄位為datetime格式
                self.fdc_process["DateTime"] = pd.to_datetime(self.fdc_process["DateTime"])
                print("成功載入raw.csv資料")
            else:
                print("找不到raw.csv檔案")
        except Exception as e:
            print(f"載入raw.csv時發生錯誤: {str(e)}")

    def output_category(self):
        self.query_child_system()
        self.map.to_csv("category.csv", index=False)

    def input_category(self):
        self.map = pd.read_csv("category.csv")

    def query_child_system(self):
        session = Session()
        cacert_path = r'\\10.9.205.78\hm10_phm\cacert\cacert.pem'
        session.verify = cacert_path
        conn = trino.dbapi.connect(
            host=HOST,
            port=PORT,
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(USER, PASSWD),
            http_session=session
        )
        sql = '''select parameter as Name, category as GroupName from hadoop.khfdc.fdc_svid
	                  where chamber = '{}'
	                  '''.format(self.chamber)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dataset = pd.DataFrame(rows, columns=columns)
        dataset["GroupName"] = dataset["GroupName"].apply(
            lambda x: np.nan if x == "" else x)
        dataset = dataset.dropna(axis=0).reset_index(drop=True)
        self.map = dataset

    @staticmethod
    def __recursion(data, features, idx=0):
        for feature in features[idx + 1:]:
            r = abs(pearsonr(data[features[idx]], data[feature])[0])
            if np.isnan(r):
                r = 0
            if r > 0.9:
                features.remove(feature)
        idx += 1
        if idx < len(features):
            Program.__recursion(data, features, idx)

    @staticmethod
    def select(data, svid):
        """
                        特徵篩選
        """
        # 取該svid的所有特徵出來
        all_features = "\n".join(data.columns[3:])
        svid_features = re.findall(
            r"^%s__.*__.+" %
            svid, all_features, flags=re.M)
        # 各svid內特徵相關性只留0.9以下
        Program.__recursion(data, svid_features, 0)
        return svid_features

    @staticmethod
    def __ols_select(data, map, sel_features):
        # 添加簡單線性回歸的x
        # print("============特徵篩選============\n(取slope<0.01 和 pvalue > 0.05)\n(各子系統留50個特徵)")
        data = data[['DateTime', 'Alias', "Recipe"] + list(sel_features)]
        X = np.arange(len(data))
        X = sm.add_constant(X)
        res = {"feature": [], "slope": [], "p_value": []}
        for feature in data.columns[3:]:
            model = sm.OLS(data[feature], X).fit()
            slope_estimate = model.params[1]
            p_value = model.pvalues[1]
            res["feature"].append(feature)
            res["slope"].append(slope_estimate)
            res["p_value"].append(p_value)
        res = pd.DataFrame(res)
        res = res[(res["p_value"] > 0.05) & (res["slope"] < 0.01)]
        res["slope"] = abs(res["slope"])
        res = res.sort_values("slope", ascending=True).reset_index(drop=True)
        res["Class"] = res["feature"].apply(
            lambda x: map[map["Name"] == x.split("__")[0]]["GroupName"].values[0])
        res["svid"] = res["feature"].apply(lambda x: x.split("__")[0])
        res["statistics"] = res["feature"].apply(lambda x: x.split("__")[2])
        res["Class"] = res["Class"].apply(lambda x: x.replace(" ", "_"))

        dic_ = {}
        for class_ in res["Class"].unique():
            sub = res[res["Class"] == class_]
            dic_[class_] = {}
            # 每個svid mean/std 要取的數量
            svids = sub["svid"].unique()
            n = int(np.ceil(25 / len(svids)))
            for svid in svids:
                features = []
                sub_s = sub[sub["svid"] == svid]
                # 取mean的特徵
                sub_s_mean = sub_s[sub_s["statistics"]
                                   == "Mean"].reset_index(drop=True)
                features_mean = list(sub_s_mean["feature"][:n])
                features += features_mean
                sub_s_std = sub_s[sub_s["statistics"]
                                  == "Stdev"].reset_index(drop=True)
                features_std = list(sub_s_std["feature"][:n])
                features += features_std
                if len(features) < int(np.ceil(50 / len(svids))):
                    diff = int(np.ceil(50 / len(svids))) - len(features)
                    sub_s_other = sub_s[sub_s["statistics"].str.startswith(
                        ("Min", "Max"))].reset_index(drop=True)
                    features_other = list(sub_s_other["feature"][:diff])
                    features += features_other
                dic_[class_][svid] = features
        # self.data 僅留t2 member 特徵
        features = []
        for class_ in dic_:
            for svid in dic_[class_]:
                features += dic_[class_][svid]
        data = data[['DateTime', 'Alias', 'Recipe'] + features]
        return data, dic_

    @staticmethod
    def __get_pc_number(explained_variance_ratio,
                        explained_variance_ratio_setting):
        total = 0
        for idx, i in enumerate(explained_variance_ratio):
            total += i
            if total > explained_variance_ratio_setting:
                break
        idx += 1
        return idx

    def __t2_train(data, dic_, map):
        # 標準化資料
        # print("============訓練T2============")
        ss = StandardScaler()
        nor = ss.fit_transform(data.iloc[:, 3:])
        nor = pd.DataFrame(nor, columns=list(data.columns[3:]))
        # 儲存 features
        stats = {}
        mean_ = ss.mean_
        std_ = ss.scale_  # ddof=0 (n)
        # 特徵 mean std 皆造 self.data.columns 順序放
        stats["features"] = [i for i in data.columns[3:]]
        stats["mean"] = list(mean_)
        stats["std"] = list(std_)

        # 儲存各子系統特徵
        for class_ in dic_:
            stats[class_] = {"features": []}
            for svid in dic_[class_]:
                stats[class_]["features"] += dic_[class_][svid]
        pca = PCA()
        for class_ in map["GroupName"].unique():
            try:
                # 各子系統特徵
                if class_ in stats:
                    features = stats[class_]["features"]
                    nor_child = nor[features]
    
                    pca.fit(nor_child)
                    pc_choose_num = Program.__get_pc_number(
                        pca.explained_variance_ratio_, 0.9)
                    eigen_vector = pca.components_[:pc_choose_num]
                    eigen_value = pca.explained_variance_[:pc_choose_num]
                    eigen_value_matrix = np.diag(eigen_value)
                    eigen_value_matrix_inv = np.linalg.inv(eigen_value_matrix)
    
                    # 紀錄 pc數, eigenvector, eigenvalue matrix, eigenvalue matrix inv
                    stats[class_]['PC'] = pc_choose_num
                    stats[class_]['eigenvector'] = eigen_vector
                    stats[class_]['eigenvalue_matrix'] = eigen_value_matrix
                    stats[class_]['eigenvalue_matrix_inv'] = eigen_value_matrix_inv
            except Exception as e:
                print(f"處理發生錯誤: {e}")
        # 輸出 model (self.stats)
        return stats

    @staticmethod
    def train(data, map, sel_features):
        # data: 挑選出的特徵 dic_:各子系統所含的svid和特徵表
        data, dic_ = Program.__ols_select(data, map, sel_features)
        stats = Program.__t2_train(data, dic_, map)
        return stats

    @staticmethod
    def trans_filename(filename):
        return re.sub(r'[\\:\/\?\|<>"]', '', filename)

    @staticmethod
    def query_svid(eqid, chamber, recipe, month):
        session = Session()
        cacert_path = r'\\10.9.205.78\hm10_phm\cacert\cacert.pem'
        session.verify = cacert_path
        conn = trino.dbapi.connect(
            host=HOST,
            port=PORT,
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(USER, PASSWD),
            http_session=session
        )
        cur = conn.cursor()
        # (datetime.strptime(date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        for recipe_name in recipe:
            if "%" in recipe_name:
                sql = '''SELECT distinct parameter, svid FROM hadoop.khfdc.fdc_hispcdata_new_view
			                  where eqp_id = '{}'
			                  and chamber = '{}'
			                  and recipe like '{}' 
			                  and month = {} '''.format(eqid, chamber, recipe_name, month)
                cur.execute(sql)
                rows = cur.fetchall()
                if len(rows) > 0:
                    break
                sql = '''SELECT distinct parameter, svid FROM hadoop.khfdc.fdc_hispcdata_new_view
							                  where eqp_id = '{}'
							                  and chamber = '{}'
							                  and recipe like '{}' 
							                  and month = {} '''.format(eqid, chamber, recipe_name, (datetime.datetime.strptime(month, "%Y%m%d") + relativedelta(months=1)).strftime("%Y%m%d"))
                cur.execute(sql)
                rows = cur.fetchall()
                if len(rows) > 0:
                    break
            else:
                sql = '''SELECT distinct parameter, svid FROM hadoop.khfdc.fdc_hispcdata_new_view
							                  where eqp_id = '{}'
							                  and chamber = '{}'
							                  and recipe = '{}' 
							                  and month = {} '''.format(eqid, chamber, recipe_name, month)
                cur.execute(sql)
                rows = cur.fetchall()
                if len(rows) > 0:
                    break
                sql = '''SELECT distinct parameter, svid FROM hadoop.khfdc.fdc_hispcdata_new_view
											                  where eqp_id = '{}'
											                  and chamber = '{}'
											                  and recipe = '{}' 
											                  and month = {} '''.format(eqid, chamber, recipe_name, (datetime.datetime.strptime(month, "%Y%m%d") + relativedelta(months=1)).strftime("%Y%m%d"))
                cur.execute(sql)
                rows = cur.fetchall()
                if len(rows) > 0:
                    break
        columns = [desc[0] for desc in cur.description]
        svid_table = pd.DataFrame(rows, columns=columns)
        svid_table["svid"] = svid_table["svid"].astype(str)
        svid_table = dict(
            zip(svid_table["parameter"], svid_table["svid"]))
        return svid_table

    @staticmethod
    def __transToE3Name(feature):
        param = feature.split("__")[0]
        step = feature.split("__")[1]
        stsc = feature.split("__")[2]

        window = "PHM__" + step.replace(" ", "_")

        newName = "%s.%s.%s" % (window, param, stsc)
        return newName

    @staticmethod
    def __mov_median(series, n):  # 移動中位數
        return list(series.rolling(n).median()[n - 1:])

    @staticmethod
    def t2_trans(data_all, stats, path, map, svid_table, training_starttime, training_endtime):
        # 標準化
        data_all = data_all[[
            "DateTime", "Alias", "Recipe"] + stats["features"]]  # 特徵順序與stats對齊
        data_all = data_all.dropna(axis=0).reset_index(drop=True)
        ss = StandardScaler()
        ss.mean_ = stats["mean"]
        ss.scale_ = stats["std"]

        nor = ss.transform(data_all.iloc[:, 3:])
        nor = pd.DataFrame(nor,
                           columns=list(data_all.columns[3:]))

        for class_ in map["GroupName"].unique():
            try:
                if class_ in stats:
                    if not os.path.exists(os.path.join(path, class_, "features")):
                        os.makedirs(os.path.join(path, class_, "features"))
                    if not os.path.exists(os.path.join(path, class_, "t2")):
                        os.makedirs(os.path.join(path, class_, "t2"))
                    if not os.path.exists(os.path.join(path, class_, "key_features")):
                        os.makedirs(os.path.join(path, class_, "key_features"))
    
                    # 輸出iapc models
                    iapc = {"eigenValue": [], "loadingArray": {},
                            "loadingName": [], "trainMean": {}, "trainSd": {}}
                    iapc["eigenValue"] = list(
                        np.diag(stats[class_]["eigenvalue_matrix"]))
                    iapc["loadingArray"] = {"PC{}".format(i + 1): list(stats[class_]["eigenvector"][i]) for i in
                                            range(stats[class_]["PC"])}
                    iapc["loadingName"] = [
                        i.split("__")[1] + "_" + svid_table[i.split("__")[0]] + "_" + i.split("__")[2].replace("Mean",
                                                                                                               "Avg").replace(
                            "Stdev", "Std") for i in stats[class_]["features"]]
    
                    index = [stats["features"].index(
                        feature) for feature in stats[class_]["features"]]
                    mean_ = [stats["mean"][i] for i in index]
                    std_ = [stats["std"][i] for i in index]
                    iapc["trainMean"] = {iapc["loadingName"][i]: mean_[i] for i in
                                         range(len(stats[class_]["features"]))}
                    iapc["trainSd"] = {iapc["loadingName"][i]: std_[i]
                                       for i in range(len(stats[class_]["features"]))}
    
                    with open(os.path.join(path, class_, "t2", "models.json"), 'w') as json_file:
                        json.dump(iapc, json_file)
    
                    # 輸出 mapping table
                    feature_window_map = pd.DataFrame(
                        {"feature": stats[class_]["features"]})
                    feature_window_map["window"] = feature_window_map["feature"].apply(
                        lambda x: "PHM__" + x.split("__")[1].replace(" ", "_"))
                    feature_window_map.to_csv(os.path.join(path, class_, "%s_feature_window_map.csv" % (class_)),
                                              index=False)
    
                    # 輸出 edwm csv
                    edwm_csv = pd.DataFrame()
                    edwm_csv["Data Type"] = ["Raw"] * len(feature_window_map)
                    edwm_csv["Parameter"] = feature_window_map["feature"].apply(lambda x: x.split("__")[0])
                    edwm_csv["SVID"] = edwm_csv["Parameter"].apply(lambda x: svid_table[x])
                    edwm_csv["Recipe Step"] = feature_window_map["feature"].apply(lambda x: x.split("__")[1])
                    edwm_csv["Feature Type"] = feature_window_map["feature"].apply(
                        lambda x: x.split("__")[2].replace("Mean", "Avg").replace("Stdev", "Std"))
                    edwm_csv.to_csv(os.path.join(path, class_, "%s_feature_iapc_map.csv" % (class_)),
                                    index=False)
    
                    # 輸出 formula
    
                    index = [stats["features"].index(
                        feature) for feature in stats[class_]["features"]]
                    features = np.array(stats["features"])[index]
                    mean_ = np.array(stats["mean"])[index]
                    std_ = np.array(stats["std"])[index]
    
                    all = ""
                    for pc in range(stats[class_]["PC"]):
                        str = ""
                        for num in range(len(index)):
                            str += "+ (%.10f * (([%s] - (%.10f)) / %.10f)) " % (
                                stats[class_]["eigenvector"][pc][num], Program.__transToE3Name(
                                    features[num]), mean_[num],
                                std_[num])
                        str = str[1:]
                        str = "(( %s )^2) * %.10f" % (str,
                                                      stats[class_]["eigenvalue_matrix_inv"][pc][pc])
                        all += "+ " + str
                    all = all[1:]
    
                    with open(os.path.join(path, class_, "%s_formula.txt" % (class_)), "w", encoding="utf-8") as f:
                        f.write(all)
    
                    sub = data_all[['DateTime', 'Alias', "Recipe"] +
                                   stats[class_]["features"]]
                    nor_sub = nor[stats[class_]["features"]]
                    nor_pca = np.dot(nor_sub, stats[class_]['eigenvector'].T)
                    dataset = pd.concat([sub, pd.DataFrame(nor_pca, columns=[
                        'PC{}'.format(i + 1) for i in range(nor_pca.shape[1])])], axis=1)
                    # 求出T2
                    nor_pca = nor_pca ** 2
                    t2 = np.dot(nor_pca, stats[class_]['eigenvalue_matrix_inv'])
                    pca_t2 = np.sum(t2, axis=1)
                    dataset['T2'] = pca_t2
    
                    # 找到change point
                    signal = np.array(Program.__mov_median(dataset["T2"], 5))
                    algo = rpt.Binseg(model="l2").fit(signal)
                    try:
                        result = algo.predict(n_bkps=1)
                    except:
                        raise ValueError(
                            "moving median 5 後資料長度只有 %d , 不足以找到 change point" % len(signal))
                    result = result[:-1][0]
    
                    # 可視化
    
                    date_start = pd.to_datetime(training_starttime)
                    date = pd.to_datetime(training_endtime)
                    # 紀錄每個特徵的change_pt
                    change_pt = {"features": [], "pt": []}
                    # feature
                    for feature in dataset.columns[3:]:
                        if "__" in feature:
                            signal = np.array(
                                Program.__mov_median(
                                    dataset[feature], 5))
                            algo = rpt.Binseg(model="l2").fit(signal)
                            # feature 前三個 change point (未來)
                            result2 = algo.predict(n_bkps=1)
                            result2 = result2[:-1][0]
                            change_pt["features"].append(feature)
                            change_pt["pt"].append(result2)
    
                            # 畫features
                            fig = plt.figure(
                                facecolor="lightgrey", figsize=(12, 8))
                            plt.title("%s" % feature)
                            plt.scatter(
                                dataset["DateTime"],
                                dataset[feature],
                                s=1,
                                label="raw")
                            plt.plot(dataset["DateTime"][4:], Program.__mov_median(
                                dataset[feature], 5), color="blue", label="mmd5")
                            plt.legend(loc="upper left")
                            ax = plt.gca()
                            ax.xaxis.set_major_locator(
                                mdate.MonthLocator(interval=1))
                            ax.tick_params(labelrotation=90)
                            plt.axvline(x=date_start, color="red")
                            plt.axvline(x=date, color="red")
    
                            max_ = np.max(Program.__mov_median(
                                dataset[feature], 5))
                            min_ = np.min(Program.__mov_median(
                                dataset[feature], 5))
                            range_ = max_ - min_
                            plt.ylim(min_ - 0.05 * range_, max_ + 0.05 * range_)
                            plt.annotate('Made by HA33 module', xy=(0.5, 0.1), xycoords='axes fraction',
                                          fontsize=20, color='black', alpha=0.5,
                                          ha='center', va='center')
                            plt.legend()
                            plt.savefig(
                                os.path.join(
                                    path,
                                    class_,
                                    "features",
                                    "%s.png" % Program.trans_filename(feature)))
    
                            plt.close(fig)
                    change_pt = pd.DataFrame(change_pt)
                    # 設定change pt區間 區間內都算該pt
                    n = 2 * len(dataset) // 100
                    change_pt = change_pt[(change_pt["pt"] >= (
                        result - n)) & (change_pt["pt"] <= (result + n))]
    
                    lst = []
                    for f, pt in zip(change_pt["features"], change_pt["pt"]):
                        d = Program.__mov_median(dataset[f], 5)
                        n = min(len(d[:pt]), len(d[pt:]))
                        # change pt 左右改變ratio
                        score = (max(np.mean(d[pt - n:pt]),
                                     np.mean(d[pt:pt + n])) - min(np.mean(d[pt - n:pt]),
                                                                  np.mean(d[pt:pt + n]))) / abs(min(np.mean(d[pt - n:pt]),
                                                                                                    np.mean(d[pt:pt + n])))
                        lst.append(score)
                    change_pt["score"] = lst
                    change_pt = change_pt.sort_values(
                        "score", ascending=False).reset_index(
                        drop=True)
    
                    for f, pt in zip(change_pt["features"].head(
                            KEYFEATURES), change_pt["pt"].head(KEYFEATURES)):
                        fig = plt.figure(facecolor="lightgrey", figsize=(12, 8))
                        plt.title("%s" % f)
                        plt.scatter(dataset["DateTime"],
                                    dataset[f], s=1, label="raw")
                        plt.plot(dataset["DateTime"][4:], Program.__mov_median(
                            dataset[f], 5), color="blue", label="mmd5")
                        plt.legend(loc="upper left")
                        ax = plt.gca()
                        ax.xaxis.set_major_locator(mdate.MonthLocator(interval=1))
                        ax.tick_params(labelrotation=90)
                        plt.axvline(x=date_start, color="red")
                        plt.axvline(x=date, color="red")
                        plt.axvline(x=dataset["DateTime"][pt], color="orange")
                        max_ = np.max(Program.__mov_median(dataset[f], 5))
                        min_ = np.min(Program.__mov_median(dataset[f], 5))
                        range_ = max_ - min_
                        plt.ylim(min_ - 0.05 * range_, max_ + 0.05 * range_)
                        plt.annotate('Made by HA33 module', xy=(0.5, 0.1), xycoords='axes fraction',
                                      fontsize=20, color='black', alpha=0.5,
                                      ha='center', va='center')
                        plt.legend()
                        plt.savefig(
                            os.path.join(
                                path,
                                class_,
                                "key_features",
                                "%s.png" %
                                Program.trans_filename(f)))
                        plt.close(fig)
    
                    feature = "T2"
                    fig = plt.figure(facecolor="lightgrey", figsize=(12, 8))
                    plt.title("%s T2" % class_)
                    plt.scatter(
                        dataset["DateTime"],
                        dataset[feature],
                        s=1,
                        label="raw")
                    plt.plot(dataset["DateTime"][4:], Program.__mov_median(
                        dataset[feature], 5), color="blue", label="mmd5")
                    plt.legend(loc="upper left")
                    ax = plt.gca()
                    ax.xaxis.set_major_locator(mdate.MonthLocator(interval=1))
                    ax.tick_params(labelrotation=90)
                    plt.axvline(x=date_start, color="red")
                    plt.axvline(x=date, color="red")
                    plt.axvline(x=dataset["DateTime"][result + 4], color="orange")
                    plt.annotate('Made by HA33 module', xy=(0.5, 0.1), xycoords='axes fraction',
                                  fontsize=20, color='black', alpha=0.5,
                                  ha='center', va='center')
    
                    plt.legend()
                    plt.ylim(
                        0, max(
                            Program.__mov_median(
                                dataset[feature], 5)) * 1.2)
    
                    plt.savefig(os.path.join(
                        path, class_, "t2", "%s.png" % class_))
                    plt.close(fig)
    
                    dataset.to_csv(
                        os.path.join(
                            path,
                            class_,
                            "%s.csv" %
                            class_),
                        index=False)
            except Exception as e:
                print(f"處理發生錯誤: {e}")

    def run(self, training_starttime, training_endtime):

        uri = 7777
        chart_uri = 7676
        project_type = "PCAT2"
        section = self.e.sec
        eqid = self.eqip
        chamber = self.chamber
        recipe = self.recipe[0]


        data = self.fdc_process[(self.fdc_process["DateTime"] >= pd.to_datetime(training_starttime)) & (
            self.fdc_process["DateTime"] <= pd.to_datetime(training_endtime))].reset_index(drop=True)
        rm = data.iloc[:, 3:].std(
        )[data.iloc[:, 3:].std() < 0.00000001].index
        data = data.drop(rm, axis=1)

        sel_features = []
        for svid in self.map['Name']:
            try:
                svid_features = Program.select(data, svid)
                sel_features += svid_features
            except Exception as e:
                print(f"發生錯誤: {e}")

        stats = Program.train(data, self.map, sel_features)
        path = os.path.join(
            "%s %s %s" % (time.strftime("%Y%m%d%H%M%S",
                          time.localtime()), eqid, chamber),
            "multProg")
        # 挑選要撈 parameter x svid 的時間
        month_ = str(list(data["DateTime"])[-1]
                   ).split(" ")[0].replace("-", "")[:6]
        svid_table = Program.query_svid(
            eqid, chamber, self.recipe, month_)
        Program.t2_trans(self.fdc_process, stats, path,
                         self.map, svid_table, training_starttime, training_endtime)

        params = {"eqid": eqid, "chamber": chamber, "recipe": self.recipe,
                  "start time": str(list(self.fdc["DateTime"])[0]).split(" ")[0],
                  "end time": str(list(self.fdc["DateTime"])[-1]).split(" ")[0],
                  "train start time": training_starttime,
                  "train end time": training_endtime,
                  "cnts": len(self.fdc["Alias"].unique())}

        metrics = {"Multi_Prog": 0}
        artifacts = [path]
        artifacts_show = {}
        
        for class_ in self.map["GroupName"].unique():
            file_path = os.path.join(path, class_, "t2", class_ + ".png")
            if os.path.exists(file_path):  # 確保檔案存在才加入 artifacts_show
                artifacts_show[class_] = file_path
        
        mlc = MLCustomOutputs(uri, chart_uri, project_type, section, eqid, chamber, recipe)
        mlc.record(params, metrics, artifacts, artifacts_show)
        mlc.set_tag("MLflow Record Done")

        print("Mult Prog 分析完成")




