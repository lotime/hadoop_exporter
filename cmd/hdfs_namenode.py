#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
import re
import time
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily, REGISTRY

import utils
from utils import get_module_logger
from consul import Consul
from common import MetricCol, common_metrics_info

logger = get_module_logger(__name__)


class NameNodeMetricCollector(MetricCol):

    def __init__(self, cluster, url):
        # 手动调用父类初始化，传入cluster名称、jmx url、组件名称、服务名称
        # 注意：服务名称应与JSON配置的文件夹名称保持一致
        MetricCol.__init__(self, cluster, url, "hdfs", "namenode")
        self._clear_init()

    def _clear_init(self):
        self._hadoop_namenode_metrics = {}
        for i in range(len(self._file_list)):
            # 读取JSON配置文件，设置每个导出指标对象
            self._hadoop_namenode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        self._clear_init()
        # 发送HTTP请求从JMX URL中获取指标数据。
        # 获取JMX中对应bean JSON数组。
        try:
            # 发起HTTP请求JMX JSON数据
            beans = utils.get_metrics(self._url)
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
            pass
        else:
            # 设置监控需要关注的每个MBean，并设置好指标对应的标签以及描述
            self._setup_metrics_labels(beans)
    
            # 设置每个指标值
            self._get_metrics(beans)
    
            # 将通用的指标更新到NameNode对应的指标中
            common_metrics = common_metrics_info(self._cluster, beans, "hdfs", "namenode")
            self._hadoop_namenode_metrics.update(common_metrics())
    
            # 遍历每一个指标分类（包含NameNode以及Common的指标分类）
            # 返回每一个指标和标签
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_namenode_metrics[service]:
                    yield self._hadoop_namenode_metrics[service][metric]

    def _setup_nnactivity_labels(self):
        # 记录是否已处理（1表示需要处理，0表示无需处理）
        num_namenode_flag,avg_namenode_flag,ops_namenode_flag = 1,1,1
        # 遍历NameNodeActivity MBean对应的指标
        for metric in self._metrics['NameNodeActivity']:
            # 对指标名称进行处理（驼峰式转下划线分隔名称）
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            # 提前定义预先的label
            label = ["cluster", "method"]
            # 按照MBean中的后缀做分类，生成指标名称、以及对应的label
            # 例如：hadoop_hdfs_namenode_nnactivity_method_avg_time_milliseconds{cluster="hadoop-ha",method="BlockReport"}
            if "NumOps" in metric:
                if num_namenode_flag:
                    key = "MethodNumOps"
                    # 构建Guage类型指标
                    # 第一个参数为指标名称
                    # 第二个参数为指标描述信息
                    # 第三个参数为标签
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, "nnactivity_method_ops_total"]),
                                                                                               "Total number of the times the method is called.",
                                                                                               labels=label)
                    # 设置为0，表示下一次同样类型的指标不做处理
                    num_namenode_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_namenode_flag:
                    key = "MethodAvgTime"
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, "nnactivity_method_avg_time_milliseconds"]),
                                                                                               "Average turn around time of the method in milliseconds.",
                                                                                               labels=label)
                    avg_namenode_flag = 0
                else:
                    continue
            else:
                # 如果没有进行指标分类维度化，则统一放到nnactivity_operations_total中存储
                if ops_namenode_flag:
                    ops_namenode_flag = 0
                    key = "Operations"
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, "nnactivity_operations_total"]),
                                                                                               "Total number of each operation.",
                                                                                               labels=label)
                else:
                    continue

    def _setup_startupprogress_labels(self):
        sp_count_flag,sp_elapsed_flag,sp_total_flag,sp_complete_flag = 1,1,1,1
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "ElapsedTime" == metric:
                key = "ElapsedTime"
                name = "total_elapsed_time_milliseconds"
                descriptions = "Total elapsed time in milliseconds."
            elif "PercentComplete" == metric:
                key = "PercentComplete"
                name = "complete_rate"
                descriptions = "Current rate completed in NameNode startup progress  (The max value is not 100 but 1.0)."
             
            elif "Count" in metric:                    
                if sp_count_flag:
                    sp_count_flag = 0
                    key = "PhaseCount"
                    name = "phase_count"
                    label = ["cluster", "phase"]
                    descriptions = "Total number of steps completed in the phase."
                else:
                    continue
            elif "ElapsedTime" in metric:
                if sp_elapsed_flag:
                    sp_elapsed_flag = 0
                    key = "PhaseElapsedTime"
                    name = "phase_elapsed_time_milliseconds"
                    label = ["cluster", "phase"]
                    descriptions = "Total elapsed time in the phase in milliseconds."
                else:
                    continue
            elif "Total" in metric:
                if sp_total_flag:
                    sp_total_flag = 0
                    key = "PhaseTotal"
                    name = "phase_total"
                    label = ["cluster", "phase"]
                    descriptions = "Total number of steps in the phase."
                else:
                    continue
            elif "PercentComplete" in metric:
                if sp_complete_flag:
                    sp_complete_flag = 0
                    key = "PhasePercentComplete"
                    name = "phase_complete_rate"
                    label = ["cluster", "phase"]
                    descriptions = "Current rate completed in the phase  (The max value is not 100 but 1.0)."                    
                else:
                    continue                
            else:
                key = metric
                name = snake_case
                label = ["cluster"]
                descriptions = self._metrics['StartupProgress'][metric]            
            self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily("_".join([self._prefix, "startup_process", name]),
                                                                                      descriptions,
                                                                                      labels = label)

    def _setup_fsnamesystem_labels(self):
        cap_flag = 1
        for metric in self._metrics['FSNamesystem']:
            if metric.startswith('Capacity'):
                if cap_flag:
                    cap_flag = 0
                    key = "capacity"
                    label = ["cluster", "mode"]
                    name = "capacity_bytes"
                    descriptions = "Current DataNodes capacity in each mode in bytes"
                else:
                    continue    
            else:
                key = metric
                label = ["cluster"]
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                descriptions = self._metrics['FSNamesystem'][metric]
            self._hadoop_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily("_".join([self._prefix, "fsname_system", name]),
                                                                                   descriptions,
                                                                                   labels = label)

    def _setup_fsnamesystem_state_labels(self):
        num_flag = 1
        for metric in self._metrics['FSNamesystemState']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'DataNodes' in metric:
                if num_flag:
                    num_flag = 0
                    key = "datanodes_num"
                    label = ["cluster", "state"]
                    descriptions = "Number of datanodes in each state"
                else:
                    continue
            else:
                key = metric
                label = ["cluster"]
                descriptions = self._metrics['FSNamesystemState'][metric]
            self._hadoop_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily("_".join([self._prefix, "fsname_system", snake_case]),
                                                                                        descriptions,
                                                                                        labels = label)

    def _setup_retrycache_labels(self):
        cache_flag = 1
        for metric in self._metrics['RetryCache']:
            if cache_flag:
                cache_flag = 0
                key = "cache"
                label = ["cluster", "mode"]
                self._hadoop_namenode_metrics['RetryCache'][key] = GaugeMetricFamily("_".join([self._prefix, "cache_total"]), 
                                                                                     "Total number of RetryCache in each mode", 
                                                                                     labels = label)
            else:
                continue

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        # 遍历每一个MBean，设置需要关注的Label
        for i in range(len(beans)):
            # 只处理指定的MBean
            if 'NameNodeActivity' in beans[i]['name']:
                self._setup_nnactivity_labels()
    
            if 'StartupProgress' in beans[i]['name']:
                self._setup_startupprogress_labels()
                    
            if 'FSNamesystem' in beans[i]['name']:
                self._setup_fsnamesystem_labels()
    
            if 'FSNamesystemState' in beans[i]['name']:
                self._setup_fsnamesystem_state_labels()
    
            if 'RetryCache' in beans[i]['name']:
                self._setup_retrycache_labels()


    def _get_nnactivity_metrics(self, bean):
        # 遍历对应分类的所有指标
        for metric in self._metrics['NameNodeActivity']:
            # 不同的指标类别进行不同处理
            if "NumOps" in metric:
                # 获取method操作Label
                method = metric.split('NumOps')[0]
                # 设置Label
                label = [self._cluster, method]
                key = "MethodNumOps"
            elif "AvgTime" in metric:
                method = metric.split('AvgTime')[0]
                label = [self._cluster, method]
                key = "MethodAvgTime"
            else:
                if "Ops" in metric:
                    method = metric.split('Ops')[0]
                else:
                    method = metric
                label = [self._cluster, method]                    
                key = "Operations"

            # 调用promethues的add_metric，设置label值和metric值
            self._hadoop_namenode_metrics['NameNodeActivity'][key].add_metric(label,
                                                                              bean[metric] if metric in bean else 0)

    def _get_startupprogress_metrics(self, bean):
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "Count" in metric:
                key = "PhaseCount"
                phase = metric.split("Count")[0]
                label = [self._cluster, phase]
            elif "ElapsedTime" in metric and "ElapsedTime" != metric:
                key = "PhaseElapsedTime"
                phase = metric.split("ElapsedTime")[0]
                label = [self._cluster, phase]
            elif "Total" in metric:
                key = "PhaseTotal"
                phase = metric.split("Total")[0]
                label = [self._cluster, phase]
            elif "PercentComplete" in metric and "PercentComplete" != metric:
                key = "PhasePercentComplete"
                phase = metric.split("PercentComplete")[0]
                label = [self._cluster, phase]
            else:
                key = metric
                label = [self._cluster]
            self._hadoop_namenode_metrics['StartupProgress'][key].add_metric(label,
                                                                             bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_metrics(self, bean):
        for metric in self._metrics['FSNamesystem']:
            key = metric
            if 'HAState' in metric:
                label = [self._cluster]
                if 'initializing' == bean['tag.HAState']:
                    value = 0.0
                elif 'active' == bean['tag.HAState']:
                    value = 1.0
                elif 'standby' == bean['tag.HAState']:
                    value = 2.0
                elif 'stopping' == bean['tag.HAState']:
                    value = 3.0
                else:
                    value = 9999
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, value)
            elif metric.startswith("Capacity"):
                key = 'capacity'
                mode = metric.split("Capacity")[1]
                label = [self._cluster, mode]
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)
            else:
                label = [self._cluster]
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_state_metrics(self, bean):
        for metric in self._metrics['FSNamesystemState']:
            label = [self._cluster]
            key = metric
            if 'FSState' in metric:
                if 'Safemode' == bean['FSState']:
                    value = 0.0
                elif 'Operational' == bean['FSState']:
                    value = 1.0
                else:
                    value = 9999
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, value)
            elif "TotalSyncTimes" in metric:
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, float(re.sub('\s', '', bean[metric])) if metric in bean and bean[metric] else 0)
            elif "DataNodes" in metric:
                key = 'datanodes_num'
                state = metric.split("DataNodes")[0].split("Num")[1]
                label = [self._cluster, state]
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

            else:
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_retrycache_metrics(self, bean):
        for metric in self._metrics['RetryCache']:
            key = "cache"
            label = [self._cluster, metric.split('Cache')[1]]
            self._hadoop_namenode_metrics['RetryCache'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)


    def _get_metrics(self, beans):
        # 遍历每一个MBean
        for i in range(len(beans)):
            # 根据每个MBean进行相应处理
            if 'NameNodeActivity' in beans[i]['name']:
                self._get_nnactivity_metrics(beans[i])
            if 'StartupProgress' in beans[i]['name']:
                self._get_startupprogress_metrics(beans[i])
            if 'FSNamesystem' in beans[i]['name'] and 'FSNamesystemState' not in beans[i]['name']:
                self._get_fsnamesystem_metrics(beans[i])
            if 'FSNamesystemState' in beans[i]['name']:
                self._get_fsnamesystem_state_metrics(beans[i])
            if 'RetryCache' in beans[i]['name']:
                self._get_retrycache_metrics(beans[i])
                    


def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.namenode_url
        REGISTRY.register(NameNodeMetricCollector(cluster, v))

        start_http_server(port)
        # print("Polling %s. Serving at port: %s" % (args.address, port))
        print("Polling %s. Serving at port: %s" % (args.address, port))
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()