package me.allenlyu.experiments.common;

/**
 * ConfigName
 *
 * @author Allen Jin
 * @date 2017/02/15
 */
public abstract class ConfigName {
    //运行模式,本地或集群
    public static final String STORM_IS_LOCAL = "storm_is_local";
    public static final String STORM_IS_DEBUG = "storm_is_debug";
    //工作节点数
    public static final String STORM_WORKER_NUM = "storm_work_num";

    public static final String K = "k";

    //数据文件
    public static final String TRAFFIC_DATA_FILE = "traffic_data_file";

    //监控窗口大小
    public static final String WINDOW_SIZE = "window_size";

    //滑动单元大小
    public static final String WINDOW_UNIT_SIZE = "window_unit_size";

    //监控节点数
    public static final String MONITORING_NODE_NUM = "monitoring_node_nums";

    //数据起始时间戳
    public static final String BEGIN_TIME = "begin_time";

    //允许误差
    public static final String ERROR_FACTOR = "error_factor";

    public static final String WITH_SLACK = "with_slack";

}