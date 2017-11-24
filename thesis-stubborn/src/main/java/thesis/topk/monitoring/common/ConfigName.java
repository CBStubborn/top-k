package thesis.topk.monitoring.common;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class ConfigName {

    //运行模式,本地或集群
    public static final String STORM_IS_LOCAL = "storm_is_local";

    //debug模式
    public static final String STORM_IS_DEBUG = "storm_is_debug";

    //工作节点数
    public static final String STORM_WORKER_NUM = "storm_work_num";

    //k取值大小
    public static final String K = "k";

    //是否使用真实数据
    public static final String IS_REAL_DATA_SET = "is_real_data_set";

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

    //coordinator node节点维护slack值情况，可选值为：without, part, all
    public static final String WITH_SLACK = "with_slack";

    //算法一计算松驰值时是采用一般算法还是更高效算法
    public static final String IS_PLAIN = "is_plain";

}