package thesis.topk.monitoring.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.MsgField;
import thesis.topk.monitoring.common.StreamIds;
import thesis.topk.monitoring.common.TopKConfig;
import thesis.topk.monitoring.manager.LocalInfoManager;
import thesis.topk.monitoring.manager.WindowManager;
import thesis.topk.monitoring.spout.SyntheticDataSpout;
import thesis.topk.monitoring.spout.TrafficFlowSpout;
import thesis.topk.monitoring.utils.CostUtil;

import java.util.Map;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class MonitoringNodeBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoringNodeBolt.class);

    private WindowManager windowManager;

    private LocalInfoManager localInfoManager;

    private int taskId;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.taskId = context.getThisTaskId();
        localInfoManager = new LocalInfoManager(collector, taskId);
        windowManager = new WindowManager(collector, localInfoManager, taskId);
    }

    @Override
    public void execute(Tuple input) {
        switch (input.getSourceStreamId()) {
            //数据源处理
            case StreamIds.DATASOURCE:
                String obj = input.getString(0);
                long timestamp = input.getLong(1);
                LOG.debug("monitoring node: [{}] receiving data source message [ obj={}, time={}]",
                        taskId, obj, timestamp);
                windowManager.putRecord(obj, timestamp);
                if ((CostUtil.getProcessed() != 0) && (CostUtil.getProcessed() % TopKConfig.getInt(ConfigName.MONITORING_NODE_NUM) == 0)) {
                    LOG.info("waiting for coordinator node process last violation");
                    LOG.info("closing data source");
                    if (TopKConfig.getBoolean(ConfigName.IS_REAL_DATA_SET)) {
                        TrafficFlowSpout.start.set(false);
                        LOG.info("start is: [{}]", TrafficFlowSpout.start.get());
                    } else {
                        SyntheticDataSpout.start.set(false);
                    }

                }
                break;

            //中心处理节点请求监控节点发送本地数据
            case StreamIds.GLOBAL_REQUEST:
                LOG.debug("monitoring node: [{}] receiving global request message", taskId);
                CostUtil.inc();
                localInfoManager.emitLocalInfo(input);
                break;

            case StreamIds.NEW_GLOBAL_TOPK:
                LOG.debug("monitoring node: [{}] receiving new global topk", taskId);
                localInfoManager.updateNewGlobalTopKAndEmitLocalBound(input);
                break;

            //更新本地约束
            case StreamIds.NEW_CONSTRAINTS:
                LOG.debug("monitoring node: [{}] receiving new constraints message", taskId);
                CostUtil.inc();
                localInfoManager.updateNewInfo(input);
                break;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamIds.INIT, new Fields(MsgField.LOCAL_COUNT_MAP));
        declarer.declareStream(StreamIds.LOCAL_VIOLATE, new Fields(MsgField.VIOLATE_SET,
                MsgField.LOCAL_COUNT_MAP, MsgField.LOCAL_BOUND_VAL));
        declarer.declareStream(StreamIds.LOCAL_INFO, new Fields(MsgField.LOCAL_COUNT_MAP, MsgField.LOCAL_BOUND_VAL));
        declarer.declareStream(StreamIds.LOCAL_BOUND, new Fields(MsgField.LOCAL_BOUND_VAL));
    }

}