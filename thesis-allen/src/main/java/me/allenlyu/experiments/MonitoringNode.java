package me.allenlyu.experiments;

import me.allenlyu.experiments.common.MsgField;
import me.allenlyu.experiments.common.StreamIds;
import me.allenlyu.experiments.manager.LocalInfoManager;
import me.allenlyu.experiments.manager.WindowManager;
import me.allenlyu.experiments.utils.CostUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * MonitoringNode
 *
 * @author Allen Jin
 * @date 2017/02/15
 */
public class MonitoringNode extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MonitoringNode.class);
    private OutputCollector collector;
    private WindowManager windowManager;
    private LocalInfoManager localInfoManager;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        localInfoManager = new LocalInfoManager(collector);
        windowManager = new WindowManager(collector, localInfoManager);
    }

    @Override
    public void execute(Tuple input) {
        switch (input.getSourceStreamId()) {
            //数据源处理
            case StreamIds.DATASOURCE:
                String obj = input.getString(0);
                long timestamp = input.getLong(1);
                windowManager.putRecord(obj, timestamp);
                break;

            //发送本地数据
            case StreamIds.GLOBAL_REQUEST:
                CostUtils.inc();
                localInfoManager.emitLocalInfo(input);
                break;

            //更新本地约束
            case StreamIds.NEW_CONSTRAINTS:
                CostUtils.inc();
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
    }
}
