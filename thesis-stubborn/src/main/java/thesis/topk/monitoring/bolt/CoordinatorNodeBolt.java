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
import thesis.topk.monitoring.manager.GlobalInfoManager;
import thesis.topk.monitoring.utils.CostUtil;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class CoordinatorNodeBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorNodeBolt.class);

    private CountDownLatch initLatch;

    //key:monitoring node id, value:一个监控节点中，一个大的时间窗口W内的数据对象及其出现次数映射的初始值
    private Map<Integer, Map<String, Long>> itemCountMap;

    private GlobalInfoManager globalInfoManager;

    private int K;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        List<Integer> monitorNodeIdList = context.getComponentTasks("monitoring-node");
        this.initLatch = new CountDownLatch(monitorNodeIdList.size());
        this.K = TopKConfig.getInt(ConfigName.K);
        int errorFactor = TopKConfig.getInt(ConfigName.ERROR_FACTOR);
        this.itemCountMap = new HashMap<>();
        this.globalInfoManager = new GlobalInfoManager(collector, monitorNodeIdList, K, errorFactor);
        createInitThread();
    }

    private void createInitThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    initLatch.await();
                    LOG.info("init global top k");
                    globalInfoManager.init(itemCountMap);
                } catch (InterruptedException e) {
                    LOG.error("init latch interrupted", e);
                }
            }
        }).start();
    }

    @Override
    public void execute(Tuple input) {
        int nodeId = input.getSourceTask();
        switch (input.getSourceStreamId()) {
            case StreamIds.INIT:
                LOG.debug("coordinator node receiving init message from monitoring node: [{}]", nodeId);
                Map<String, Long> localCountMap = (Map<String, Long>) input.getValueByField(MsgField.LOCAL_COUNT_MAP);
                LOG.debug("monitorNodeId = {}, map = {}", input.getSourceTask(), localCountMap);
                itemCountMap.put(nodeId, localCountMap);
                initLatch.countDown();
                break;

            case StreamIds.LOCAL_VIOLATE:
                LOG.debug("coordinator node receiving local violate message from monitoring node: [{}]", nodeId);
                CostUtil.inc();
                globalInfoManager.registerViolateNode(input);
                break;

            case StreamIds.LOCAL_INFO:
                LOG.debug("coordinator node receiving local info message from monitoring node: [{}]", nodeId);
                CostUtil.inc();
                globalInfoManager.receiveLocalInfo(input);
                break;

            case StreamIds.LOCAL_BOUND:
                LOG.debug("coordinator node receiving local bound message from monitoring node: [{}]", nodeId);
                globalInfoManager.receiveLocalBound(input);
                break;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamIds.NEW_CONSTRAINTS, new Fields(MsgField.GLOBAL_TOP_K, MsgField.REVISION_MAP, MsgField.IS_INIT));
        declarer.declareStream(StreamIds.GLOBAL_REQUEST, new Fields(MsgField.RESOLVE_SET));
        declarer.declareStream(StreamIds.NEW_GLOBAL_TOPK, new Fields(MsgField.NEW_TOPK));
    }

}