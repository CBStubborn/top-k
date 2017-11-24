package me.allenlyu.experiments;

import me.allenlyu.experiments.common.ConfigName;
import me.allenlyu.experiments.common.MsgField;
import me.allenlyu.experiments.common.StreamIds;
import me.allenlyu.experiments.manager.GlobalInfoManager;
import me.allenlyu.experiments.utils.CostUtils;
import me.allenlyu.experiments.utils.TopKConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * CoordinatorNode
 *
 * @author Allen Jin
 * @date 2017/02/15
 */
public class CoordinatorNode extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorNode.class);
    private CountDownLatch initLatch;
    private Map<Integer, Map<String, Long>> itemCountMap;
    private int K;
    private GlobalInfoManager globalInfoManager;

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
                    LOG.debug("init global top k");
                    globalInfoManager.init(itemCountMap);
                } catch (InterruptedException e) {
                    LOG.error("init latch interrupted", e);
                }
            }
        }).start();
    }

    @Override
    public void execute(Tuple input) {
        switch (input.getSourceStreamId()) {
            case StreamIds.INIT:
                Map<String, Long> localCountMap = (Map<String, Long>) input.getValueByField(MsgField.LOCAL_COUNT_MAP);
                LOG.info("monitorNodeId = {}, map = {}", input.getSourceTask(), localCountMap);
                itemCountMap.put(input.getSourceTask(), localCountMap);
                initLatch.countDown();
                break;
            case StreamIds.LOCAL_VIOLATE:
                CostUtils.inc();
                globalInfoManager.registerViolateNode(input);
                break;
            case StreamIds.LOCAL_INFO:
                CostUtils.inc();
                globalInfoManager.receiveLocalInfo(input);
                break;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamIds.NEW_CONSTRAINTS, new Fields(MsgField.GLOBAL_TOP_K, MsgField.REVISION_MAP));
        declarer.declareStream(StreamIds.GLOBAL_REQUEST, new Fields(MsgField.RESOLVE_SET));
//        declarer.declareStream(StreamIds.ONE_QUERY_REQUEST, new Fields());
    }
}
