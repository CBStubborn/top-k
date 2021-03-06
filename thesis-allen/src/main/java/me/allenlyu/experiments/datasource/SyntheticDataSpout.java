package me.allenlyu.experiments.datasource;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;
import me.allenlyu.experiments.common.MsgField;
import me.allenlyu.experiments.common.StreamIds;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SyntheticDataSpout
 *
 * @author Allen Jin
 * @date 2017/02/15
 */
public class SyntheticDataSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticDataSpout.class);
    private SpoutOutputCollector collector;
    private Random random = new Random();
    private Map<String, Long> map = new HashMap<>();
    private long beginTime;
    private long period = 150l;
    private List<Integer> nodeIdList;
    private int nodeSize;
    private AtomicLong totals = new AtomicLong(0);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamIds.DATASOURCE, new Fields(MsgField.OBJ_ID, MsgField.OBJ_TIMESTAMP));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
//        beginTime = TopKConfig.getLong(ConfigName.BEGIN_TIME);
        beginTime = System.currentTimeMillis();
        nodeIdList = context.getComponentTasks("monitoring-node");
        nodeSize = nodeIdList.size();
    }

    @Override
    public void nextTuple() {
        List data = new ArrayList();
        int zv = Distributions.nextZipfInt(1.8, RandomEngine.makeDefault());
        String obj = "NO." + zv;
        data.add(obj);
        data.add(beginTime);
        int nodeIdIndex = (int) (totals.getAndIncrement() % nodeSize);
        int nodeId = nodeIdList.get(nodeIdIndex);
        collector.emitDirect(nodeId, StreamIds.DATASOURCE, data);
//        if (map.get(obj) == null) {
//            map.put(obj, 1L);
//        } else {
//            map.put(obj, map.get(obj) + 1L);
//        }
        beginTime += period;
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            LOG.error("synthetic next tuple interrupt", e);
        }
    }
}
