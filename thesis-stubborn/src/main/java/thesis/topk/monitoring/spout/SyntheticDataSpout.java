package thesis.topk.monitoring.spout;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.common.MsgField;
import thesis.topk.monitoring.common.StreamIds;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class SyntheticDataSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SyntheticDataSpout.class);

    private SpoutOutputCollector collector;

    private long beginTime;

    private long period = 150l;

    private List<Integer> nodeIdList;

    private int nodeSize;

    private AtomicLong totals = new AtomicLong(0);

    public static AtomicBoolean start = new AtomicBoolean(true);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamIds.DATASOURCE, new Fields(MsgField.OBJ_ID, MsgField.OBJ_TIMESTAMP));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        beginTime = System.currentTimeMillis();
        nodeIdList = context.getComponentTasks("monitoring-node");
        LOG.info("monitoring node list: [{}]", nodeIdList.toString());
        nodeSize = nodeIdList.size();
    }

    @Override
    public void nextTuple() {
        while (true) {
            if (start.get()) {
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
            }
        }
        List data = new ArrayList();
        int zv = Distributions.nextZipfInt(1.8, RandomEngine.makeDefault());
        String obj = "NO." + zv;
        data.add(obj);
        data.add(beginTime);
        int nodeIdIndex = (int) (totals.getAndIncrement() % nodeSize);
        int nodeId = nodeIdList.get(nodeIdIndex);
        LOG.debug("emitting a data record: [{}] to monitoring node: [{}]", data, nodeId);
        collector.emitDirect(nodeId, StreamIds.DATASOURCE, data);
        beginTime += period;
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            LOG.error("synthetic next tuple interrupt", e);
        }
    }
}