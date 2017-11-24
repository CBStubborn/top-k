package thesis.topk.monitoring.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.MsgField;
import thesis.topk.monitoring.common.StreamIds;
import thesis.topk.monitoring.common.TopKConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class TrafficFlowSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(TrafficFlowSpout.class);

    private SpoutOutputCollector collector;

    private BufferedReader bufferedReader;

    private FileReader fileReader;

    private Set<String> locationSet;

    private List<Integer> nodeIdList;

    private int nodeSize;

    private AtomicLong totals = new AtomicLong(0);

    private static Random random;

    public static AtomicBoolean start = new AtomicBoolean(true);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamIds.DATASOURCE, new Fields(MsgField.OBJ_ID, MsgField.OBJ_TIMESTAMP));
        locationSet = new HashSet<>();
        random = new Random();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        nodeIdList = context.getComponentTasks("monitoring-node");
        LOG.info("monitoring node list: [{}]", nodeIdList.toString());
        nodeSize = nodeIdList.size();
        try {
            this.fileReader = new FileReader(TopKConfig.get(ConfigName.TRAFFIC_DATA_FILE));
            this.bufferedReader = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            LOG.error("can't find file ", e);
        }
    }

    @Override
    public void nextTuple() {
        while (true) {
            if (start.get()) {
                LOG.debug("permit send source data");
                break;
            } else {
                LOG.info("forbidden to send source data");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
            }
        }
        String line;
        try {
            line = bufferedReader.readLine();
            if (line != null && !"".equals(line)) {
                List data = new ArrayList();
                String[] arrays = line.split(";");
                String bayId = arrays[0];
                long timestamp = Long.valueOf(arrays[1]);
                locationSet.add(bayId);
                data.add(bayId);
                data.add(timestamp);
                int nodeIdIndex = (int) (totals.getAndIncrement() % nodeSize);
                int nodeId = nodeIdList.get(nodeIdIndex);
                LOG.debug("emitting a data record: [{}] to monitoring node:[{}]", data, nodeId);
                collector.emitDirect(nodeId, StreamIds.DATASOURCE, data);
                Thread.sleep(3);
            }
        } catch (IOException e) {
            LOG.error("nextTuple io  exception", e);
        } catch (InterruptedException e) {
            LOG.error(" in nextTuple", e);
        }
    }

    @Override
    public void close() {
        LOG.info("close open file");
        try {
            this.bufferedReader.close();
            this.fileReader.close();
        } catch (IOException e) {
            LOG.error("close file error", e);
        }
    }
}