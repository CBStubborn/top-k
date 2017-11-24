package thesis.topk.monitoring;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.bolt.CoordinatorNodeBolt;
import thesis.topk.monitoring.bolt.MonitoringNodeBolt;
import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.StreamIds;
import thesis.topk.monitoring.spout.SyntheticDataSpout;
import thesis.topk.monitoring.spout.TrafficFlowSpout;
import thesis.topk.monitoring.common.TopKConfig;

/**
 * Created by Stubborn on 2017/7/28.
 */
public class TopKTopology {

    public static final Logger LOG = LoggerFactory.getLogger(TopKTopology.class);

    public static void main(String[] args) throws InterruptedException,
            InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setDebug(TopKConfig.getBoolean(ConfigName.STORM_IS_DEBUG));
        config.setMaxSpoutPending(1);
        TopologyBuilder builder = new TopologyBuilder();
        if (TopKConfig.getBoolean(ConfigName.IS_REAL_DATA_SET)) {
            //交通数据
            builder.setSpout("monitoring-datasource", new TrafficFlowSpout(), 1);
        } else {
            //人造数据
            builder.setSpout("monitoring-datasource", new SyntheticDataSpout(), 1);
        }
        builder.setBolt("monitoring-node", new MonitoringNodeBolt(), TopKConfig.getInt(ConfigName.MONITORING_NODE_NUM))
                .directGrouping("monitoring-datasource", StreamIds.DATASOURCE)
                .directGrouping("coordinator-node", StreamIds.NEW_CONSTRAINTS)
                .directGrouping("coordinator-node", StreamIds.NEW_GLOBAL_TOPK)
                .directGrouping("coordinator-node", StreamIds.GLOBAL_REQUEST);
        builder.setBolt("coordinator-node", new CoordinatorNodeBolt(), 1)
                .allGrouping("monitoring-node", StreamIds.INIT)
                .allGrouping("monitoring-node", StreamIds.LOCAL_VIOLATE)
                .allGrouping("monitoring-node", StreamIds.LOCAL_BOUND)
                .allGrouping("monitoring-node", StreamIds.LOCAL_INFO);

        if (TopKConfig.getBoolean(ConfigName.STORM_IS_LOCAL)) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("TopK_Topology", config, builder.createTopology());
            Thread.sleep(2 * 60 * 60 * 1000l);
            cluster.shutdown();
        } else {
            config.setNumWorkers(TopKConfig.getInt(ConfigName.STORM_WORKER_NUM));
            StormSubmitter.submitTopologyWithProgressBar("TopK-Topology", config, builder.createTopology());
        }
    }
}