package me.allenlyu.experiments;

import me.allenlyu.experiments.common.ConfigName;
import me.allenlyu.experiments.common.StreamIds;
import me.allenlyu.experiments.datasource.SyntheticDataSpout;
import me.allenlyu.experiments.utils.TopKConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TopKTopology
 *
 * @author Allen Jin
 * @date 2017/02/15
 */
public class TopKTopology {
    public static final Logger LOG = LoggerFactory.getLogger(TopKTopology.class);

    public static void main(String[] args) throws InterruptedException,
            InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();

        config.setDebug(TopKConfig.getBoolean(ConfigName.STORM_IS_DEBUG));
//        config.setMaxSpoutPending(1);
//        人造数据
        builder.setSpout("monitoring-datasource", new SyntheticDataSpout(), 1);
        //交通数据
       // builder.setSpout("monitoring-datasource", new TrafficFlowSpout(), 1);
//
        builder.setBolt("monitoring-node", new MonitoringNode(), TopKConfig.getInt(ConfigName.MONITORING_NODE_NUM))
                .directGrouping("monitoring-datasource", StreamIds.DATASOURCE)
                .directGrouping("coordinator-node", StreamIds.NEW_CONSTRAINTS)
                .directGrouping("coordinator-node", StreamIds.GLOBAL_REQUEST);
//
        builder.setBolt("coordinator-node", new CoordinatorNode(), 1)
                .allGrouping("monitoring-node", StreamIds.INIT)
                .allGrouping("monitoring-node", StreamIds.LOCAL_VIOLATE)
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
