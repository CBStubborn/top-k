package thesis.topk.monitoring.manager;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.NodeViolationBundle;
import thesis.topk.monitoring.common.TopKConfig;
import thesis.topk.monitoring.utils.CostUtil;
import thesis.topk.monitoring.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class GlobalInfoManager {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalInfoManager.class);

    private AtomicBoolean isViolating;

    private Map<Integer, NodeViolationBundle> violateNodeMap;

    private ResolutionManager resolutionManager;

    private int K;

    private Object object = new Object();

    public GlobalInfoManager(OutputCollector collector, List<Integer> nodeIdList, int K, int errorFactor) {
        this.violateNodeMap = new ConcurrentHashMap<>();
        this.K = K;
        this.isViolating = new AtomicBoolean(false);
        this.resolutionManager = new ResolutionManager(collector, nodeIdList, K, errorFactor);
        createAThread();
    }

    private void createAThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    if ((CostUtil.getProcessed() != 0) && CostUtil.getProcessed() % TopKConfig.getInt(ConfigName.MONITORING_NODE_NUM) == 0) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {

                        }
                        Map<Integer, NodeViolationBundle> cacheMap = new HashMap<>(violateNodeMap);
                        violateNodeMap = new ConcurrentHashMap<>();
                        resolutionManager.handleViolation(cacheMap);
                        synchronized (object) {
                            try {
                                object.wait();
                            } catch (InterruptedException e) {

                            }
                        }
                    }
                }
            }
        }).start();
    }

    public void registerViolateNode(Tuple tuple) {
        synchronized (object) {
            object.notify();
        }
        LOG.debug("registering a violate monitoring node: [{}]", tuple.getSourceTask());
        NodeViolationBundle bundle = new NodeViolationBundle().builder(tuple);
        violateNodeMap.put(tuple.getSourceTask(), bundle);
    }

    /**
     * 当调用全局解决算法时，中心处理节点向监控节点发送请求，以获取监控节点local info,
     * 监控节点发送local info后，中心节点调用此方法以接收监控节点发来的local info
     *
     * @param tuple
     */
    public void receiveLocalInfo(Tuple tuple) {
        LOG.debug("receiving local info from monitoring node: [{}]", tuple.getSourceTask());
        resolutionManager.handleLocalInfo(tuple);
    }

    public void receiveLocalBound(Tuple tuple) {
        LOG.debug("receiving local bound from monitoring node: [{}]", tuple.getSourceTask());
        resolutionManager.handleLocalBound(tuple);
    }

    /**
     * 将每个对象在各个监控节点中的值求和
     *
     * @param nodeCountMap key:monitoring node id, value:一个监控节点中，
     *                     一个大的时间窗口W内的数据对象及其出现次数映射的初始值
     * @return 对象-在所有监控节点出现总次数映射
     */
    private Map<String, Long> unitObjectCountMap(Map<Integer, Map<String, Long>> nodeCountMap) {
        Map<String, Long> unitObjectCountMap = new HashMap<>();
        for (Integer nodeId : nodeCountMap.keySet()) {
            unitObjectCountMap = MapUtil.combine(unitObjectCountMap, nodeCountMap.get(nodeId));
        }
        return unitObjectCountMap;
    }

    /**
     * 当每个监控节点都向主节点发送初始化的“对象-出现次数”映射后，调用此方法
     *
     * @param nodeCountMap
     */
    public void init(Map<Integer, Map<String, Long>> nodeCountMap) {
        Map<String, Long> unitObjectCountMap = unitObjectCountMap(nodeCountMap);
        Map<String, Long> newTopKMap = MapUtil.findTopK(unitObjectCountMap, K, null);
        LOG.info("topKMap = {}", newTopKMap);
        Set<String> topKSet = newTopKMap.keySet();
        Map<Integer, Map<String, Long>> initFactorMap = new HashMap<>();
        for (Integer nodeId : nodeCountMap.keySet()) {
            initFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        resolutionManager.initTopKSet(topKSet);
        resolutionManager.initNodeFactorMap(initFactorMap);
        resolutionManager.assignInfoToNode(topKSet, initFactorMap, true);
    }
}