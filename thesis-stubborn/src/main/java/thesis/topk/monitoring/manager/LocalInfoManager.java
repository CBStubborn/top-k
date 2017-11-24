package thesis.topk.monitoring.manager;

import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.MsgField;
import thesis.topk.monitoring.common.StreamIds;
import thesis.topk.monitoring.common.TopKConfig;
import thesis.topk.monitoring.spout.SyntheticDataSpout;
import thesis.topk.monitoring.spout.TrafficFlowSpout;
import thesis.topk.monitoring.utils.CostUtil;
import thesis.topk.monitoring.utils.MapUtil;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class LocalInfoManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalInfoManager.class);

    //全局TopK
    private Set<String> globalTopKSet;

    //调整参数
    private Map<String, Long> revisionFactorMap;

    private OutputCollector collector;

    private int taskId;

    //本地监控节点上，一个大的时间窗口W的所有对象及其出现次数的映射
    private Map<String, Long> nodeCountMap;

    private int K;

    private static BlockingQueue queue = new LinkedBlockingQueue<>(TopKConfig.getInt(ConfigName.MONITORING_NODE_NUM));

    private static Object object = new Object();

    public LocalInfoManager(OutputCollector collector, int taskId) {
        this.collector = collector;
        this.taskId = taskId;
        globalTopKSet = new HashSet<>();
        revisionFactorMap = new HashMap<>();
        this.K = TopKConfig.getInt(ConfigName.K);
    }

    /**
     * 当需要调用全局解决算法时，中心节点会向监控节点发送请求，以获取监控节点的local info
     *
     * @param tuple
     */
    public void emitLocalInfo(Tuple tuple) {
        Set<String> resolveSet = (Set<String>) tuple.getValueByField(MsgField.RESOLVE_SET);
        LOG.debug("resolve set is: [{}] when coordinator node send global request", resolveSet);
        List list = new ArrayList();
        Map<String, Long> countMap = MapUtil.findTargetMap(nodeCountMap, resolveSet);
        LOG.debug("resolve set's objects and it's count value mapping in monitoring node: [{}] is: [{}]", taskId, countMap);
        list.add(countMap);
        list.add(0l);
        LOG.debug("monitoring node: [{}] emitting local info", taskId);
        collector.emit(StreamIds.LOCAL_INFO, list);
    }

    /**
     * 计算论文中的Pj,注：nodeCountMap 只是本地监控节点上的obj-count map
     *
     * @param resolveSet 包含top-k及所有监控节点上的vio object
     * @return
     */
    public long computeBoundVal(Set<String> resolveSet) {
        long maxVal = 0;
        for (String obj : nodeCountMap.keySet()) {
            if (!resolveSet.contains(obj)) {
                long val = nodeCountMap.get(obj) + getFactor(obj);
                if (val > maxVal) {
                    maxVal = val;
                }
            }
        }
        for(String obj : resolveSet) {

        }
       /* for (String obj : resolveSet) {
            if (nodeCountMap.get(obj) != null) {
                Long val = nodeCountMap.get(obj) + getFactor(obj);
                if (val < maxVal) {
                    maxVal = val;
                }
            }
        }*/
        return maxVal;
    }

    /**
     * 向中心处理节点发送本监控节点违反约束的信息
     *
     * @param violateSet
     * @param countMap
     * @param boundVal
     */
    public void sendViolateInfo(Set<String> violateSet,
                                Map<String, Long> countMap,
                                long boundVal) {
        List list = new ArrayList<>();
        list.add(violateSet);
        list.add(countMap);
        list.add(boundVal);
        LOG.debug("monitoring node: [{}] emitting local violate message: [{}]", taskId, list.toString());
        collector.emit(StreamIds.LOCAL_VIOLATE, list);
    }

    /**
     * 更新monitoring node本地的top-k和revision factor map
     *
     * @param tuple
     */
    public void updateNewInfo(Tuple tuple) {
        globalTopKSet = (Set<String>) tuple.getValueByField(MsgField.GLOBAL_TOP_K);
        Map<String, Long> nodeFactorMap = (Map<String, Long>) tuple.getValueByField(MsgField.REVISION_MAP);
        boolean isInit = (boolean) tuple.getValueByField(MsgField.IS_INIT);
        LOG.info("monitoring node: [{}], update local info, new global top set: [{}]", taskId, globalTopKSet);
        updateFactorMap(nodeFactorMap, isInit);
    }

    public void updateNewGlobalTopKAndEmitLocalBound(Tuple tuple) {
        globalTopKSet = (Set<String>) tuple.getValueByField(MsgField.NEW_TOPK);
        LOG.info("monitoring node: [{}], global top-k: [{}]", taskId, globalTopKSet);
        List list = new ArrayList();
        long boundVal = computeBoundVal(globalTopKSet);
        LOG.info("monitoring node: [{}], bound value: [{}]",taskId, boundVal);
        list.add(boundVal);
        LOG.debug("monitoring node: [{}] emitting local bound", taskId);
        collector.emit(StreamIds.LOCAL_BOUND, list);

    }

    /**
     * update local "object-revision factor" map
     *
     * @param localFactorMap
     */
    public void updateFactorMap(Map<String, Long> localFactorMap, boolean isInit) {
        LOG.debug("per violation is: {}", CostUtil.getPerViolation());
        if (CostUtil.getPerViolation() > 0) {
            if ((CostUtil.decPerViolation() == 0) && (!isInit)) {
                LOG.debug("notify, begin process next slide");
                try{
                    Thread.sleep(1000);
                } catch(InterruptedException e) {

                }
                CostUtil.setProcessed(0);
                queue.clear();
                LOG.info("opening data source...");
                if (TopKConfig.getBoolean(ConfigName.IS_REAL_DATA_SET)) {
                    TrafficFlowSpout.start.set(true);
                } else {
                    SyntheticDataSpout.start.set(true);
                }

            }
        }

        if (localFactorMap != null) {
            for (String obj : localFactorMap.keySet()) {
                revisionFactorMap.put(obj, localFactorMap.get(obj));
            }
        }
        LOG.info("monitoring node: [{}], receiving new factor map: [{}], and new factor map " +
                "is: [{}] after updated", taskId, localFactorMap, revisionFactorMap);

    }

    /**
     * needs to recompute local info when monitoring window slides
     *
     * @param countMap "object-count" map in a monitoring node
     */
    public void reComputeInfo(Map<String, Long> countMap) {
        try {
            queue.put(countMap);
        } catch (InterruptedException e) {

        }
        CostUtil.incProcessed();
        LOG.debug("processed count: {}", CostUtil.getProcessed());
        long boundVal;
        nodeCountMap = new ConcurrentHashMap<>(countMap);
        Set<String> violateSet = new HashSet<>();
        Map<String, Long> localCountMap = new HashMap<>();
        LOG.info("monitoring node: [{}], revision factor map: [{}]", taskId, revisionFactorMap);
        Map<String, Long> fixedCountMap = MapUtil.mapAddMap(countMap, revisionFactorMap);
        LOG.info("monitoring node: [{}], fixed count map: [{}]", taskId, MapUtil.sortByValue(fixedCountMap));
        LOG.info("monitoring node: [{}], global top-k set: [{}]", taskId, globalTopKSet);
        Map<String, Long> localTopKMap = MapUtil.findTopK(fixedCountMap, K + 1, globalTopKSet);
        LOG.info("monitoring node: [{}], local top-k: [{}]", taskId, localTopKMap);
        List<String> localTopList = new ArrayList<>(localTopKMap.keySet());
        String boundObj = null;
        if (localTopList.size() == K + 1) {
            boundObj = localTopList.remove(localTopList.size() - 1);
        }
        LOG.debug("monitoring node: [{}], top-k list is: [{}] and global top-k list is: [{}]", taskId, localTopList, globalTopKSet);
        for (String obj : localTopList) {
            if (!globalTopKSet.contains(obj)) {
                violateSet.add(obj);
                localCountMap.put(obj, countMap.get(obj));
            }
        }
        LOG.info("monitoring node: [{}], violateSet = {}", taskId, violateSet);
        //本地约束违反
        if (violateSet.size() > 0) {
            for (String obj : globalTopKSet) {
                localCountMap.put(obj, countMap.get(obj));
            }
            LOG.debug("monitoring node: [{}] local count map is: [{}]", taskId, localCountMap);
            if (boundObj != null) {
                boundVal = countMap.get(boundObj);
            } else {
                boundVal = 0l;
            }
            LOG.debug("monitoring node: [{}] 's bound obj is: [{}] and it's value is: [{}]", taskId, boundObj, boundVal);
            CostUtil.incViolation();
            sendViolateInfo(violateSet, localCountMap, boundVal);
            CostUtil.incPerViolation();
        } else {
            LOG.debug("no violation in monitoring node: [{}]", taskId);
        }
    }

    /**
     * 获取某一对象的revision factor
     *
     * @param obj
     * @return
     */
    private Long getFactor(String obj) {
        if (revisionFactorMap.get(obj) == null) {
            return 0l;
        } else {
            return revisionFactorMap.get(obj);
        }
    }
}