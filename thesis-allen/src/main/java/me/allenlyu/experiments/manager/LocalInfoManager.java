package me.allenlyu.experiments.manager;

import me.allenlyu.experiments.common.ConfigName;
import me.allenlyu.experiments.common.MsgField;
import me.allenlyu.experiments.common.StreamIds;
import me.allenlyu.experiments.utils.MapUtils;
import me.allenlyu.experiments.utils.TopKConfig;
import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * LocalInfoManager
 *
 * @author Allen Jin
 * @date 2017/02/16
 */
public class LocalInfoManager {
    private static final Logger LOG = LoggerFactory.getLogger(LocalInfoManager.class);
    //全局TopK
    private Set<String> globalTopKSet;
    //调整参数
    private Map<String, Long> revisionFactorMap;
    private OutputCollector collector;
    private Map<String, Long> nodeCountMap;

    private int K;

    public LocalInfoManager(OutputCollector collector) {
        this.collector = collector;
        globalTopKSet = new HashSet<>();
        revisionFactorMap = new HashMap<>();
        this.K = TopKConfig.getInt(ConfigName.K);
    }

    public void emitLocalInfo(Tuple tuple) {
        Set<String> violateSet = (Set<String>) tuple.getValueByField(MsgField.RESOLVE_SET);
        List list = new ArrayList();
//        synchronized (nodeCountMap) {
//        if (nodeCountMap != null) {
        LOG.info("nodeCountMap = {}", nodeCountMap);
            Map<String, Long> countMap = MapUtils.findTargetMap(nodeCountMap, violateSet);
            list.add(countMap);
            long boundVal = computeBoundVal(violateSet);
            list.add(boundVal);
//        } else {
//            list.add(new HashMap<>());
//            list.add(0l);
//        }
//    }
        collector.emit(StreamIds.LOCAL_INFO, list);
    }

    public long computeBoundVal(Set<String> violateSet) {
        long maxVal = 0;
        for (String obj : nodeCountMap.keySet()) {
            if (!violateSet.contains(obj)) {
                long val = nodeCountMap.get(obj) + getFactor(obj);
                if (val > maxVal) {
                    maxVal = val;
                }
            }
        }
        for (String obj : violateSet) {
            if (nodeCountMap.get(obj) != null) {
                Long val = nodeCountMap.get(obj) + getFactor(obj);
                if (val < maxVal) {
                    maxVal = val;
                }
            }
        }
        return maxVal;
    }

    public void sendViolateInfo(Set<String> violateSet,
                                Map<String, Long> countMap,
                                long boundVal) {
        List list = new ArrayList<>();
        list.add(violateSet);
        list.add(countMap);
        list.add(boundVal);
        collector.emit(StreamIds.LOCAL_VIOLATE, list);
    }

    public void updateNewInfo(Tuple tuple) {
        globalTopKSet = (Set<String>) tuple.getValueByField(MsgField.GLOBAL_TOP_K);
        Map<String, Long> nodeFactorMap = (Map<String, Long>) tuple.getValueByField(MsgField.REVISION_MAP);
        updateFactorMap(nodeFactorMap);
    }

    public void updateFactorMap(Map<String, Long> localFactorMap) {
        if (localFactorMap != null) {
            for (String obj : localFactorMap.keySet()) {
                revisionFactorMap.put(obj, localFactorMap.get(obj));
            }
        }
    }

    public void reComputeInfo(Map<String, Long> countMap) {
//        synchronized (nodeCountMap) {
        nodeCountMap = new ConcurrentHashMap<>(countMap);
//        }
        Set<String> violateSet = new HashSet<>();
        Map<String, Long> localCountMap = new HashMap<>();
        Map<String, Long> fixedCountMap = MapUtils.combine(countMap, revisionFactorMap);
        Map<String, Long> localTopKMap = MapUtils.findTopK(fixedCountMap, K + 1);
//        LOG.info("$$LOCAL TOP K = {}", localTopKMap);
//        LOG.info("$$LOCAL FACTOR MAP = {}", revisionFactorMap);
        List<String> localTopList = new ArrayList<>(localTopKMap.keySet());
        String boundObj = localTopList.remove(localTopList.size() - 1);
        for (String obj : localTopList) {
            if (!globalTopKSet.contains(obj)) {
                violateSet.add(obj);
                localCountMap.put(obj, countMap.get(obj));
            }
        }
        LOG.info("violateSet = {}", violateSet);
        //本地约束违反
        if (violateSet.size() > 0) {
//            LOG.info("slide check : local constraints violated");
            for (String obj : globalTopKSet) {
                localCountMap.put(obj, countMap.get(obj));
            }
            long boundVal = countMap.get(boundObj);
            sendViolateInfo(violateSet, localCountMap, boundVal);
        } else {
//            LOG.info("slide check : local constraints valid");
        }
    }

    private Long getFactor(String obj) {
        if (revisionFactorMap.get(obj) == null) {
            return 0l;
        } else {
            return revisionFactorMap.get(obj);
        }
    }
}
