package me.allenlyu.experiments.manager;

import me.allenlyu.experiments.common.ConfigName;
import me.allenlyu.experiments.common.NodeViolationBundle;
import me.allenlyu.experiments.common.StreamIds;
import me.allenlyu.experiments.utils.CostUtils;
import me.allenlyu.experiments.utils.MapUtils;
import me.allenlyu.experiments.utils.TopKConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * ResolutionManager
 *
 * @author Allen Jin
 * @date 2017/02/17
 */
public class ResolutionManager {
    private static final Logger LOG = LoggerFactory.getLogger(ResolutionManager.class);
    private OutputCollector collector;
    private List<Integer> nodeIdList;
    private Set<String> topKSet;
    private Map<Integer, Map<String, Long>> nodeFactorMap;
    private Map<String, Long> globalSlackMap;
    private int errorFactor;
    private int K;
    private CountDownLatch resolveLatch;
    private Map<Integer, NodeViolationBundle> resolveMap;
    private boolean isSlack;
    private Set<String> lastTopK = null;

    public ResolutionManager(OutputCollector collector, List<Integer> nodeIdList, int K, int errorFactor) {
        this.collector = collector;
        this.nodeIdList = nodeIdList;
        this.isSlack = TopKConfig.getBoolean(ConfigName.WITH_SLACK);
        this.K = K;
        this.errorFactor = errorFactor;
        topKSet = new HashSet<>();
        globalSlackMap = new HashMap<>();
        nodeFactorMap = new HashMap<>();
    }

    public synchronized void handleViolation(Map<Integer, NodeViolationBundle> nodeInfoMap) {
        CostUtils.incPartial();
        if (!partialResolve(nodeInfoMap)) {
            globalResolve(nodeInfoMap);
        } else {
            LOG.info("partial succeed : {}", System.currentTimeMillis());
            CostUtils.incValid();
        }
    }

    //出现多个违反约束,根据已有信息解决违反
    private boolean partialResolve(Map<Integer, NodeViolationBundle> nodeInfoMap) {
        if (!isSlack) {
            return false;
        }
        Map<String, Long> slackLeftMap = new HashMap<>();
        for (String topItem : topKSet) {
            slackLeftMap.put(topItem, valFormat(globalSlackMap, topItem));
        }
        Map<Integer, Map<String, Long>> nodeUsedMap = new HashMap<>();
        for (Integer nodeId : nodeInfoMap.keySet()) {
            nodeUsedMap.put(nodeId, new HashMap<String, Long>());
            NodeViolationBundle bundle = nodeInfoMap.get(nodeId);
            Map<String, Long> localCountMap = bundle.getNodeCountMap();
            Set<String> violateSet = bundle.getViolateSet();
            Map<String, Long> localFactorMap = nodeFactorMap.get(nodeId);
            long maxViolateVal = findMaxViolateVal(localCountMap, violateSet, localFactorMap);
//            LOG.info("nodeId = {}, localCountMap = {}", nodeId, localCountMap);
//            LOG.info("localFactor = {}", localFactorMap);
//            LOG.info("maxViolateVal = {}", maxViolateVal);
            for (String topItem : topKSet) {
                long slackLeft = slackLeftMap.get(topItem);
                long usedSlack = maxViolateVal - (valFormat(localCountMap, topItem) + valFormat(localFactorMap, topItem));
                nodeUsedMap.get(nodeId).put(topItem, usedSlack);
                slackLeft -= usedSlack;
                slackLeftMap.put(topItem, slackLeft);
            }
        }
        long maxSlackNot = findMaxSlackNotSet(topKSet);
//        LOG.info("globalSlackMap = {}", globalSlackMap);
//        LOG.info("slackLeftMap = {}", slackLeftMap);
//        LOG.info("maxSlackNot = {}", maxSlackNot);
        for (String obj : slackLeftMap.keySet()) {
            if (slackLeftMap.get(obj) + errorFactor < maxSlackNot) {
                return false;
            }
        }
//        LOG.info("partial resolve succeed!");
//        LOG.info("nodeFactorMap = {}", nodeFactorMap);
//        LOG.info("slackLeftMap = {}", slackLeftMap);
        Map<Integer, Map<String, Long>> partialNewFactor = partialNewFactor(slackLeftMap, nodeUsedMap, maxSlackNot);
//        LOG.info("##GLOBAL partialNewFactor: {}", partialNewFactor);
        assignInfoToNode(topKSet, partialNewFactor);
        return true;
    }

    private long findMaxSlackNotSet(Set<String> set) {
        Long maxVal = null;
        for (String obj : globalSlackMap.keySet()) {
            if (!set.contains(obj)) {
                if (maxVal == null || maxVal < globalSlackMap.get(obj)) {
                    maxVal = globalSlackMap.get(obj);
                }
            }
        }
        if (maxVal == null) maxVal = 0l;
        return maxVal;
    }

    private Map<Integer, Map<String, Long>> partialNewFactor(Map<String, Long> slackLeftMap,
                                                             Map<Integer, Map<String, Long>> nodeUsedMap,
                                                             long maxVal) {
        Map<Integer, Map<String, Long>> result = new HashMap<>();
        Set<Integer> nodeSet = nodeUsedMap.keySet();
        for (Integer nodeId : nodeSet) {
            result.put(nodeId, new HashMap<String, Long>());
        }

//       ====local0

//        for (String obj : slackLeftMap.keySet()) {
//            for (Integer nodeId : nodeSet) {
//                long used = nodeUsedMap.get(nodeId).get(obj);
//                result.get(nodeId).put(obj, used);
//                nodeFactorMap.get(nodeId).put(obj, used);
//            }
//            globalSlackMap.put(obj, slackLeftMap.get(obj));
//        }
//        ========

        int divError = (errorFactor / (nodeIdList.size() + 1)) * (nodeSet.size() + 1);
        for (String obj : slackLeftMap.keySet()) {
            long left = slackLeftMap.get(obj) - maxVal + divError;
//            LOG.info("PARTIAL obj = [{}], left=[{}]", obj, left);
            long over = left % (nodeSet.size() + 1);
            left -= over;
            long even = left / (nodeSet.size() + 1);
            for (Integer nodeId : nodeSet) {
                long used = nodeUsedMap.get(nodeId).get(obj);
                long local = even + used;
                if (over != 0) {
                    if (over < 0) {
                        local--;
                        over++;
                    } else {
                        local++;
                        over--;
                    }
                }
                result.get(nodeId).put(obj, local);
                nodeFactorMap.get(nodeId).put(obj, local);
                left -= even;
            }
            globalSlackMap.put(obj, left + maxVal - divError);
//            LOG.info("obj={}, val={}", obj, globalSlackMap.get(obj));
        }
//        LOG.info("PARTIAL RESULT = {}", result);
        return result;
    }

    private long findMaxViolateVal(Map<String, Long> localCountMap,
                                   Set<String> violateSet,
                                   Map<String, Long> localFactorMap) {
        if (violateSet == null) return 0;
        long max = Long.MIN_VALUE;
        for (String obj : violateSet) {
            long val = valFormat(localCountMap, obj) + valFormat(localFactorMap, obj);
            if (val > max) {
                max = val;
            }
        }
        return max;
    }


    private long valFormat(Map<String, Long> map, String obj) {
        return map.get(obj) == null ? 0 : map.get(obj);
    }

    private void globalResolve(Map<Integer, NodeViolationBundle> nodeInfoMap) {
        final Set<String> allViolateSet = new HashSet<>();
        allViolateSet.addAll(topKSet);
        for (NodeViolationBundle bundle : nodeInfoMap.values()) {
            allViolateSet.addAll(bundle.getViolateSet());
        }
        resolveLatch = new CountDownLatch(nodeIdList.size());
        resolveMap = new ConcurrentHashMap<>();
        new Thread(new Runnable() {
            @Override
            public void run() {
//                LOG.info("===START GLOBAL RESOLUTION");
                try {
                    resolveLatch.await();
//                    LOG.info("resolve map = {}", resolveMap);
                    computeNewTopK(allViolateSet);
//                    LOG.info("===END GLOBAL RESOLUTION");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        for (Integer nodeId : nodeIdList) {
            List list = new ArrayList();
            list.add(allViolateSet);
            collector.emitDirect(nodeId, StreamIds.GLOBAL_REQUEST, list);
        }
    }

    public void computeNewTopK(Set<String> allViolateSet) {
        Map<String, Long> unitCountMap = new HashMap<>();
        Map<Integer, Long> boundValMap = new HashMap<>();
        Map<Integer, Map<String, Long>> nodeCountMap = new HashMap<>();
        for (Integer nodeId : resolveMap.keySet()) {
            NodeViolationBundle bundle = resolveMap.get(nodeId);
            nodeCountMap.put(nodeId, bundle.getNodeCountMap());
            unitCountMap = MapUtils.combine(unitCountMap, bundle.getNodeCountMap());
            boundValMap.put(nodeId, bundle.getBoundVal());
        }
        Map<String, Long> newTopKMap = MapUtils.findTopK(unitCountMap, K);
//        LOG.info("new topK = {}", newTopKMap);
        topKSet = newTopKMap.keySet();
        Map<Integer, Map<String, Long>> newFactorMap = null;
        if (isSlack) {
            long globalBoundVal = findMaxSlackNotSet(allViolateSet);
            Map<String, Long> slackMap = computeSlackMap(allViolateSet, nodeCountMap, boundValMap, globalBoundVal);
            newFactorMap = computeFactor(nodeCountMap, slackMap, boundValMap, globalBoundVal);
//        LOG.info("##GLOBAL newFactorMap: {}", newFactorMap);
//        LOG.info("new factor map = {}", newFactorMap);
//        updateFactorMap(newFactorMap);
//        LOG.info("global slack map = {}", globalSlackMap);

        } else {    //without slack
            newFactorMap = computeNoSlackFactor(allViolateSet, nodeCountMap, boundValMap);
            lastTopK = new HashSet<>(topKSet);
//            LOG.info("newFactorMap = {}", newFactorMap);
        }
        assignInfoToNode(topKSet, newFactorMap);
        LOG.info("global finished : {}", System.currentTimeMillis());
    }

    public Map<Integer, Map<String, Long>> computeNoSlackFactor(Set<String> resolveSet,
                                                                Map<Integer, Map<String, Long>> nodeCountMap,
                                                                Map<Integer, Long> boundMap) {
        Map<String, Long> slackMap = computeSlackMap(resolveSet, nodeCountMap, boundMap, 0);
        Map<Integer, Map<String, Long>> newFactorMap = new HashMap<>();
        Set<Integer> nodeSet = nodeCountMap.keySet();
        for (Integer nodeId : nodeSet) {
            newFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        for (String obj : slackMap.keySet()) {
            long leftSlack = slackMap.get(obj);
            if (lastTopK != null) {
                if (lastTopK.contains(obj)) {
                    leftSlack -= errorFactor;
                }
            }
            if (topKSet.contains(obj)) {
                leftSlack += errorFactor;
            }

            long overSlack = leftSlack % (nodeSet.size());  //防止除后出现余数
            leftSlack -= overSlack;
            long evenSlack = leftSlack / (nodeSet.size());
            for (Integer nodeId : nodeSet) {
                long localObjCount = valFormat(nodeCountMap.get(nodeId), obj);
                long localSlack = boundMap.get(nodeId) - localObjCount + evenSlack;
                //按节点顺序分配多余的数,保证一致增加
                if (overSlack != 0) {
                    if (overSlack < 0) {
                        localSlack--;
                        overSlack++;
                    } else {
                        localSlack++;
                        overSlack--;
                    }
                }
                newFactorMap.get(nodeId).put(obj, localSlack);
                //update global
                nodeFactorMap.get(nodeId).put(obj, localSlack);
            }
        }
        return newFactorMap;
    }

//    public void updateFactorMap(Map<Integer, Map<String, Long>> newFactorMap) {
//        for (Integer nodeId : newFactorMap.keySet()) {
//            Map<String, Long> localFactorMap = newFactorMap.get(nodeId);
//            for (String obj : localFactorMap.keySet()) {
//                nodeFactorMap.get(nodeId).put(obj, localFactorMap.get(obj));
//            }
//        }
//    }

    public void handleLocalInfo(Tuple tuple) {
//        LOG.info("receive nodeId : {}", tuple.getSourceTask());
        NodeViolationBundle bundle = new NodeViolationBundle().builder(tuple);
        resolveMap.put(tuple.getSourceTask(), bundle);
        resolveLatch.countDown();
    }

    public void initTopKSet(Set<String> topKSet) {
        this.topKSet = topKSet;
    }

    public void initNodeFactorMap(Map<Integer, Map<String, Long>> nodeFactorMap) {
        this.nodeFactorMap = nodeFactorMap;
    }

    public void assignInfoToNode(Set<String> topKSet,
                                 Map<Integer, Map<String, Long>> newFactorMap) {
        for (Integer nodeId : newFactorMap.keySet()) {
            List list = new ArrayList();
            list.add(topKSet);
            list.add(newFactorMap.get(nodeId));
            collector.emitDirect(nodeId, StreamIds.NEW_CONSTRAINTS, list);
        }
    }

    public Map<Integer, Map<String, Long>> computeFactor(Map<Integer, Map<String, Long>> nodeCountMap,
                                                         Map<String, Long> slackMap,
                                                         Map<Integer, Long> boundMap,
                                                         long globalBoundVal) {
        Map<Integer, Map<String, Long>> newFactorMap = new HashMap<>();
        Set<Integer> nodeSet = nodeCountMap.keySet();
        for (Integer nodeId : nodeSet) {
            newFactorMap.put(nodeId, new HashMap<String, Long>());
        }

//        Map<String, Long> sm = MapUtils.findTopK(slackMap, K + 1);
//        List<String> topList = new ArrayList<>(sm.keySet());
//        long boundVal = sm.get(topList.get(topList.size() - 1));

        for (String obj : slackMap.keySet()) {
            long leftSlack = slackMap.get(obj);

//            if (topKSet.contains(obj)) {
//                globalSlackMap.put(obj, leftSlack - boundVal);
//                leftSlack = boundVal;
//            }

//            if (topKSet.contains(obj)) {
//                leftSlack += errorFactor;
//            }
//            for (Integer nodeId : nodeSet) {
//                long localObjCount = valFormat(nodeCountMap.get(nodeId), obj);
//                long used = boundMap.get(nodeId) - localObjCount;
//                newFactorMap.get(nodeId).put(obj, used);
//                nodeFactorMap.get(nodeId).put(obj, used);
//                leftSlack = leftSlack - used;
//            }
            long overSlack = leftSlack % (nodeSet.size() + 1);  //防止除后出现余数
            leftSlack -= overSlack;
            long evenSlack = leftSlack / (nodeSet.size() + 1);
            for (Integer nodeId : nodeSet) {
                long localObjCount = valFormat(nodeCountMap.get(nodeId), obj);
                long localSlack = boundMap.get(nodeId) - localObjCount + evenSlack;
                //按节点顺序分配多余的数,保证一致增加
                if (overSlack != 0) {
                    if (overSlack < 0) {
                        localSlack--;
                        overSlack++;
                    } else {
                        localSlack++;
                        overSlack--;
                    }
                }
                newFactorMap.get(nodeId).put(obj, localSlack);
                //update global
                nodeFactorMap.get(nodeId).put(obj, localSlack);
                leftSlack -= localSlack;
            }
            if (topKSet.contains(obj)) {
                leftSlack += globalBoundVal;
//                leftSlack -= errorFactor;
            }
            globalSlackMap.put(obj, leftSlack);
        }
        return newFactorMap;
    }

    public Map<String, Long> computeSlackMap(Set<String> resolveSet,
                                             Map<Integer, Map<String, Long>> nodeCountMap,
                                             Map<Integer, Long> boundMap,
                                             long globalBoundVal) {
        Map<String, Long> slackMap = new HashMap<>();
        for (String obj : resolveSet) {
            long slack = 0;
            for (Integer nodeId : nodeCountMap.keySet()) {
                Map<String, Long> localCountMap = nodeCountMap.get(nodeId);
                if (localCountMap.get(obj) != null) {
                    slack += localCountMap.get(obj);
                }
                slack -= boundMap.get(nodeId);
            }
            if (topKSet.contains(obj)) {
                slack -= globalBoundVal;
            }
            slackMap.put(obj, slack);
        }
        return slackMap;
    }
}
