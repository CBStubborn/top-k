package thesis.topk.monitoring.manager;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.common.*;
import thesis.topk.monitoring.utils.CostUtil;
import thesis.topk.monitoring.utils.MapUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class ResolutionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ResolutionManager.class);

    private OutputCollector collector;

    private List<Integer> nodeIdList;

    private Set<String> topKSet;

    private Map<Integer, Map<String, Long>> nodeFactorMap;

    //用于存储对象在中心处理节点上的松弛值
    private Map<String, Long> globalSlackMap;

    private int errorFactor;

    private int K;

    private CountDownLatch localInfoLatch;

    private CountDownLatch localBoundLatch;

    private Map<Integer, NodeViolationBundle> resolveMap;

    private String withSlack;

    private boolean isPlain;

    public ResolutionManager(OutputCollector collector, List<Integer> nodeIdList, int K, int errorFactor) {
        this.collector = collector;
        this.nodeIdList = nodeIdList;
        this.withSlack = TopKConfig.get(ConfigName.WITH_SLACK);
        this.isPlain = TopKConfig.getBoolean(ConfigName.IS_PLAIN);
        this.K = K;
        this.errorFactor = errorFactor;
        topKSet = new HashSet<>();
        globalSlackMap = new HashMap<>();
        nodeFactorMap = new HashMap<>();
    }

    /**
     * 初始化top-k set
     *
     * @param topKSet
     */
    public void initTopKSet(Set<String> topKSet) {
        this.topKSet = topKSet;
    }

    /**
     * @param nodeFactorMap key:nodeId, value:object-factor mapping
     */
    public void initNodeFactorMap(Map<Integer, Map<String, Long>> nodeFactorMap) {
        this.nodeFactorMap = nodeFactorMap;
    }

    /**
     * 将更新后的 top-k set 和 factor map 发送给指定的monitoring node
     *
     * @param topKSet
     * @param newFactorMap
     */
    public void assignInfoToNode(Set<String> topKSet,
                                 Map<Integer, Map<String, Long>> newFactorMap, boolean isInit) {
        LOG.debug("assign new constraints to monitoring node, top-k set is: [{}], new factor " +
                "map is: [{}], and global slack map is: [{}]", topKSet, newFactorMap, globalSlackMap);
        for (Integer nodeId : newFactorMap.keySet()) {
            List list = new ArrayList();
            list.add(topKSet);
            list.add(newFactorMap.get(nodeId));
            list.add(isInit);
            collector.emitDirect(nodeId, StreamIds.NEW_CONSTRAINTS, list);
        }
    }

    /**
     * 处理监控节点发来的本地信息
     *
     * @param tuple
     */
    public void handleLocalInfo(Tuple tuple) {
        NodeViolationBundle bundle = new NodeViolationBundle().builder(tuple);
        resolveMap.put(tuple.getSourceTask(), bundle);
        localInfoLatch.countDown();
    }

    public void handleLocalBound(Tuple tuple) {
        NodeViolationBundle bundle = resolveMap.get(tuple.getSourceTask());
        bundle.setBoundVal(tuple.getLongByField(MsgField.LOCAL_BOUND_VAL));
        localBoundLatch.countDown();
    }

    /**
     * 处理违反约束的情况
     *
     * @param nodeInfoMap
     */
    public synchronized void handleViolation(Map<Integer, NodeViolationBundle> nodeInfoMap) {
        LOG.debug("handle violation: [{}]", nodeInfoMap);
        CostUtil.incPartial();
        if (!partialResolve(nodeInfoMap)) {
            globalResolve(nodeInfoMap);
        } else {
            LOG.info("partial succeed : {}", System.currentTimeMillis());
            CostUtil.incValid();
        }
    }

    /**
     * 论文算法一的实现
     *
     * @param nodeInfoMap
     * @return
     */
    //出现多个违反约束,根据已有信息解决违反
    private boolean partialResolve(Map<Integer, NodeViolationBundle> nodeInfoMap) {
        LOG.info("do partial resolve...");
        if ("without".equals(withSlack)) {
            LOG.debug("the parameter of 'with slack' is 'without', partial resolve failed, will enter in global resolve");
            return false;
        }
        Map<String, Long> slackLeftMap = new HashMap<>();
        for (String topItem : topKSet) {
            slackLeftMap.put(topItem, valFormat(globalSlackMap, topItem));
        }
        Map<Integer, Map<String, Long>> nodeUsedMap = new HashMap<>();
        Set<String> allViolateSet = new HashSet<>();
        for (Integer nodeId : nodeInfoMap.keySet()) {
            nodeUsedMap.put(nodeId, new HashMap<String, Long>());
            NodeViolationBundle bundle = nodeInfoMap.get(nodeId);
            Map<String, Long> localCountMap = bundle.getObjectCountMap();
            Set<String> violateSet = bundle.getViolateSet();
            allViolateSet.addAll(violateSet);
            Map<String, Long> localFactorMap = nodeFactorMap.get(nodeId);
            long maxViolateVal = findMaxViolateVal(localCountMap, violateSet, localFactorMap);
            for (String topItem : topKSet) {
                long slackLeft = slackLeftMap.get(topItem);
                long usedSlack = maxViolateVal - (valFormat(localCountMap, topItem) + valFormat(localFactorMap, topItem));
                slackLeft -= usedSlack;
                nodeUsedMap.get(nodeId).put(topItem, usedSlack);
                slackLeftMap.put(topItem, slackLeft);
            }
        }

        //long maxSlackNot = findMaxSlackNotSet(topKSet);
        long maxSlackIn = findMaxSlackInSet(allViolateSet);
        LOG.info("max slack in is: [{}]", maxSlackIn);
        for (String obj : slackLeftMap.keySet()) {
            if (slackLeftMap.get(obj) + errorFactor < maxSlackIn) {
                return false;
            }
        }

        Map<Integer, Map<String, Long>> partialNewFactor;
        if (isPlain) {
            partialNewFactor = plainPartialNewFactor(slackLeftMap, nodeUsedMap);
        } else {
            partialNewFactor = effectivePartialNewFactor(slackLeftMap, nodeUsedMap, maxSlackIn);
        }
        assignInfoToNode(topKSet, partialNewFactor, false);
        LOG.debug("succeed in partial resolving, and node factor map is: [{}]", nodeFactorMap);
        return true;
    }

    private Map<Integer, Map<String, Long>> plainPartialNewFactor(Map<String, Long> slackLeftMap,
                                                                  Map<Integer, Map<String, Long>> nodeUsedMap) {
        Map<Integer, Map<String, Long>> result = new HashMap<>();
        Set<Integer> nodeSet = nodeUsedMap.keySet();
        for (Integer nodeId : nodeSet) {
            result.put(nodeId, new HashMap<String, Long>());
        }
        for (String obj : slackLeftMap.keySet()) {
            for (Integer nodeId : nodeSet) {
                long used = nodeUsedMap.get(nodeId).get(obj);
                long before;
                if (nodeFactorMap.get(nodeId).get(obj) != null) {
                    before = nodeFactorMap.get(nodeId).get(obj);
                } else {
                    before = 0l;
                }
                result.get(nodeId).put(obj, used + before);
                nodeFactorMap.get(nodeId).put(obj, used + before);
            }
            globalSlackMap.put(obj, slackLeftMap.get(obj));
        }
        return result;
    }

    /**
     * 如果算法一成功解决冲突，则重新计算对象的松驰值
     *
     * @param slackLeftMap
     * @param nodeUsedMap
     * @param maxVal
     * @return
     */
    private Map<Integer, Map<String, Long>> effectivePartialNewFactor(Map<String, Long> slackLeftMap,
                                                                      Map<Integer, Map<String, Long>> nodeUsedMap,
                                                                      long maxVal) {
        Map<Integer, Map<String, Long>> result = new HashMap<>();
        Set<Integer> nodeSet = nodeUsedMap.keySet();
        for (Integer nodeId : nodeSet) {
            result.put(nodeId, new HashMap<String, Long>());
        }
        int divError = (errorFactor / (nodeIdList.size() + 1)) * (nodeSet.size() + 1);
        for (String obj : slackLeftMap.keySet()) {
            long left = slackLeftMap.get(obj) - maxVal + divError;
            long over = left % (nodeSet.size() + 1);
            long remain = left - over;
            long even = remain / (nodeSet.size() + 1);
            for (Integer nodeId : nodeSet) {
                long before;
                if (nodeFactorMap.get(nodeId).get(obj) != null) {
                    before = nodeFactorMap.get(nodeId).get(obj);
                } else {
                    before = 0l;
                }
                long used = nodeUsedMap.get(nodeId).get(obj);
                long local = even;
                if (over != 0) {
                    if (over < 0) {
                        local--;
                        over++;
                    } else {
                        local++;
                        over--;
                    }
                }
                left -= local;
                local = local + used + before;
                result.get(nodeId).put(obj, local);
                nodeFactorMap.get(nodeId).put(obj, local);
            }
            globalSlackMap.put(obj, left + maxVal - divError);
        }
        return result;
    }

    /**
     * 此方法即为实现论文中算法一的第二步：获取bound值
     *
     * @param localCountMap  包含top-k set 及违反约束的对象集， 即论文中的R
     * @param violateSet     违反约束的对象集
     * @param localFactorMap "object-revision factor" map
     * @return 返回本监控节点上的bound值
     */
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

    /**
     * 论文算法二的实现
     *
     * @param nodeInfoMap
     */
    private void globalResolve(Map<Integer, NodeViolationBundle> nodeInfoMap) {
        LOG.info("do global resolve...");
        final Set<String> allViolateSet = new HashSet<>();
        allViolateSet.addAll(topKSet);
        for (NodeViolationBundle bundle : nodeInfoMap.values()) {
            allViolateSet.addAll(bundle.getViolateSet());
        }
        localInfoLatch = new CountDownLatch(nodeIdList.size());
        resolveMap = new ConcurrentHashMap<>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    localInfoLatch.await();
                    computeNewTopK(allViolateSet);
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

    /**
     * 算法二中,重新计算top-k
     *
     * @param allViolateSet
     */
    private void computeNewTopK(Set<String> allViolateSet) {
        Map<String, Long> unitCountMap = new HashMap<>();
        for (Integer nodeId : resolveMap.keySet()) {
            NodeViolationBundle bundle = resolveMap.get(nodeId);
            unitCountMap = MapUtil.combine(unitCountMap, bundle.getObjectCountMap());
        }
        LOG.info("unit count map: [{}]", unitCountMap);
        LOG.info("last top-k set: [{}]", topKSet);
        Map<String, Long> newTopKMap = MapUtil.findTopK(unitCountMap, K, null);
        topKSet = newTopKMap.keySet();
        LOG.info("new top-k set: [{}]", topKSet);
        for (Integer nodeId : nodeIdList) {
            List list = new ArrayList();
            list.add(topKSet);
            collector.emitDirect(nodeId, StreamIds.NEW_GLOBAL_TOPK, list);
        }
        computeRevisionFactors(allViolateSet);
    }

    private void computeRevisionFactors(Set<String> violateSet) {
        LOG.info("compute revision factors...");
        final Set<String> allViolateSet = violateSet;
        localBoundLatch = new CountDownLatch(nodeIdList.size());
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    localBoundLatch.await();
                    doCompute(allViolateSet);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void doCompute(Set<String> allViolateSet) {
        Map<Integer, Long> boundValMap = new HashMap<>();
        Map<Integer, Map<String, Long>> nodeCountMap = new HashMap<>();
        for (Integer nodeId : resolveMap.keySet()) {
            NodeViolationBundle bundle = resolveMap.get(nodeId);
            nodeCountMap.put(nodeId, bundle.getObjectCountMap());
            boundValMap.put(nodeId, bundle.getBoundVal());
        }
        LOG.info("bound value map: [{}]", boundValMap);
        Map<Integer, Map<String, Long>> newFactorMap = null;
        if ("all".equals(withSlack)) {
            long globalBoundVal = findMaxSlackNotSet(allViolateSet);    //即论文中的P0
            Map<String, Long> slackMap = computeSlackMap(allViolateSet, nodeCountMap, boundValMap, globalBoundVal, null);
            newFactorMap = computeAllFactor(nodeCountMap, slackMap, boundValMap, globalBoundVal);
        } else if ("part".endsWith(withSlack)) {
            long globalBoundVal = findMaxSlackNotSet(allViolateSet);
            Map<String, Map<Integer, Long>> objectNodeGapMap = new HashMap<String, Map<Integer, Long>>();
            Map<String, Long> slackMap = computeSlackMap(allViolateSet, nodeCountMap, boundValMap, globalBoundVal, objectNodeGapMap);
            LOG.info("slack map is: [{}]", slackMap);
            newFactorMap = computePartFactor(nodeCountMap, slackMap, boundValMap, globalBoundVal, objectNodeGapMap);
        } else if ("without".equals(withSlack)) {
            long globalBoundVal = findMaxSlackNotSet(allViolateSet);
            Map<String, Long> slackMap = computeSlackMap(allViolateSet, nodeCountMap, boundValMap, 0l, null);
            newFactorMap = computeNoSlackFactor(nodeCountMap, slackMap, boundValMap, globalBoundVal);
        } else if ("part-effective".equals(withSlack)) {
            long globalBoundVal = findMaxSlackNotSet(allViolateSet);
            Map<String, Map<Integer, Long>> objectNodeGapMap = new HashMap<String, Map<Integer, Long>>();
            Map<String, Long> slackMap = computeSlackMap(allViolateSet, nodeCountMap, boundValMap, globalBoundVal, objectNodeGapMap);
            newFactorMap = computeEffectivePartFactor(nodeCountMap, slackMap, boundValMap, globalBoundVal, objectNodeGapMap);
        } else {  //other
            LOG.error("the parameter of 'with slack' is invalid, please check it!!!");
            System.exit(1);
        }
        assignInfoToNode(topKSet, newFactorMap, false);
        LOG.debug("global finished : {}", System.currentTimeMillis());
    }

    /**
     * @param resolveSet
     * @param nodeCountMap
     * @param boundMap
     * @param globalBoundVal
     * @return
     */
    private Map<String, Long> computeSlackMap(Set<String> resolveSet,
                                              Map<Integer, Map<String, Long>> nodeCountMap,
                                              Map<Integer, Long> boundMap,
                                              long globalBoundVal,
                                              Map<String, Map<Integer, Long>> objectNodeGapMap) {
        Map<String, Map<Integer, Long>> tmpNodeSlackMap = new HashMap<>();
        Map<String, Long> slackMap = new HashMap<>();
        for (String obj : resolveSet) {
            long slack = 0;
            Map<Integer, Long> nodeGapMap = new HashMap<>();
            for (Integer nodeId : nodeCountMap.keySet()) {
                Map<String, Long> localCountMap = nodeCountMap.get(nodeId);
                long gap = 0;
                if (localCountMap.get(obj) != null) {
                    gap = localCountMap.get(obj) - boundMap.get(nodeId);
                } else {
                    gap -= boundMap.get(nodeId);
                }
                slack += gap;
                nodeGapMap.put(nodeId, gap);
            }
            tmpNodeSlackMap.put(obj, nodeGapMap);
            slack -= globalBoundVal;
            /*if (topKSet.contains(obj)) {
                slack -= errorFactor;
            }*/
            slackMap.put(obj, slack);
        }
        if (objectNodeGapMap != null) {
            objectNodeGapMap.putAll(tmpNodeSlackMap);
        }
        return slackMap;
    }

    /**
     * 针对中心处理节点维护全部松驰值的情况
     *
     * @param nodeCountMap
     * @param slackMap
     * @param boundMap
     * @param globalBoundVal
     * @return
     */
    private Map<Integer, Map<String, Long>> computeAllFactor(Map<Integer, Map<String, Long>> nodeCountMap,
                                                             Map<String, Long> slackMap,
                                                             Map<Integer, Long> boundMap,
                                                             long globalBoundVal) {
        Map<Integer, Map<String, Long>> newFactorMap = new HashMap<>();
        Set<Integer> nodeSet = nodeCountMap.keySet();
        for (Integer nodeId : nodeSet) {
            newFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        for (String obj : slackMap.keySet()) {
            long leftSlack = slackMap.get(obj);
            globalSlackMap.put(obj, leftSlack + globalBoundVal);
            /*if (topKSet.contains(obj)) {
                globalSlackMap.put(obj, leftSlack + globalBoundVal + errorFactor);
            } else {
                globalSlackMap.put(obj, leftSlack + globalBoundVal);
            }*/
            for (Integer nodeId : nodeSet) {
                long localObjCount = valFormat(nodeCountMap.get(nodeId), obj);
                long used = boundMap.get(nodeId) - localObjCount;
                newFactorMap.get(nodeId).put(obj, used);
                nodeFactorMap.get(nodeId).put(obj, used);
            }
        }
        return newFactorMap;
    }

    /**
     * 针对中心处理节点维护部分松驰值的情况
     *
     * @param nodeCountMap
     * @param slackMap
     * @param boundMap
     * @param globalBoundVal
     * @return
     */
    private Map<Integer, Map<String, Long>> computePartFactor(Map<Integer, Map<String, Long>> nodeCountMap,
                                                              Map<String, Long> slackMap,
                                                              Map<Integer, Long> boundMap,
                                                              long globalBoundVal,
                                                              Map<String, Map<Integer, Long>> objectNodeGapMap) {
        Map<Integer, Map<String, Long>> newFactorMap = new HashMap<>();
        Set<Integer> nodeSet = nodeCountMap.keySet();
        for (Integer nodeId : nodeSet) {
            newFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        for (String obj : slackMap.keySet()) {
            long leftSlack = slackMap.get(obj);
            long overSlack = leftSlack % (nodeSet.size() + 1);
            long remain = leftSlack - overSlack;
            long evenSlack = remain / (nodeSet.size() + 1);
            for (Integer nodeId : nodeSet) {
                long localObjCount = valFormat(nodeCountMap.get(nodeId), obj);
                long localSlack = evenSlack;
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
                leftSlack -= localSlack;
                localSlack = localSlack + boundMap.get(nodeId) - localObjCount;
                newFactorMap.get(nodeId).put(obj, localSlack);
                nodeFactorMap.get(nodeId).put(obj, localSlack);
            }
            leftSlack += globalBoundVal;
           /* if (topKSet.contains(obj)) {
                leftSlack += errorFactor;
            }*/
            //剩余的相当于存放在中心处理节点上
            globalSlackMap.put(obj, leftSlack);
        }
        LOG.info("object node gap map: [{}]", objectNodeGapMap);
        LOG.info("object slack map: [{}]", slackMap);
        LOG.info("object new factor map: [{}]", newFactorMap);
        LOG.info("global slack map: [{}]", globalSlackMap);

        return newFactorMap;
    }

    /**
     * 针对中心处理节点不维护松驰值的情况
     *
     * @param nodeCountMap
     * @param slackMap
     * @param boundMap
     * @return
     */
    private Map<Integer, Map<String, Long>> computeNoSlackFactor(Map<Integer, Map<String, Long>> nodeCountMap,
                                                                 Map<String, Long> slackMap,
                                                                 Map<Integer, Long> boundMap,
                                                                 long globalBoundVal) {
        Map<Integer, Map<String, Long>> newFactorMap = new HashMap<>();
        Set<Integer> nodeSet = nodeCountMap.keySet();
        for (Integer nodeId : nodeSet) {
            newFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        for (String obj : slackMap.keySet()) {
            long leftSlack = slackMap.get(obj);
            /*if (topKSet.contains(obj)) {
                leftSlack += errorFactor;
            }*/
            leftSlack += globalBoundVal;
            long overSlack = leftSlack % (nodeSet.size());
            long remain = leftSlack - overSlack;
            long evenSlack = remain / (nodeSet.size());
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
                nodeFactorMap.get(nodeId).put(obj, localSlack);
            }
        }
        return newFactorMap;
    }

    /**
     * 对某一对象来说，在其大于边界值的节点中应分配较少的松弛值，
     * 相反在小于边界值的节点中应分配更多的松弛值
     *
     * @param nodeCountMap
     * @param slackMap
     * @param boundMap
     * @param globalBoundVal
     * @return
     */
    private Map<Integer, Map<String, Long>> computeEffectivePartFactor(Map<Integer, Map<String, Long>> nodeCountMap,
                                                                       Map<String, Long> slackMap,
                                                                       Map<Integer, Long> boundMap,
                                                                       long globalBoundVal,
                                                                       Map<String, Map<Integer, Long>> objectNodeGapMap) {
        Map<String, Map<Integer, Long>> reComputeSlackMap = new HashMap<>();
        Set<Integer> nodeSet = nodeCountMap.keySet();
        for (String obj : slackMap.keySet()) {
            Map<Integer, Long> reSlackMap = new HashMap<>();
            Map<Integer, Long> nodeGapMap = objectNodeGapMap.get(obj);
            long leftSlack = slackMap.get(obj);
            long even = leftSlack / (nodeSet.size() + 1);
            int underZeroCount = 0;
            for (Map.Entry<Integer, Long> entry : nodeGapMap.entrySet()) {
                long gap = entry.getValue();
                if (gap > 0) {
                    long local = even / gap;
                    reSlackMap.put(entry.getKey(), local);
                    leftSlack -= local;
                } else if (gap == 0) {
                    reSlackMap.put(entry.getKey(), even);
                    leftSlack -= even;
                } else {
                    underZeroCount -= gap;
                }
            }
            if (underZeroCount == 0) {
                //说明此前每个gap值都大于或等于0，那么该对象在所有节点都已分配松驰值，
                // 剩余的直接分配给中心处理节点
                reSlackMap.put(0, leftSlack);
            } else {
                //减去中心处理节点一定会分配到的slack值
                leftSlack -= even;
                long underZeroOver = leftSlack % underZeroCount;
                //多余的值也分配给中心处理节点
                reSlackMap.put(0, even + underZeroOver);
                long underZeroEven = leftSlack / underZeroCount;  //加1是中心处理节点占一
                for (Map.Entry<Integer, Long> entry : nodeGapMap.entrySet()) {
                    long gap = entry.getValue();
                    if (gap < 0) {
                        long local = -gap * underZeroEven;
                        reSlackMap.put(entry.getKey(), local);
                    }
                }
            }
            reComputeSlackMap.put(obj, reSlackMap);
        }

        Map<Integer, Map<String, Long>> newFactorMap = new HashMap<>();
        for (Integer nodeId : nodeSet) {
            newFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        for (String obj : slackMap.keySet()) {
            for (Integer nodeId : nodeSet) {
                long localObjCount = valFormat(nodeCountMap.get(nodeId), obj);
                long localSlack = reComputeSlackMap.get(obj).get(nodeId) + boundMap.get(nodeId) - localObjCount;
                newFactorMap.get(nodeId).put(obj, localSlack);
                nodeFactorMap.get(nodeId).put(obj, localSlack);
            }
            globalSlackMap.put(obj, globalBoundVal + reComputeSlackMap.get(obj).get(0));
            /*if (topKSet.contains(obj)) {
                globalSlackMap.put(obj, globalBoundVal + reComputeSlackMap.get(obj).get(0) + errorFactor);
            } else {
                globalSlackMap.put(obj, globalBoundVal + reComputeSlackMap.get(obj).get(0));
            }*/
        }
        LOG.debug("object node gap map: [{}]", objectNodeGapMap);
        LOG.debug("object slack map: [{}]", slackMap);
        LOG.debug("object reallocated slack map: [{}]", reComputeSlackMap);
        LOG.debug("object new factor map: [{}]", newFactorMap);
        LOG.debug("global slack map: [{}]", globalSlackMap);
        return newFactorMap;
    }

    /**
     * 对应论文中算法一的第9行
     *
     * @param set
     * @return
     */
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

    /**
     *
     * @param set
     * @return
     */
    private long findMaxSlackInSet(Set<String> set) {
        Long maxVal = null;
        for (String obj : globalSlackMap.keySet()) {
            if (set.contains(obj)) {
                if (maxVal == null || maxVal < globalSlackMap.get(obj)) {
                    maxVal = globalSlackMap.get(obj);
                }
            }
        }
        if (maxVal == null) maxVal = 0l;
        return maxVal;
    }

    /**
     * 获取指定对象在指定map中的value
     *
     * @param map
     * @param obj
     * @return
     */
    private long valFormat(Map<String, Long> map, String obj) {
        return map.get(obj) == null ? 0 : map.get(obj);
    }

}