package me.allenlyu.experiments.manager;

import me.allenlyu.experiments.common.NodeViolationBundle;
import me.allenlyu.experiments.utils.MapUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * GlobalInfoManager
 *
 * @author Allen Jin
 * @date 2017/02/16
 */
public class GlobalInfoManager {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalInfoManager.class);
    private OutputCollector collector;
    private AtomicBoolean isViolating;
    private Map<Integer, NodeViolationBundle> violateNodeMap;
    private ResolutionManager resolutionManager;
    private int K;

    public GlobalInfoManager(OutputCollector collector, List<Integer> nodeIdList, int K, int errorFactor) {
        this.collector = collector;
        this.K = K;
        isViolating = new AtomicBoolean(false);
        resolutionManager = new ResolutionManager(collector, nodeIdList, K, errorFactor);
    }

    public void registerViolateNode(Tuple tuple) {
        NodeViolationBundle bundle = new NodeViolationBundle().builder(tuple);
        if (!isViolating.getAndSet(true)) {
//            LOG.info("first true");
            violateNodeMap = new ConcurrentHashMap<>();
            violateNodeMap.put(tuple.getSourceTask(), bundle);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(100l);
                        LOG.info("wait finished: {}", System.currentTimeMillis());
                        Map<Integer, NodeViolationBundle> cacheMap = new HashMap<>(violateNodeMap);
                        violateNodeMap = null;
                        isViolating.set(false);
                        resolutionManager.handleViolation(cacheMap);
                    } catch (InterruptedException e) {
                        LOG.info("violate thread interrupted");
                    }
                }
            }).start();
        } else {
            LOG.info("other: {}", System.currentTimeMillis());
            if (violateNodeMap != null)
                violateNodeMap.put(tuple.getSourceTask(), bundle);
        }
    }

    public void receiveLocalInfo(Tuple tuple) {
        resolutionManager.handleLocalInfo(tuple);
    }

    private Map<String, Long> unitCountMap(Map<Integer, Map<String, Long>> nodeCountMap) {
        Map<String, Long> countMap = new HashMap<>();
        for (Integer nodeId : nodeCountMap.keySet()) {
            countMap = MapUtils.combine(countMap, nodeCountMap.get(nodeId));
        }
        return countMap;
    }

    public void init(Map<Integer, Map<String, Long>> nodeCountMap) {
        Map<String, Long> unitCountMap = unitCountMap(nodeCountMap);
        Map<String, Long> newTopKMap = MapUtils.findTopK(unitCountMap, K);
        LOG.info("topKMap = {}", newTopKMap);
        Set<String> topKSet = newTopKMap.keySet();
        Map<Integer, Map<String, Long>> initFactorMap = new HashMap<>();
        for (Integer nodeId : nodeCountMap.keySet()) {
            initFactorMap.put(nodeId, new HashMap<String, Long>());
        }
        resolutionManager.initTopKSet(topKSet);
        resolutionManager.initNodeFactorMap(initFactorMap);
        resolutionManager.assignInfoToNode(topKSet, initFactorMap);
        //        Map<Integer, Long> boundMap = computeBoundMap(topKSet, nodeCountMap);
//        LOG.info("boundMap = {}", boundMap);
//        Map<String, Long> slackMap = computeSlackMap(topKSet, nodeCountMap, boundMap);
//        LOG.info("slackMap = {}", slackMap);
//        Map<Integer, Map<String, Long>> newFactorMap = computeFactor(nodeCountMap, slackMap, boundMap);
//        LOG.info("factorMap = {}", newFactorMap);
//        updateFactorMap(newFactorMap);
    }
}
