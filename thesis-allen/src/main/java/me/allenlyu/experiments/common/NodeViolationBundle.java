package me.allenlyu.experiments.common;

import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Set;

/**
 * NodeViolationBundle
 *
 * @author Allen Jin
 * @date 2017/02/17
 */
public class NodeViolationBundle {
    private Integer nodeId;
    private Set<String> violateSet;
    private Map<String, Long> nodeCountMap;
    private Long boundVal;

    public NodeViolationBundle builder(Tuple tuple) {
        Integer nodeId = tuple.getSourceTask();
        if (tuple.getSourceStreamId() == StreamIds.LOCAL_VIOLATE) {
            Set<String> violateSet = (Set<String>) tuple.getValueByField(MsgField.VIOLATE_SET);
            setViolateSet(violateSet);
        }
        Map<String, Long> nodeCountMap = (Map<String, Long>) tuple.getValueByField(MsgField.LOCAL_COUNT_MAP);
        Long localBoundVal = tuple.getLongByField(MsgField.LOCAL_BOUND_VAL);
        setNodeId(nodeId);
        setNodeCountMap(nodeCountMap);
        setBoundVal(localBoundVal);
        return this;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public Set<String> getViolateSet() {
        return violateSet;
    }

    public void setViolateSet(Set<String> violateSet) {
        this.violateSet = violateSet;
    }

    public Map<String, Long> getNodeCountMap() {
        return nodeCountMap;
    }

    public void setNodeCountMap(Map<String, Long> nodeCountMap) {
        this.nodeCountMap = nodeCountMap;
    }

    public Long getBoundVal() {
        return boundVal;
    }

    public void setBoundVal(Long boundVal) {
        this.boundVal = boundVal;
    }

    @Override
    public String toString() {
        return "NodeViolationBundle{" +
                "nodeId=" + nodeId +
                ", violateSet=" + violateSet +
                ", nodeCountMap=" + nodeCountMap +
                ", boundVal=" + boundVal +
                '}';
    }
}
