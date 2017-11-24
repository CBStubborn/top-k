package thesis.topk.monitoring.common;

import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class NodeViolationBundle {

    private static final Logger LOG = LoggerFactory.getLogger(NodeViolationBundle.class);

    private Integer nodeId;

    //violated object set in local monitoring node
    private Set<String> violateSet;

    //object-count map in local monitoring node，
    //objects contains top-k and violation object(it represented by Rj in our paper)
    private Map<String, Long> objectCountMap;

    //local bound value
    private Long boundVal;

    /**
     * build a entity
     *
     * @param tuple
     * @return
     */
    public NodeViolationBundle builder(Tuple tuple) {
        Integer nodeId = tuple.getSourceTask();
        if (StreamIds.LOCAL_VIOLATE.equalsIgnoreCase(tuple.getSourceStreamId())) {
            Set<String> violateSet = (Set<String>) tuple.getValueByField(MsgField.VIOLATE_SET);
            setViolateSet(violateSet);
            LOG.debug("violated set is: {} in node: [{}]", violateSet.toString(), nodeId);
        }
        Map<String, Long> objectCountMap = (Map<String, Long>) tuple.getValueByField(MsgField.LOCAL_COUNT_MAP);
        LOG.debug("In node: [{}], the object count map is: [{}]", nodeId, objectCountMap);
        Long localBoundVal = tuple.getLongByField(MsgField.LOCAL_BOUND_VAL);
        LOG.debug("local bound is: [{}]", localBoundVal);
        setNodeId(nodeId);
        setObjectCountMap(objectCountMap);
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

    public Map<String, Long> getObjectCountMap() {
        return objectCountMap;
    }

    public void setObjectCountMap(Map<String, Long> objectCountMap) {
        this.objectCountMap = objectCountMap;
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
                ", objectCountMap=" + objectCountMap +
                ", boundVal=" + boundVal +
                '}';
    }
}