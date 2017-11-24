package me.allenlyu.experiments.manager;

import java.util.HashMap;
import java.util.Map;

/**
 * SlideUnit
 *
 * @author Allen Jin
 * @date 2017/02/16
 */
public class SlideUnit {
    private long startTime;
    private Map<String, Long> itemCountMap;

    public SlideUnit(long startTime) {
        this.startTime = startTime;
        this.itemCountMap = new HashMap<>();
    }

    public void inc(String objKey) {
        if (itemCountMap.containsKey(objKey)) {
            itemCountMap.put(objKey, Long.valueOf(itemCountMap.get(objKey) + 1L));
        } else {
            itemCountMap.put(objKey, 1L);
        }
    }

    public long getStartTime() {
        return startTime;
    }

    public Map<String, Long> getItemCountMap() {
        return itemCountMap;
    }

    @Override
    public String toString() {
        return "SlideUnit{" +
                "startTime=" + startTime +
                ", itemCountMap=" + itemCountMap +
                '}';
    }
}
