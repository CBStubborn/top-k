package thesis.topk.monitoring.beans;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class SlideUnit {

    private long startTime;

    //存储此slide unit内的对象及其出现次数的映射
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