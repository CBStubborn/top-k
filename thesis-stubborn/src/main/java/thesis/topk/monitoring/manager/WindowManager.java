package thesis.topk.monitoring.manager;

import org.apache.storm.task.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.beans.SlideUnit;
import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.StreamIds;
import thesis.topk.monitoring.utils.CostUtil;
import thesis.topk.monitoring.utils.MapUtil;
import thesis.topk.monitoring.common.TopKConfig;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class WindowManager {

    private static final Logger LOG = LoggerFactory.getLogger(WindowManager.class);

    private Queue<SlideUnit> slideUnitQueue;

    private long slideUnitSize;

    private SlideUnit currentUnit;

    private ExecutorService slideThread;

    //存储一个大的监控时间窗口W内，对象及其出现次数的映射关系
    private Map<String, Long> itemCountMap;

    private OutputCollector collector;

    private long slideUnitNum;

    private LocalInfoManager localInfoManager;

    private int taskId;

    public WindowManager(OutputCollector collector, LocalInfoManager localInfoManager, int taskId) {
        long beginTime = TopKConfig.getLong(ConfigName.BEGIN_TIME);
        this.slideUnitSize = TopKConfig.getLong(ConfigName.WINDOW_UNIT_SIZE);
        long windowSize = TopKConfig.getLong(ConfigName.WINDOW_SIZE);
        this.collector = collector;
        this.taskId = taskId;
        this.slideUnitQueue = new LinkedList<>();
        this.itemCountMap = new HashMap<>();
        this.slideUnitNum = windowSize / slideUnitSize;
        this.localInfoManager = localInfoManager;
        SlideUnit firstUnit = new SlideUnit(beginTime);
        slideUnitQueue.add(firstUnit);
        currentUnit = firstUnit;
        slideThread = Executors.newSingleThreadExecutor();
    }

    private void slide(SlideUnit lastUnit) {
        LOG.info("monitoring node: {} added a new slide unit", taskId);
        if (slideUnitQueue.size() > slideUnitNum) {
            if (slideUnitQueue.size() == slideUnitNum + 1) {
                itemCountMap = MapUtil.combine(itemCountMap, lastUnit.getItemCountMap());
                List list = new ArrayList<>();
                list.add(itemCountMap);
                LOG.debug("monitoring node: {} emit an init message", taskId);
                collector.emit(StreamIds.INIT, list);
            } else {
                CostUtil.print();
                CostUtil.incSlide();
                SlideUnit expiredUnit = slideUnitQueue.poll();
                List<Map.Entry<String, Long>> beforeEntries = new ArrayList<>(itemCountMap.entrySet());
                Collections.sort(beforeEntries, new Comparator<Map.Entry<String, Long>>() {
                    @Override
                    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                        return (int) (o2.getValue() - o1.getValue());
                    }
                });
                LOG.info("monitoring node: [{}], before item count map: [{}]", taskId, beforeEntries);
                itemCountMap = MapUtil.subtract(itemCountMap, expiredUnit.getItemCountMap());
                itemCountMap = MapUtil.combine(itemCountMap, lastUnit.getItemCountMap());
                List<Map.Entry<String, Long>> afterEntries = new ArrayList<>(itemCountMap.entrySet());
                Collections.sort(afterEntries, new Comparator<Map.Entry<String, Long>>() {
                    @Override
                    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                        return (int) (o2.getValue() - o1.getValue());
                    }
                });
                LOG.info("monitoring node: [{}], after item count map: [{}]", taskId, afterEntries);
                localInfoManager.reComputeInfo(itemCountMap);
            }
        }
    }

    public void putRecord(String obj, long timestamp) {
        long endTime = currentUnit.getStartTime() + slideUnitSize;
        if (timestamp >= currentUnit.getStartTime() && timestamp < endTime) {
            currentUnit.inc(obj);
        } else {
            LOG.debug("monitoring node: [{}], creating a new slide unit, last unit begin time: {}, this unit begin time: {}",
                    taskId, currentUnit.getStartTime(), currentUnit.getStartTime() + slideUnitSize);
            SlideUnit nextUnit = new SlideUnit(endTime);
            nextUnit.inc(obj);
            final SlideUnit lastUnit = currentUnit;
            slideUnitQueue.add(nextUnit);
            currentUnit = nextUnit;
            slideThread.submit(new Runnable() {
                @Override
                public void run() {
                    slide(lastUnit);
                }
            });
        }
    }
}