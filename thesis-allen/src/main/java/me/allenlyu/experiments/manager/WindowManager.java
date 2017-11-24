package me.allenlyu.experiments.manager;

import me.allenlyu.experiments.common.ConfigName;
import me.allenlyu.experiments.common.StreamIds;
import me.allenlyu.experiments.utils.CostUtils;
import me.allenlyu.experiments.utils.MapUtils;
import me.allenlyu.experiments.utils.TopKConfig;
import org.apache.storm.task.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * WindowManager
 *
 * @author Allen Jin
 * @date 2017/02/16
 */
public class WindowManager {

    private static final Logger LOG = LoggerFactory.getLogger(WindowManager.class);

    private Queue<SlideUnit> slideUnitQueue;
    private long slideUnitSize;
    private SlideUnit currentUnit;
    private ExecutorService slideThread;
    private Map<String, Long> itemCountMap;
    private OutputCollector collector;
    private long slideUnitNum;
    private LocalInfoManager localInfoManager;

    public WindowManager(OutputCollector collector, LocalInfoManager localInfoManager) {
        long beginTime = TopKConfig.getLong(ConfigName.BEGIN_TIME);
        this.slideUnitSize = TopKConfig.getLong(ConfigName.WINDOW_UNIT_SIZE);
        long windowSize = TopKConfig.getLong(ConfigName.WINDOW_SIZE);
        this.collector = collector;
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
        itemCountMap = MapUtils.combine(itemCountMap, lastUnit.getItemCountMap());
        if (slideUnitQueue.size() > slideUnitNum) {
            //init topK
            if (slideUnitQueue.size() == slideUnitNum + 1) {
//                LOG.info("init, {}", itemCountMap);
//                LOG.info("init queue: {}", slideUnitQueue);
                List list = new ArrayList<>();
                list.add(itemCountMap);
                collector.emit(StreamIds.INIT, list);
            } else {    // slide
                CostUtils.print();
                CostUtils.incSlide();
                SlideUnit expiredUnit = slideUnitQueue.poll();
                itemCountMap = MapUtils.subtract(itemCountMap, expiredUnit.getItemCountMap());
                localInfoManager.reComputeInfo(itemCountMap);
//                LOG.info("time={}, cost={}", lastUnit.getStartTime(), CostUtils.print());
//                LOG.info("expiredUnit = {}", expiredUnit);
            }
        } else {
//            LOG.info("append to window, lastUnit= {}", lastUnit);
        }
//        LOG.info("itemCountMap = {}", itemCountMap);
    }

    public void putRecord(String obj, long timestamp) {
        long endTime = currentUnit.getStartTime() + slideUnitSize;
        if (timestamp >= currentUnit.getStartTime()
                && timestamp < endTime) {
            currentUnit.inc(obj);
        } else {
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
