package me.allenlyu.experiments.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CostUtils
 *
 * @author Allen Jin
 * @date 2017/01/10
 */
public class CostUtils {
    public static final Logger LOG = LoggerFactory.getLogger(CostUtils.class);
    private static AtomicLong total = new AtomicLong(0);

    private static AtomicLong partialCount = new AtomicLong(0);

    private static AtomicLong slideCount = new AtomicLong(0);

    private static AtomicLong validCount = new AtomicLong(0);

    public static void inc() {
        total.incrementAndGet();
    }

    public static void print() {
        LOG.info("slide Num={}, cost={},partial = [{}], valid = {}", slideCount.get(), total, partialCount, validCount);
//        long interval = slideCount.get() * TopKConfig.getLong(ConfigName.WINDOW_UNIT_SIZE);
//        if (interval % (60 * 1000) == 0) {
//            LOG.info("slide={}, cost={}", slideCount.get(), total);
//        }
    }

    public static void incSlide() {
        slideCount.incrementAndGet();
    }

    public static void incPartial() {
        partialCount.incrementAndGet();
    }

    public static void incValid() {
        validCount.incrementAndGet();
    }

    public static void printAll() {
        LOG.info("current Date = {}\n" +
                        "current Node send message count = [{}], \n" +
                        "current Node slide count = [{}], \n" +
                        "current Node violate count = [{}], \n" +
                        "current Node valid count = [{}]",
                new Date(),
                total.longValue(),
                slideCount.longValue(),
                partialCount.longValue(),
                validCount.longValue());
    }
}
