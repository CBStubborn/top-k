package thesis.topk.monitoring.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thesis.topk.monitoring.common.ConfigName;
import thesis.topk.monitoring.common.TopKConfig;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class CostUtil {

    public static final Logger LOG = LoggerFactory.getLogger(CostUtil.class);

    private static AtomicLong total = new AtomicLong(0);

    private static AtomicLong partialCount = new AtomicLong(0);

    private static AtomicLong slideCount = new AtomicLong(0);

    private static AtomicLong validCount = new AtomicLong(0);

    private static AtomicLong violationCount = new AtomicLong(0);

    private static AtomicInteger processedSlideCount = new AtomicInteger(0);

    private static AtomicInteger perViolationCount = new AtomicInteger(0);

    public static void inc() {
        total.incrementAndGet();
    }

    public static void print() {
        LOG.info("slide Num={}, cost={}, partial = [{}], valid = {}, violation = {}", slideCount.get(), total, partialCount, validCount, violationCount);
        long interval = slideCount.get() * TopKConfig.getLong(ConfigName.WINDOW_UNIT_SIZE);
        if (interval % (60 * 1000 * 5) == 0) {
            LOG.info("slide={}, cost={}", slideCount.get(), total);
        }
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

    public static void incViolation() {
        violationCount.incrementAndGet();
    }

    public static void incProcessed() {
        processedSlideCount.incrementAndGet();
    }

    public static int getProcessed() {
        return processedSlideCount.get();
    }

    public static void setProcessed(int newValue) {
        processedSlideCount.set(newValue);
    }

    public static void incPerViolation() {
        perViolationCount.incrementAndGet();
    }

    public static int getPerViolation() {
        return perViolationCount.get();
    }

    public static int decPerViolation() {
        return perViolationCount.decrementAndGet();
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