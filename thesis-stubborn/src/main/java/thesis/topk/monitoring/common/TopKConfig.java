package thesis.topk.monitoring.common;

import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class TopKConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TopKConfig.class);

    private static Map<String, Object> configMap = new HashMap<>();

    private static final String CONFIG_FILE = "conf/config.yaml";

    static {
        loadConfig();
    }

    private static void loadConfig() {
        try {
            initDefault();
            File configFile = new File(CONFIG_FILE);
            FileReader fileReader = new FileReader(configFile);
            Yaml yaml = new Yaml();
            LinkedHashMap map = (LinkedHashMap<String, Object>) yaml.load(fileReader);
            if (map != null) {
                for (String key : configMap.keySet()) {
                    if (map.get(key) != null) {
                        configMap.put(key, map.get(key));
                    }
                }
            }
            LOG.info("configuration : {}", configMap);
        } catch (FileNotFoundException e) {
            LOG.error("cannot found config file, it's path is: {}, please check it!!!", CONFIG_FILE);
            System.exit(1);
        }
    }

    private static void initDefault() {
        configMap.put(ConfigName.STORM_IS_LOCAL, true);
        configMap.put(ConfigName.STORM_WORKER_NUM, 10);
        configMap.put(ConfigName.STORM_IS_DEBUG, false);
        configMap.put(ConfigName.K, 10);
        configMap.put(ConfigName.ERROR_FACTOR, 0);
        configMap.put(ConfigName.MONITORING_NODE_NUM, 15);
        configMap.put(ConfigName.WITH_SLACK, "part");    //是否在coordinator节点存储slack
        configMap.put(ConfigName.IS_PLAIN, false);
        configMap.put(ConfigName.BEGIN_TIME, System.currentTimeMillis());
        configMap.put(ConfigName.WINDOW_SIZE, 5 * 60 * 1000);
        configMap.put(ConfigName.WINDOW_UNIT_SIZE, 5 * 1000);
        configMap.put(ConfigName.IS_REAL_DATA_SET, true);
        configMap.put(ConfigName.TRAFFIC_DATA_FILE, "E:\\top-k\\2016-08\\processed\\10\\valid-item.record");
    }

    public static String get(String key) {
        return String.valueOf(configMap.get(key));
    }

    public static long getLong(String key) {
        return Long.valueOf(get(key));
    }

    public static int getInt(String key) {
        return Integer.valueOf(get(key));
    }

    public static boolean getBoolean(String key) {
        return Boolean.valueOf(get(key));
    }

}