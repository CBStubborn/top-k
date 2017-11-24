package thesis.topk.monitoring.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Stubborn on 2017/7/29.
 */
public class MapUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MapUtil.class);

    /**
     * @param map1
     * @param map2
     * @param isCombined
     * @return
     */
    private static Map<String, Long> doUnit(Map<String, Long> map1, Map<String, Long> map2, boolean isCombined) {
        int flag = isCombined ? 1 : -1;
        Map<String, Long> result = new HashMap<>();
        for (String map1Key : map1.keySet()) {
            //两个map中都有
            if (map2.containsKey(map1Key)) {
                result.put(map1Key, map1.get(map1Key) + (map2.get(map1Key) * flag));
            } else {    //map1中有,map2中无
                result.put(map1Key, map1.get(map1Key));
            }
        }
        //map2中有,map1中无
        for (String map2Key : map2.keySet()) {
            if (!map1.containsKey(map2Key)) {
                result.put(map2Key, map2.get(map2Key) * flag);
            }
        }
        return result;
    }

    /**
     * @param map1
     * @param map2
     * @return
     */
    public static Map<String, Long> combine(Map map1, Map map2) {
        return doUnit(map1, map2, true);
    }

    /**
     * @param map1
     * @param map2
     * @return
     */
    public static Map<String, Long> subtract(Map map1, Map map2) {
        Map<String, Long> result = doUnit(map1, map2, false);
        Iterator<Map.Entry<String, Long>> it = result.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (entry.getValue() <= 0) {
                it.remove();
            }
        }
        return result;
    }

    /**
     * @param map
     * @param k
     * @return
     */
    public static Map<String, Long> findTopK(Map<String, Long> map, int k, Set oldTopK) {
        LinkedHashMap<String, Long> result = new LinkedHashMap<>();
        if (oldTopK == null) {
            List<String> topSet = new ArrayList<>();
            for (int i = 0; i < k; i++) {
                String curTop = null;
                for (String obj : map.keySet()) {
                    if (topSet.contains(obj)) {//过滤在set中的值
                        continue;
                    }
                    if (curTop == null) {
                        curTop = obj;
                    } else if (map.get(curTop) < map.get(obj)) {
                        curTop = obj;
                    }
                }
                if (curTop != null) {
                    topSet.add(curTop);
                }
            }
            for (String top : topSet) {
                result.put(top, map.get(top));
            }
        } else {
            Map<Long, List<String>> countObjectListMap = new HashMap<>();
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                List<String> objectList = countObjectListMap.get(entry.getValue());
                if (objectList == null) {
                    objectList = new ArrayList<>();
                    countObjectListMap.put(entry.getValue(), objectList);
                }
                objectList.add(entry.getKey());
            }
            List<Map.Entry<Long, List<String>>> entries = new ArrayList<>(countObjectListMap.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<Long, List<String>>>() {
                @Override
                public int compare(Map.Entry<Long, List<String>> o1, Map.Entry<Long, List<String>> o2) {
                    return (int) (o2.getKey() - o1.getKey());
                }
            });
            for (Map.Entry<Long, List<String>> entry : entries) {
                List<String> objects = entry.getValue();
                if (objects.size() <= k) {
                    for (String obj : objects) {
                        result.put(obj, entry.getKey());
                    }
                    k -= objects.size();
                } else {
                    for (String obj : objects) {
                        if (oldTopK.contains(obj)) {
                            result.put(obj, entry.getKey());
                            --k;
                            if (k == 0) {
                                return result;
                            }
                        }
                    }
                    for (String obj : objects) {
                        if (!result.containsKey(obj)) {
                            result.put(obj, entry.getKey());
                            --k;
                            if (k == 0) {
                                return result;
                            }
                        }
                    }
                }
                if (k == 0) {
                    return result;
                }
            }
        }
        return result;
    }

    /**
     * @param map
     * @param target
     * @return
     */
    public static Map<String, Long> findTargetMap(Map<String, Long> map, Collection<String> target) {
        Map<String, Long> result = new HashMap<>();
        if (target != null) {
            for (String item : target) {
                if (map == null) {
                    LOG.error("map is null");
                    System.exit(-1);
                }
                if (map.containsKey(item)) {
                    result.put(item, map.get(item));
                }
            }
        }
        return result;
    }

    /**
     * 遍历map1中的元素，如果map2中也包含此元素，则将两者的值相加
     * 否则，只取map1中的值
     *
     * @param map1
     * @param map2
     * @return
     */
    public static Map<String, Long> mapAddMap(Map<String, Long> map1, Map<String, Long> map2) {
        Map<String, Long> addResult = new HashMap<>();
        for (Map.Entry<String, Long> entry : map1.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue();
            if (map2.containsKey(key)) {
                addResult.put(key, value + map2.get(key));
            } else {
                addResult.put(key, value);
            }
        }
        return addResult;
    }

    /**
     * 查找map中最大value值
     *
     * @param map
     * @return
     */
    public static long findMaxVal(Map<String, Long> map) {
        long max = Long.MIN_VALUE;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (entry.getValue() > max) {
                max = entry.getValue();
            }
        }
        return max;
    }


    public static List sortByValue(Map<String, Long> map) {
        List<Map.Entry<String, Long>> entries = new ArrayList<>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (int) (o2.getValue() - o1.getValue());
            }
        });
        return entries;
    }

    public static List sortByKey(Map<Long, String> map) {
        List<Map.Entry<Long, String>> entries = new ArrayList<>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<Long, String>>() {
            @Override
            public int compare(Map.Entry<Long, String> o1, Map.Entry<Long, String> o2) {
                return (int) (o2.getKey() - o1.getKey());
            }
        });
        return entries;
    }

    public static void main(String[] args) {
        HashMap testMap = new HashMap<>();
        testMap.put("djsa", 1l);
        testMap.put("faf", 2l);
        testMap.put("gefda", 4l);
        testMap.put("gafeawe", 6l);
        testMap.put("fwfwfw", 4l);
        Set set = new HashSet<>();
        set.add("gefda");
        Map topk = findTopK(testMap, 2, set);
        System.out.println(topk.toString());
    }
}