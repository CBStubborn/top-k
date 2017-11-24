package me.allenlyu.experiments.utils;

import java.util.*;

/**
 * MapUtils
 *
 * @author Allen Jin
 * @date 2017/02/16
 */
public abstract class MapUtils {

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

    public static Map<String, Long> combine(Map map1, Map map2) {
        return doUnit(map1, map2, true);
    }

    public static Map<String, Long> subtract(Map map1, Map map2) {
        return doUnit(map1, map2, false);
    }

    public static Map<String, Long> findTopK(Map<String, Long> map, int k) {
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
            topSet.add(curTop);
        }
        LinkedHashMap<String, Long> result = new LinkedHashMap<>();
        for (String top : topSet) {
            result.put(top, map.get(top));
        }
        return result;
    }

    public static Map<String, Long> findTargetMap(Map<String, Long> map, Collection<String> target) {
        Map<String, Long> result = new HashMap<>();
        if (target != null) {
            for (String item : target) {
                if (map.containsKey(item)) {
                    result.put(item, map.get(item));
                }
            }
        }
        return result;
    }

    public static long findMaxVal(Map<String, Long> map) {
        long max = Long.MIN_VALUE;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (entry.getValue() > max) {
                max = entry.getValue();
            }
        }
        return max;
    }
}
