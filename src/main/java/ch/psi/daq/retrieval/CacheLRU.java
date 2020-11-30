package ch.psi.daq.retrieval;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

public class CacheLRU<K extends Comparable<K>, V> {

    class Value {
        V config;
        long ts;
    }

    TreeMap<K, Value> map = new TreeMap<>();
    int maxEntries;

    public CacheLRU(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public synchronized Optional<V> get(K k) {
        if (!map.containsKey(k)) {
            return Optional.empty();
        }
        return Optional.of(map.get(k).config);
    }

    public synchronized Optional<V> getYounger(K k, long maxAge) {
        if (!map.containsKey(k)) {
            return Optional.empty();
        }
        Value val = map.get(k);
        if (System.currentTimeMillis() - val.ts > maxAge) {
            return Optional.empty();
        }
        return Optional.of(val.config);
    }

    public synchronized void insert(K k, V v) {
        gcMaybe();
        Value val = new Value();
        val.config = v;
        val.ts = System.currentTimeMillis();
        map.put(k, val);
    }

    public synchronized void gcMaybe() {
        if (map.size() <= 1.5 * maxEntries) {
            return;
        }
        gc();
    }

    public synchronized void gc() {
        List<Long> l1 = new ArrayList<>();
        for (Value v : map.values()) {
            l1.add(v.ts);
        }
        l1.sort(Long::compareTo);
        long keepTs = l1.get(map.size() - maxEntries);
        TreeMap<K, Value> map2 = new TreeMap<>();
        map.forEach((k, v) -> {
            if (v.ts >= keepTs) {
                map2.put(k, v);
            }
        });
        map = map2;
    }

}
