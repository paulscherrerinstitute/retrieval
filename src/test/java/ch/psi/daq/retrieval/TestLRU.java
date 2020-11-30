package ch.psi.daq.retrieval;

import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

public class TestLRU {

    @Test
    public void lru1() throws InterruptedException {
        CacheLRU<String, String> cache = new CacheLRU<>(2);
        cache.insert("1", "v1");
        Thread.sleep(10);
        cache.insert("2", "v2");
        Thread.sleep(10);
        cache.insert("3", "v3");
        assertEquals(3, cache.map.size());
        cache.gc();
        assertEquals(2, cache.map.size());
        assertEquals(Set.of("2", "3"), cache.map.keySet());
    }

    @Test
    public void lru2() throws InterruptedException {
        CacheLRU<String, String> cache = new CacheLRU<>(2);
        cache.insert("1", "v1");
        Thread.sleep(10);
        cache.insert("2", "v1");
        Thread.sleep(10);
        cache.insert("3", "v3");
        Thread.sleep(10);
        cache.insert("1", "v1b");
        assertEquals(3, cache.map.size());
        cache.gc();
        assertEquals(2, cache.map.size());
        assertEquals(Set.of("1", "3"), cache.map.keySet());
        cache.gcMaybe();
        assertEquals(2, cache.map.size());
        assertEquals(Set.of("1", "3"), cache.map.keySet());
        assertTrue(cache.get("X").isEmpty());
        assertEquals(Optional.of("v3"), cache.get("3"));
    }

}
