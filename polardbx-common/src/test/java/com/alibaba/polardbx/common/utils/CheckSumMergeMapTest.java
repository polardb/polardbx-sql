package com.alibaba.polardbx.common.utils;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CheckSumMergeMapTest {

    @Test
    public void test() {
        Map<String, Object> base = ImmutableMap.<String, Object>builder()
            .put("a", 1).put("b", 2).put("c", 3).build();
        MergeHashMap<String, Object> merged = new MergeHashMap<>(base);

        assertEquals(2, merged.put("b", null));
        assertEquals(3, merged.put("c", 30));
        assertNull(merged.put("d", 40));

        assertEquals(1, merged.get("a"));
        assertNull(merged.get("b"));
        assertEquals(30, merged.get("c"));
        assertEquals(40, merged.get("d"));

        assertEquals(4, merged.size());
        assertFalse(merged.isEmpty());
        assertTrue(merged.containsKey("b"));
    }

    @Test
    public void testDeepCopy() {
        Map<String, Object> base = ImmutableMap.<String, Object>builder()
            .put("a", 1).put("b", 2).put("c", 3).build();
        MergeHashMap<String, Object> merged = new MergeHashMap<>(base);

        assertEquals(2, merged.put("b", null));
        assertEquals(3, merged.put("c", 30));
        assertNull(merged.put("d", 40));

        assertEquals(1, merged.get("a"));
        assertNull(merged.get("b"));
        assertEquals(30, merged.get("c"));
        assertEquals(40, merged.get("d"));

        assertEquals(4, merged.size());
        assertFalse(merged.isEmpty());
        assertTrue(merged.containsKey("b"));

        Map<String, Object> newMerged = merged.deepCopy();
        newMerged.put("b", 20);
        newMerged.put("c", 15);
        newMerged.put("e", 30);
        assertEquals(merged.get("a"), newMerged.get("a"));
        assertNotEquals(merged.get("b"), newMerged.get("b"));
        assertNotEquals(merged.get("c"), newMerged.get("c"));
        assertNotEquals(merged.size(), newMerged.size());
        assertEquals(5, newMerged.size());

    }

    @Test
    public void testEntrySet() {

        Map<String, Object> base = ImmutableMap.<String, Object>builder()
            .put("a", 1).put("b", 2).put("c", 3).build();

        MergeHashMap<String, Object> merged = new MergeHashMap<>(base);
        HashMap<String, Object> targetMap = new HashMap<>(base);

        Set set1 = merged.entrySet();
        Set targetSet1 = targetMap.entrySet();
        assertTrue(set1.equals(targetSet1));

        assertEquals(2, merged.put("b", null));
        assertEquals(3, merged.put("c", 30));
        assertNull(merged.put("d", 40));

        targetMap.put("b", null);
        targetMap.put("c", 30);
        targetMap.put("d", 40);

        Set set2 = merged.entrySet();
        Set targetSet2 = targetMap.entrySet();
        assertTrue(set2.equals(targetSet2));

        assertEquals(null, merged.put("b", 2));
        assertEquals(30, merged.put("c", 3));

        targetMap.put("b", 2);
        targetMap.put("c", 3);

        Set set3 = merged.entrySet();
        Set targetSet3 = targetMap.entrySet();
        assertTrue(set3.equals(targetSet3));

        assertTrue(targetSet3.equals(targetSet2));

    }
}