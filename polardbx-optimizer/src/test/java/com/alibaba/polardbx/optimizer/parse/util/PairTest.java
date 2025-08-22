package com.alibaba.polardbx.optimizer.parse.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class PairTest {

    @Test
    public void testHashCode() {
        Pair<Integer, Integer> pair = new Pair<>(1, 2);
        Pair<Integer, Integer> pair2 = new Pair<>(2, 3);
        Pair<Integer, Integer> pair3 = new Pair<>(2, 1);
        assertNotEquals(pair.hashCode(), pair2.hashCode());
        assertNotEquals(pair.hashCode(), pair3.hashCode());
        assertNotEquals(pair2.hashCode(), pair3.hashCode());
    }
}