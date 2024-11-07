package com.alibaba.polardbx.executor.gms;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class FlashbackDeleteBitmapManagerTest {

    @Test
    public void testWeakReferenceGC() {
        FlashbackDeleteBitmapManager manager =
            FlashbackDeleteBitmapManager.getInstance(1L, "test_schema", "test_table", "p0", Collections.emptyList());
        assertNotNull(manager);
        assertEquals(1, FlashbackDeleteBitmapManager.MANAGER_MAP.size());

        manager = null;

        // before GC
        assertEquals(1, FlashbackDeleteBitmapManager.MANAGER_MAP.size());

        System.gc();
        int i = 60;
        int referenceCount = 1;
        while (i-- > 0) {
            referenceCount = FlashbackDeleteBitmapManager.MANAGER_MAP.size();
            if (referenceCount == 0) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // after GC
        assertEquals(0, referenceCount);
    }
}