package com.alibaba.polardbx.optimizer.spill;

import org.junit.Test;
import org.mockito.Mockito;

public class SpillSpaceManagerTest {

    @Test(expected = RuntimeException.class)
    public void testMaxLimit() {
        SpillSpaceManager spillSpaceMonitor = Mockito.spy(new SpillSpaceManager());
        Mockito.when(spillSpaceMonitor.getCurrentMaxSpillBytes()).thenReturn(100L);
        spillSpaceMonitor.updateBytes(100);
    }

    @Test
    public void testCleanup() {
        SpillSpaceManager spillSpaceMonitor = Mockito.spy(new SpillSpaceManager());
        Mockito.when(spillSpaceMonitor.getCurrentMaxSpillBytes()).thenReturn(200L);
        spillSpaceMonitor.updateBytes(100);
        spillSpaceMonitor.updateBytes(-100);
        spillSpaceMonitor.updateBytes(102);
    }
}
