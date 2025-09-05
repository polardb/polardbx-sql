package com.alibaba.polardbx.executor.mpp.operator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DriverRuntimeStatisticsUpdaterTest {

    private DriverContext.DriverRuntimeStatisticsUpdater statisticsUpdater;

    @Before
    public void setUp() {
        statisticsUpdater = new DriverContext.DriverRuntimeStatisticsUpdater();
        statisticsUpdater.markStartTimestamp();
    }

    @Test
    public void testStartAndFinishPending() {
        statisticsUpdater.startPending();
        sleep(100); // Simulate some pending time
        statisticsUpdater.finishPending();

        // Check the costs and counts
        DriverContext.DriverRuntimeStatistics stats = statisticsUpdater.build();
        assertTrue("Pending cost should match expected", stats.getPendingCost() > 100);
        assertEquals("Pending count should be 1", 1, stats.getPendingCount());
        assertEquals("Running count should be 0", 0, stats.getRunningCount());
        assertEquals("Blocked count should be 0", 0, stats.getBlockedCount());
    }

    @Test
    public void testStartAndFinishRunning() {
        statisticsUpdater.startRunning();
        sleep(200); // Simulate some running time
        statisticsUpdater.finishRunning();

        // Check the costs and counts
        DriverContext.DriverRuntimeStatistics stats = statisticsUpdater.build();
        assertTrue("Running cost should match expected", 200 < stats.getRunningCost());
        assertEquals("Running count should be 1", 1, stats.getRunningCount());
        assertEquals("Pending count should be 0", 0, stats.getPendingCount());
        assertEquals("Blocked count should be 0", 0, stats.getBlockedCount());
    }

    @Test
    public void testStartAndFinishBlocked() {
        statisticsUpdater.startBlocked();
        sleep(150); // Simulate some blocked time
        statisticsUpdater.finishBlocked();

        // Check the costs and counts
        DriverContext.DriverRuntimeStatistics stats = statisticsUpdater.build();
        assertTrue("Blocked cost should match expected", 150 > stats.getBlockedCost());
        assertEquals("Blocked count should be 1", 1, stats.getBlockedCount());
        assertEquals("Running count should be 0", 0, stats.getRunningCount());
        assertEquals("Pending count should be 0", 0, stats.getPendingCount());
    }

    @Test
    public void testMultipleStates() {
        statisticsUpdater.startPending();
        sleep(50); // Simulate some pending time
        statisticsUpdater.finishPending();

        statisticsUpdater.startBlocked();
        sleep(100); // Simulate some blocked time
        statisticsUpdater.finishBlocked();

        statisticsUpdater.startRunning();
        sleep(200); // Simulate some running time
        statisticsUpdater.finishRunning();

        // Check the costs and counts
        DriverContext.DriverRuntimeStatistics stats = statisticsUpdater.build();
        assertTrue("Pending cost should match expected", 50 < stats.getPendingCost());
        assertTrue("Blocked cost should match expected", 100 > stats.getBlockedCost());
        assertTrue("Running cost should match expected", 200 < stats.getRunningCost());
        assertEquals("Pending count should be 1", 1, stats.getPendingCount());
        assertEquals("Blocked count should be 1", 1, stats.getBlockedCount());
        assertEquals("Running count should be 1", 1, stats.getRunningCount());
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
