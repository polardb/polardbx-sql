package com.alibaba.polardbx.common.memory;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.GraphLayout;

import java.lang.ref.WeakReference;
import java.text.MessageFormat;

public interface MemoryCountable {

    static double getDeviation(MemoryCountable countable) {
        GraphLayout layout = GraphLayout.parseInstance(countable);
        MemoryUsageReport reportWithoutAnnotation =
            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, false, true, false);
        MemoryUsageReport report = FastMemoryCounter.parseInstance(countable);
        long estimatedSize = countable.getMemoryUsage();

        Preconditions.checkArgument(layout.totalSize() == reportWithoutAnnotation.getTotalSize(),
            MessageFormat.format("GraphLayout = {0}, but FastMemoryCounter = {1}", layout.totalSize(),
                reportWithoutAnnotation.getTotalSize()));
        long expected = report.getTotalSize();

        double deviation = ((double) (estimatedSize - expected) / expected);
        return deviation;
    }

    static void checkDeviation(MemoryCountable countable, double expectedDeviation) {
        checkDeviation(countable, expectedDeviation, false);
    }

    static void checkDeviation(MemoryCountable countable, double expectedDeviation, boolean printVerbose) {
        // statistics 1: GraphLayout memory dump.
        GraphLayout layout = GraphLayout.parseInstance(countable);

        // statistics 2: TreeStructGraphLayout memory dump. (for test)
        TreeStructGraphLayout treeLayout = TreeStructGraphLayout.parseInstance(countable);

        // statistics 3: FastMemoryCounter memory dump without any config;
        MemoryUsageReport reportWithoutAnnotation =
            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, false, false, false);

        Preconditions.checkArgument(layout.totalSize() == treeLayout.totalSize(),
            MessageFormat.format("GraphLayout = {0}, but TreeStructGraphLayout = {1}", layout.totalSize(),
                treeLayout.totalSize()));

        Preconditions.checkArgument(layout.totalSize() == reportWithoutAnnotation.getTotalSize(),
            MessageFormat.format("GraphLayout = {0}, but FastMemoryCounter = {1}, tree struct =\n{2}, \n\ncounter struct =\n{3}\n\n", layout.totalSize(),
                reportWithoutAnnotation.getTotalSize(), treeLayout.printTree(), reportWithoutAnnotation.getMemoryUsageTree()));

        // statistics 3: FastMemoryCounter memory dump with fast config;
        MemoryUsageReport report =
            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, true, true, true);
        long estimatedSize = countable.getMemoryUsage();

        long expected = report.getTotalSize();

        double deviation = ((double) (estimatedSize - expected) / expected);

        Preconditions.checkArgument(Math.abs(deviation) <= expectedDeviation,
            MessageFormat.format(
                "memory estimated = {0}, deviation = {1}, but memory report = {2}, expected deviation = {3}, tree = \n{4}",
                estimatedSize, deviation, expected, expectedDeviation, report.getMemoryUsageTree()));

        if (printVerbose) {
            System.out.println("Deviation : " + deviation);
            System.out.println(report.getMemoryUsageTree());
        }
    }

    long getMemoryUsage();
}
