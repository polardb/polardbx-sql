package com.alibaba.polardbx.executor.physicalbackfill;

import com.google.common.collect.ImmutableList;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PhysicalBackfillReporter {
    private final PhysicalBackfillManager backfillManager;

    /**
     * Extractor position mark
     */
    private PhysicalBackfillManager.BackfillBean backfillBean;

    public PhysicalBackfillReporter(PhysicalBackfillManager backfillManager) {
        this.backfillManager = backfillManager;
    }

    public PhysicalBackfillManager getBackfillManager() {
        return backfillManager;
    }

    public PhysicalBackfillManager.BackfillBean loadBackfillMeta(long backfillId, String schemaName, String phyDb,
                                                                 String physicalTable,
                                                                 String phyPartition) {
        backfillBean = backfillManager.loadBackfillMeta(backfillId, schemaName, phyDb, physicalTable, phyPartition);
        return backfillBean;
    }

    public PhysicalBackfillManager.BackfillBean getBackfillBean() {
        return backfillBean;
    }

    public void updateBackfillInfo(PhysicalBackfillManager.BackfillObjectBean backfillObject) {
        backfillManager.updateBackfillObjectBean(ImmutableList.of(backfillObject));
    }

    public void updateBackfillObject(PhysicalBackfillManager.BackfillObjectRecord backfillObjectRecord) {
        backfillManager.updateBackfillObject(ImmutableList.of(backfillObjectRecord));
    }

}
