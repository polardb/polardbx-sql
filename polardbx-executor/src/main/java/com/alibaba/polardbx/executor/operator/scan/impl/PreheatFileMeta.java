package com.alibaba.polardbx.executor.operator.scan.impl;

import org.apache.arrow.util.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;

import java.util.Map;

public class PreheatFileMeta {
    private final Path filePath;
    private Map<Long, PreheatStripeMeta> preheatStripes;
    private OrcTail preheatTail;

    public PreheatFileMeta(Path filePath) {
        this.filePath = filePath;
    }

    public Path getFilePath() {
        return filePath;
    }

    public void setPreheatStripes(
        Map<Long, PreheatStripeMeta> preheatStripes) {
        this.preheatStripes = preheatStripes;
    }

    public OrcTail getPreheatTail() {
        return preheatTail;
    }

    public Map<Long, PreheatStripeMeta> getPreheatStripes() {
        return preheatStripes;
    }

    public void setPreheatTail(OrcTail preheatTail) {
        this.preheatTail = preheatTail;
    }

    public OrcIndex getOrcIndex(long stripeIndex) {
        PreheatStripeMeta stripeMeta = preheatStripes.get(stripeIndex);
        Preconditions.checkNotNull(stripeMeta);

        return stripeMeta.getOrcIndex();
    }

    public OrcProto.StripeFooter getStripeFooter(long stripeIndex) {
        PreheatStripeMeta stripeMeta = preheatStripes.get(stripeIndex);
        Preconditions.checkNotNull(stripeMeta);

        return stripeMeta.getStripeFooter();
    }
}
