package com.alibaba.polardbx.executor.operator.scan.impl;

import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

public class PreheatStripeMeta {
    /**
     * Stripe id.
     */
    private final long stripeId;

    /**
     * The orc RowIndex is read from index streams.
     */
    private final OrcIndex orcIndex;

    /**
     * The stripe footer is read from the tail of a stripe.
     */
    private final OrcProto.StripeFooter stripeFooter;

    public PreheatStripeMeta(long stripeId, OrcIndex orcIndex, OrcProto.StripeFooter stripeFooter) {
        this.stripeId = stripeId;
        this.orcIndex = orcIndex;
        this.stripeFooter = stripeFooter;
    }

    public OrcProto.StripeFooter getStripeFooter() {
        return stripeFooter;
    }

    public long getStripeId() {
        return stripeId;
    }

    public OrcIndex getOrcIndex() {
        return orcIndex;
    }
}
