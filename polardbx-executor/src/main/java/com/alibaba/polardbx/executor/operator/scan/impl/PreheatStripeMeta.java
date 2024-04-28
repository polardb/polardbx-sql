/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
