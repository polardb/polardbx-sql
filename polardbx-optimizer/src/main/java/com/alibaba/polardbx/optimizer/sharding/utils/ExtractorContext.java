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

package com.alibaba.polardbx.optimizer.sharding.utils;

import com.alibaba.polardbx.optimizer.sharding.LabelBuilder;
import com.alibaba.polardbx.optimizer.sharding.RelToLabelConverter;
import com.alibaba.polardbx.optimizer.sharding.RexExtractorContext;
import org.apache.calcite.rel.RelNode;

/**
 * @author chenmo.cm
 * @date 2019-08-13 16:48
 */
public class ExtractorContext {

    private final DigestCache digestCache;
    private final ExtractorType type;

    private ExtractorContext(ExtractorType type){
        this.digestCache = new DigestCache();
        this.type = type;
    }

    public DigestCache getDigestCache() {
        return digestCache;
    }

    public static ExtractorContext emptyContext(ExtractorType type) {
        return new ExtractorContext(type);
    }

    public ExtractorType getType() {
        return type;
    }

    public RexExtractorContext createRexExtractor(RelToLabelConverter relToLabelConverter, RelNode relNode) {
        return RexExtractorContext.create(type, relToLabelConverter, relNode);
    }

    public LabelBuilder labelBuilder() {
        return LabelBuilder.create(this);
    }
}
