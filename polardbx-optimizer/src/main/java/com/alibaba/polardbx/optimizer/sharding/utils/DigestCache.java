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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author chenmo.cm
 * @date 2019-08-13 16:34
 */
public class DigestCache {

    private static final long            CACHE_SIZE  = TddlConstants.DEFAULT_DIGEST_CACHE_SIZE;
    private static final long            EXPIRE_TIME = TddlConstants.DEFAULT_DIGEST_EXPIRE_TIME;

    private final Cache<RexNode, String> cache;

    public DigestCache(){

        this.cache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .expireAfterWrite(EXPIRE_TIME, TimeUnit.MILLISECONDS)
            .softValues()
            .build();
    }

    public String digest(@Nonnull RexNode rexNode) {
        Preconditions.checkNotNull(rexNode);

        try {
            return cache.get(rexNode, rexNode::toString);
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, e);
        }
    }
}
