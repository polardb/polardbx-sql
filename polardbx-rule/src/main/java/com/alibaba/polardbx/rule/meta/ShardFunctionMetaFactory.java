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

package com.alibaba.polardbx.rule.meta;

/**
 * @author chenghui.lch 2017年6月12日 上午1:37:37
 * @since 5.0.0
 */
public class ShardFunctionMetaFactory {

    public static ShardFunctionMeta getShardFunctionMetaByPartitionByType(String shardFuncName) {

        ShardFunctionMeta shardFunctionMeta = null;
        if (shardFuncName.toUpperCase().equals("RIGHT_SHIFT")) {
            shardFunctionMeta = new BinRightShiftMeta();
        } else if (shardFuncName.toUpperCase().equals("RANGE_HASH")) {
            shardFunctionMeta = new RangeHashMeta();
        } else if (shardFuncName.toUpperCase().equals("RANGE_HASH1")) {
            shardFunctionMeta = new RangeHash1Meta();
        } else if (shardFuncName.toUpperCase().equals("UNI_HASH")) {
            shardFunctionMeta = new UniformHashMeta();
        } else if (shardFuncName.toUpperCase().equals("STR_HASH")) {
            shardFunctionMeta = new StringHashMeta();
        }

        return shardFunctionMeta;
    }
}
