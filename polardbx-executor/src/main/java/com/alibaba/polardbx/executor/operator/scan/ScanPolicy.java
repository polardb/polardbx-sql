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

package com.alibaba.polardbx.executor.operator.scan;

/**
 * Policy of data scan.
 */
public enum ScanPolicy {
    IO_PRIORITY(1),

    FILTER_PRIORITY(2),

    MERGE_IO(3),

    DELETED_SCAN(4);

    private final int policyId;

    ScanPolicy(int policyId) {
        this.policyId = policyId;
    }

    public int getPolicyId() {
        return policyId;
    }

    public static ScanPolicy of(final int policyId) {
        switch (policyId) {
        case 4:
            return DELETED_SCAN;
        case 2:
            return FILTER_PRIORITY;
        case 3:
            return MERGE_IO;
        case 1:
        default:
            return IO_PRIORITY;
        }
    }
}
