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

package com.alibaba.polardbx.gms.lease;

import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;

import java.util.Optional;

public interface LeaseManager {

    /**
     * @param schemaName
     * @param leaseKey
     * @param ttlMillis
     * @return
     *      Optional.empty()    if failed
     *      LeaseRecord         if succeed
     */
    Optional<LeaseRecord> acquire(String schemaName, String leaseKey, long ttlMillis);

    Optional<LeaseRecord> extend(String leaseKey);

    long timeRemaining(String leasekey);

    void release(String leaseKey);

}