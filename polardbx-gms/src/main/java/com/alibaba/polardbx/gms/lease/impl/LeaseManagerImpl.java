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

package com.alibaba.polardbx.gms.lease.impl;

import com.alibaba.polardbx.gms.lease.LeaseManager;
import com.alibaba.polardbx.gms.metadb.lease.LeaseAccessDelegate;
import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;

import java.util.Optional;

public class LeaseManagerImpl implements LeaseManager {

    @Override
    public Optional<LeaseRecord> acquire(String schemaName, String leaseKey, long ttlMillis) {
        return new LeaseAccessDelegate<Optional<LeaseRecord>>(){
            @Override
            protected Optional<LeaseRecord> invoke() {
                accessor.deleteIfExpired(leaseKey);
                int insertCount = accessor.acquire(LeaseRecord.create(schemaName, leaseKey, ttlMillis));
                if(insertCount != 1){
                    //todo 最好改成幂等的
                    return Optional.empty();
                }
                Optional<LeaseRecord> recordInDb = accessor.queryByHolderAndKey(LeaseRecord.getLeaseHolder(), leaseKey);
                if(!recordInDb.isPresent()){
                    return Optional.empty();
                }
                return recordInDb;
            }
        }.execute();
    }

    @Override
    public Optional<LeaseRecord> extend(String leaseKey) {
        return new LeaseAccessDelegate<Optional<LeaseRecord>>(){
            @Override
            protected Optional<LeaseRecord> invoke() {
                int count = accessor.extendByHolderAndKey(LeaseRecord.getLeaseHolder(), leaseKey);
                if(count != 1){
                    return Optional.empty();
                }
                Optional<LeaseRecord> recordInDb = accessor.queryByHolderAndKey(LeaseRecord.getLeaseHolder(), leaseKey);
                if(!recordInDb.isPresent()){
                    return Optional.empty();
                }
                return recordInDb;
            }
        }.execute();
    }

    @Override
    public long timeRemaining(String leaseKey) {
        return new LeaseAccessDelegate<Long>(){
            @Override
            protected Long invoke() {
                return accessor.selectRemainingTime(LeaseRecord.getLeaseHolder(), leaseKey);
            }
        }.execute();
    }

    @Override
    public void release(String leaseKey) {
        new LeaseAccessDelegate<Integer>(){
            @Override
            protected Integer invoke() {
                return accessor.deleteByHolderAndKey(LeaseRecord.getLeaseHolder(), leaseKey);
            }
        }.execute();
    }
}