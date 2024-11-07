/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class MergedStorageInfo {
    private final boolean supportXA;
    private final boolean supportTso;
    private final boolean supportPurgeTso;
    private final boolean supportTsoHeartbeat;
    private final boolean supportCtsTransaction;
    private final boolean supportAsyncCommit;
    private final boolean supportLizard1PCTransaction;
    private final boolean supportDeadlockDetection;
    private final boolean supportMdlDeadlockDetection;
    private final boolean supportsBloomFilter;
    private final boolean supportOpenSSL;
    private final boolean supportSharedReadView;
    private final boolean supportsReturning;
    private final boolean supportsBackfillReturning;
    private final boolean supportsAlterType;
    private final boolean readOnly;
    private final boolean lowerCaseTableNames;
    private final boolean supportHyperLogLog;
    private final boolean lessMy56Version;
    private final boolean supportXxHash;
    private final boolean isMysql80;

    /**
     * FastChecker: generate checksum on xdb node
     * Since: 5.4.13 fix
     * Requirement: XDB supports HASHCHECK function
     */
    private final boolean supportFastChecker;

    private final boolean supportChangeSet;

    private final boolean supportXOptForAutoSp;

    private final boolean supportXRpc;

    private final boolean supportMarkDistributed;

    private final boolean supportXOptForPhysicalBackfill;

    private final boolean supportSyncPoint;

    private final boolean supportFlashbackArea;
}
