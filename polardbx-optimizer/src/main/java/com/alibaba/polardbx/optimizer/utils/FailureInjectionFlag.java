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

package com.alibaba.polardbx.optimizer.utils;

public class FailureInjectionFlag {

    public final boolean failDuringPrimaryCommit;
    public final boolean failBeforePrimaryCommit;
    public final boolean failAfterPrimaryCommit;
    public final boolean delayBeforeWriteCommitLog;

    private FailureInjectionFlag(boolean f0, boolean f1, boolean f3, boolean f4) {
        this.failDuringPrimaryCommit = f0;
        this.failBeforePrimaryCommit = f1;
        this.failAfterPrimaryCommit = f3;
        this.delayBeforeWriteCommitLog = f4;
    }

    public static FailureInjectionFlag parseString(String str) {
        return new FailureInjectionFlag(
            str.contains("FAIL_DURING_PRIMARY_COMMIT"),
            str.contains("FAIL_BEFORE_PRIMARY_COMMIT"),
            str.contains("FAIL_AFTER_PRIMARY_COMMIT"),
            str.contains("DELAY_BEFORE_WRITE_COMMIT_LOG")
        );
    }

    public static final FailureInjectionFlag EMPTY =
        new FailureInjectionFlag(false, false, false, false);
}
