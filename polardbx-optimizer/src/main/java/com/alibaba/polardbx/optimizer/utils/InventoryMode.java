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

public class InventoryMode {

    public final static int COMMIT_ON_SUCCESS = 0b01;
    public final static int ROLLBACK_ON_FAIL = 0b10;
    public final static int TARGET_AFFECT_ROW = 0b100;

    private int mode = 0;

    public InventoryMode() {
    }

    public boolean isCommitOnSuccess() {
        return (mode & COMMIT_ON_SUCCESS) == COMMIT_ON_SUCCESS;
    }

    public boolean isRollbackOnFail() {
        return (mode & ROLLBACK_ON_FAIL) == ROLLBACK_ON_FAIL;
    }

    public boolean isTargetAffectRow() {
        return (mode & TARGET_AFFECT_ROW) == TARGET_AFFECT_ROW;
    }

    public boolean isInventoryHint() {
        return mode > 0;
    }

    public void enableCommitOnSuccess() {
        this.mode = mode | COMMIT_ON_SUCCESS;
    }

    public void enableRollbackOnFail() {
        this.mode = mode | ROLLBACK_ON_FAIL;
    }

    public void enableTargetAffectRow() {
        this.mode = mode | TARGET_AFFECT_ROW;
    }

    public void resetInventoryMode() {
        this.mode = 0;
    }
}
