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

package com.alibaba.polardbx.executor.operator;

public interface ResumeExec {

    /**
     * @return True of false
     * true mean resume the Exec successfully.
     * False mean the Exec is really finish!
     */
    boolean resume();

    /**
     * @return True of false
     * True mean the Exec is finish temporarily, and it should  suspend.
     * False mean the Exec is still running, or it maybe really finished.
     */
    boolean shouldSuspend();

    /**
     * suspend the Exec.
     */
    void doSuspend();
}
