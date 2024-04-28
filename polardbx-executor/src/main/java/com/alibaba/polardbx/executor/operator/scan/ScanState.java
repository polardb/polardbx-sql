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
 * States of File Scan.
 * The transformation between states:
 * +----------+          +----------+      +---------+
 * |   READY  |<-------> | BLOCKED  |----->| FAILED  |
 * +----------+          +----------+      +---------+
 * |                                         |
 * |                                         |
 * |                                         |
 * |                                         |
 * |                +------------+           |--> +---------+
 * | -------------> | FINISHED   |--------------->| CLOSED  |
 * .                +------------+                +---------+
 */
public enum ScanState {
    /**
     * IO production is ready for fetching.
     */
    READY,
    /**
     * Consumption is blocked because IO progress is running.
     */
    BLOCKED,
    /**
     * IO progress is done, and waiting for resource closed.
     */
    FINISHED,
    /**
     * IO progress is broken because of any exceptions.
     */
    FAILED,
    /**
     * File resources are all closed.
     */
    CLOSED
}
