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

package com.alibaba.polardbx.gms.metadb.table;

public enum ColumnarTableStatus {
    NONE,
    /**
     * 待创建
     */
    CREATING,
    /**
     * 校验中
     */
    CHECKING,
    /**
     * 正常状态
     */
    PUBLIC,
    /**
     * 出错了
     */
    ERROR,
    /**
     * 隐藏
     */
    ABSENT,
    /**
     * cn标记了删除，等待列存同步，列存收到后会将DROP 改为PURGE状态
     */
    DROP,
    /**
     * 等待后台线程进行purge，真正删除文件。
     */
    PURGE;

    public static ColumnarTableStatus from(String value) {
        switch (value.toLowerCase()) {
        case "node":
            return NONE;
        case "creating":
            return CREATING;
        case "checking":
            return CHECKING;
        case "public":
            return PUBLIC;
        case "error":
            return ERROR;
        case "absent":
            return ABSENT;
        case "drop":
            return DROP;
        case "purge":
            return PURGE;
        default:
            throw new IllegalArgumentException("Illegal ColumnarTableStatus: " + value);
        }
    }

    public IndexStatus toIndexStatus() {
        switch (this) {
        case CREATING:
            return IndexStatus.CREATING;
        case CHECKING:
            return IndexStatus.WRITE_REORG;
        case PUBLIC:
            return IndexStatus.PUBLIC;
        case DROP:
        case ABSENT:
        case PURGE:
            return IndexStatus.ABSENT;
        case NONE:
        case ERROR:
        default:
            throw new IllegalArgumentException(String.format("Cannot convert %s to IndexStatus", this.name()));
        }
    }
}
