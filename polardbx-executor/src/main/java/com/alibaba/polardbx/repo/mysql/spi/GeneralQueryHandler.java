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

package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.SQLException;

/**
 * 集中所有jdbc操作到这个Handler. 这样才能支持同步化和异步化执行
 *
 * @author Whisper 2013-6-20 上午9:32:46
 * @since 3.0.1
 */
public interface GeneralQueryHandler {

    /**
     * 初始化结果集
     */
    public abstract void executeQuery(CursorMeta meta, BaseQueryOperation query) throws SQLException;

    /**
     * 获取结果集
     */
    public abstract Cursor getResultCursor();

    /**
     * 关闭,但不是真关闭
     */
    public abstract void close() throws SQLException;

    /**
     * 指针下移 返回null则表示没有后项了
     */
    public abstract Row next() throws SQLException;

    /**
     * 获取当前row
     */
    public abstract Row getCurrent();

    /**
     * 是否触发过initResultSet()这个方法。 与isDone不同的是这个主要是防止多次提交用。（估计）
     */
    public abstract boolean isInited();

    public abstract int[] executeUpdate(BaseQueryOperation put) throws SQLException;

    /**
     * 用于异步化，等同于Future.isDone();
     */
    public abstract boolean isDone();

}
