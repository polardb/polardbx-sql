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
package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.repo.mysql.spi.CursorFactoryMyImpl;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.truth.Truth;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static com.alibaba.polardbx.common.properties.ConnectionParams.OUTPUT_MYSQL_INDENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogicalShowCreateTablesForShardingDatabaseHandlerTest extends LogicalShowCreateTableTest {
    protected static final String CREATE_TABLE_WITH_TAB = "CREATE TABLE `" + TABLE_NAME + "` (\n"
        + "\t`id` varchar(128) NOT NULL,\n"
        + "\t`c1` varchar(50) NOT NULL,\n"
        + "\t`c2` longblob,\n"
        + "\t`c3` longblob,\n"
        + "\tPRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ";

    protected static final String CREATE_TABLE_WITH_SPACE = "CREATE TABLE `" + TABLE_NAME + "` (\n"
        + "  `id` varchar(128) NOT NULL,\n"
        + "  `c1` varchar(50) NOT NULL,\n"
        + "  `c2` longblob,\n"
        + "  `c3` longblob,\n"
        + "  PRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ";

    @Test
    public void showWithMySqlIndentTest() {
        final SqlNodeList showCreateTableNode = new FastsqlParser().parse(SHOW_CREATE_TABLE);

        final ArrayRow mockRow = mock(ArrayRow.class);
        when(mockRow.getString(eq(0))).thenReturn(TABLE_NAME);
        when(mockRow.getString(eq(1))).thenReturn(CREATE_TABLE_WITH_SPACE);
        final ArrayResultCursor physicalResultCursor = mock(ArrayResultCursor.class);
        when(physicalResultCursor.next()).thenReturn(mockRow).thenReturn(null);

        final CursorFactoryMyImpl cursorFactoryMyImpl = mock(CursorFactoryMyImpl.class);
        when(cursorFactoryMyImpl.repoCursor(any(), any())).thenReturn(physicalResultCursor);
        final MyRepository mockRepo = mock(MyRepository.class);
        when(mockRepo.getCursorFactory()).thenReturn(cursorFactoryMyImpl);

        try (final MockedStatic<OptimizerContext> mockOptimizerContextStatic = mockStatic(OptimizerContext.class);
            final MockedConstruction<PhyShow> phyShowMockedConstruction = mockConstruction(PhyShow.class);
            final MockedStatic<ExecutorContext> mockExecutorContextStatic = mockStatic(ExecutorContext.class);
        ) {
            mockMetaSystem(SCHEMA_NAME, TABLE_NAME, mockOptimizerContextStatic, mockExecutorContextStatic);

            final LogicalShowCreateTablesForShardingDatabaseHandler showHandler =
                new LogicalShowCreateTablesForShardingDatabaseHandler(mockRepo);

            final LogicalShow show = mock(LogicalShow.class);
            when(show.getNativeSqlNode()).thenReturn(showCreateTableNode.get(0));

            final CursorMeta cursorMeta = createCursorMeta();
            when(show.getCursorMeta()).thenReturn(cursorMeta);

            final ExecutionContext ec = new ExecutionContext();
            ec.setSchemaName(SCHEMA_NAME);

            Cursor result = showHandler.handle(show, ec);
            Row row = result.next();
            Truth.assertThat(row.getString(0));
            Truth.assertThat(row.getString(1)).isEqualTo(CREATE_TABLE_WITH_TAB);

            // Set output mysql indent
            ParamManager.setBooleanVal(ec.getParamManager().getProps(), OUTPUT_MYSQL_INDENT, true, true);

            when(physicalResultCursor.next()).thenReturn(mockRow).thenReturn(null);
            result = showHandler.handle(show, ec);
            row = result.next();
            Truth.assertThat(row.getString(0));
            Truth.assertThat(row.getString(1)).isEqualTo(CREATE_TABLE_WITH_SPACE);
        }
    }
}
