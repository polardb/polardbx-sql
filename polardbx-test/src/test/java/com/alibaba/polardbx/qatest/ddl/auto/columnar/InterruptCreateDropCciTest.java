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

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.List;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InterruptCreateDropCciTest extends DDLBaseNewDBTestCase {
    private static final String TEST_PRIMARY_NAME_PREFIX = "interrupt_create_drop_cci_primary";
    private static final String TEST_CCI_NAME_PREFIX = "interrupt_create_drop_cci_cci";

    protected static final String CREATE_TABLE =
        "create table %s (c1 int not null primary key, c2 varchar(100), c3 int, c4 int) partition by key(c1) partitions %s";
    protected static final String CREATE_CCI = "CREATE CLUSTERED COLUMNAR INDEX %s on %s(%s)";
    protected static final String CCI_SORT_KEY_C1 = "c1";
    protected static final String CREATE_PREFIX = "Create";
    protected static final String DROP_PREFIX = "Drop";

    private static final int NUM_SHARDS = 3;

    private String primaryFullName;
    private String cciFullName;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        final String suffix = "__" + formatter.format("%04x", random.nextInt(0x10000));
        this.cciFullName = TEST_CCI_NAME_PREFIX + suffix;
        this.primaryFullName = TEST_PRIMARY_NAME_PREFIX + suffix;

        final String sqlCreateTable = String.format(CREATE_TABLE, primaryFullName, NUM_SHARDS);

        // create table
        dropTableIfExists(primaryFullName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable);
    }

    @After
    public void after() {
        dropTableIfExists(primaryFullName);
    }

    @Test
    public void t21_pause_create_then_rollback() throws SQLException, InterruptedException {
        final String sqlCreateCci = String.format(CREATE_CCI, cciFullName, primaryFullName, CCI_SORT_KEY_C1);

        // create cci with new logical connection
        try (final Connection polardbxConn = getPolardbxDirectConnection(getDdlSchema())) {
            createCciAsync(polardbxConn, failOnDdlTaskHint("WaitColumnarTableCreationTask") + sqlCreateCci);
        }

        // check ddl job status
        final JobInfo job = fetchCurrentJobUntil(DdlState.PAUSED, primaryFullName, 180);
        checkJobState(DdlState.PAUSED, primaryFullName);

        // rollback ddl job
        rollbackDDL(job);

        // check rollback success
        checkJobGone(primaryFullName);

        // check cdc mark
        final List<CdcDdlRecord> cdcDdlRecords = queryDdlRecordByJobId(job.parentJob.jobId);
        cdcDdlRecords.sort(Comparator.comparingLong(o -> o.id));
        Truth
            .assertWithMessage("Expected two cdc ddl mark(one for create, one for drop)")
            .that(cdcDdlRecords)
            .hasSize(2);

        final DDLExtInfo createCciExtInfo = JSONObject.parseObject(cdcDdlRecords.get(0).ext, DDLExtInfo.class);
        final DDLExtInfo dropCciExtInfo = JSONObject.parseObject(cdcDdlRecords.get(1).ext, DDLExtInfo.class);

        Truth.assertThat(createCciExtInfo.getOriginalDdl()).ignoringCase().contains("CREATE CLUSTERED COLUMNAR INDEX");
        Truth.assertThat(dropCciExtInfo.getOriginalDdl()).ignoringCase().contains("DROP INDEX");

        checkLatestColumnarSchemaEvolutionRecord(job.parentJob.jobId,
            getDdlSchema(),
            primaryFullName,
            cciFullName,
            DdlType.DROP_INDEX,
            ColumnarTableStatus.DROP);
    }

}
