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

package com.alibaba.polardbx.qatest.failpoint.recoverable.newpartition;

import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionTestBase;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartitionedTableDdlTestCase extends PartitionTestBase {

    public static final String DEFAULT_PARTITIONING_DEFINITION =
        " partition by key(id) partitions 8";
    protected String host;
    protected String password;
    protected String dbName;
    protected Connection conn;

    protected static List<String> partColInfos = new ArrayList<>();

    protected static List<String> gsiPartColInfos = new ArrayList<>();
    protected static List<String> gsiCoveringColInfos = new ArrayList<>();

    protected static List<PartitionStrategy> partStrategies = new ArrayList<PartitionStrategy>();
    protected static Map<String, List<String>> partColRangeBounds = new HashMap<>();
    protected static Map<String, List<String>> partColListBounds = new HashMap<>();

    protected static String logTblName = "part_tbl_with_gsi";
    protected static String bigintPartCol = "c_bigint_64_un";
    protected static String datetimePartCol = "c_datetime";
    protected static String varcharPartCol = "c_varchar";

    protected static String gsiBigintPartCol = "c_int_32_un";
    protected static String gsiDatetimePartCol = "c_datetime_1";
    protected static String gsiVarcharPartCol = "c_char";

    protected static String NEW_PART_DEF_TEMPLATE = "PARTITION BY %s(%s) %s";

    protected static String HASH_DEF_TEMPLATE = "PARTITIONS %s";
    protected static String NON_HAHSH_DEF_TEMPLATE = "(%s)";

    protected static String PART_NAME_TEMPLATE = "p%s";
    protected static String ONE_RANGE_DEF_TEMPLATE = "PARTITION %s VALUES LESS THAN (%s)";
    protected static String ONE_LIST_DEF_TEMPLATE = "PARTITION %s VALUES IN (%s)";

    protected static String GSI_NAME_TEMPLATE = "gsi_%s";
    protected static String GSI_TEMPLATE = "GLOBAL INDEX %s USING HASH ON %s (%s) COVERING (%s) %s";
    protected static String CREATE_GSI_TEMPLATE = "CREATE %s";
    protected static String DROP_GSI_TEMPLATE = "DROP INDEX %s on %s;";

    /**
     *
     * <pre>
     *
     UNIQUE GLOBAL INDEX gsi_id2 USING HASH ON test_part_tbl (pk, id2) COVERING (vc1, vc2) DBPARTITION BY RANGE_HASH (pk, id2, 4) TBPARTITION BY RANGE_HASH (pk, id2, 3) TBPARTITIONS 3 ,

     UNIQUE GLOBAL INDEX gsi_id2 USING HASH (pk, id2) COVERING (vc1, vc2) DBPARTITION BY RANGE_HASH (pk, id2, 4) TBPARTITION BY RANGE_HASH (pk, id2, 3) TBPARTITIONS 3
     *
     * </pre>
     *
     *
     */

    static {

        // 
        partStrategies.add(PartitionStrategy.HASH);
        partStrategies.add(PartitionStrategy.KEY);
        partStrategies.add(PartitionStrategy.RANGE);
        partStrategies.add(PartitionStrategy.RANGE_COLUMNS);
        partStrategies.add(PartitionStrategy.LIST);
        partStrategies.add(PartitionStrategy.LIST_COLUMNS);

        partColInfos.add(bigintPartCol);
        partColInfos.add(datetimePartCol);
        partColInfos.add(varcharPartCol);

        gsiPartColInfos.add(gsiBigintPartCol);
        gsiPartColInfos.add(gsiDatetimePartCol);
        gsiPartColInfos.add(gsiVarcharPartCol);

        gsiCoveringColInfos.add("c_int_32");
        gsiCoveringColInfos.add("c_text");

        //====== Range =====
        List<String> intRngBounds = new ArrayList<>();
        intRngBounds.add("10000");
        intRngBounds.add("20000");
        intRngBounds.add("30000");
        partColRangeBounds.put(bigintPartCol, intRngBounds);
        partColRangeBounds.put(gsiBigintPartCol, intRngBounds);

        List<String> datetimeRngBounds = new ArrayList<>();
        datetimeRngBounds.add("'2000-01-01 01:00:00'");
        datetimeRngBounds.add("'2010-01-01 01:00:00'");
        datetimeRngBounds.add("'2020-01-01 01:00:00'");
        partColRangeBounds.put(datetimePartCol, datetimeRngBounds);
        partColRangeBounds.put(gsiDatetimePartCol, datetimeRngBounds);

        List<String> varcharRngBounds = new ArrayList<>();
        varcharRngBounds.add("abc");
        varcharRngBounds.add("efg");
        varcharRngBounds.add("hijk");
        partColRangeBounds.put(varcharPartCol, varcharRngBounds);
        partColRangeBounds.put(gsiVarcharPartCol, varcharRngBounds);

        //====== LIST =====
        List<String> intListBounds = new ArrayList<>();
        intListBounds.add("10000");
        intListBounds.add("20000");
        intListBounds.add("30000");
        partColListBounds.put(bigintPartCol, intListBounds);

        List<String> datetimeLstBounds = new ArrayList<>();
        datetimeLstBounds.add("'2000-01-01 01:00:00'");
        datetimeLstBounds.add("'2010-01-01 01:00:00'");
        datetimeLstBounds.add("'2020-01-01 01:00:00'");
        partColListBounds.put(datetimePartCol, datetimeLstBounds);

        List<String> varcharListBounds = new ArrayList<>();
        varcharListBounds.add("abc");
        varcharListBounds.add("efg");
        varcharListBounds.add("hijk");
        partColListBounds.put(varcharPartCol, varcharListBounds);
    }

    public PartitionedTableDdlTestCase() {
    }

    public static String buildGsiDefWithSimpleRangePartDef(String logTblName, String gsiName, String gsiCol) {

        //     protected static String GSI_TEMPLATE = "GLOBAL INDEX %s USING HASH ON %s (%s) COVERING (%s) %s";
        String gsiColsStr = gsiCol;

        String coveringColsStr = "";
        List<String> tmpCoveringColInfos = new ArrayList<>();
        tmpCoveringColInfos.addAll(gsiCoveringColInfos);
        for (int i = 0; i < tmpCoveringColInfos.size(); i++) {
            if (!coveringColsStr.isEmpty()) {
                coveringColsStr += ",";
            }
            coveringColsStr += tmpCoveringColInfos.get(i);
        }

        String gsiPartDef = buildSimpleRangePartDef(gsiCol);
        String gsiDefStr = String.format(GSI_TEMPLATE, gsiName, logTblName, gsiColsStr, coveringColsStr, gsiPartDef);

        return gsiDefStr;
    }

    protected static String buildSimpleRangePartDef(String partCol) {

        String strategy = PartitionStrategy.RANGE_COLUMNS.getStrategyExplainName();

        List<String> rngBndList = partColRangeBounds.get(partCol);
        String allPartBndDef = "";
        for (int i = 0; i < rngBndList.size(); i++) {
            if (!allPartBndDef.isEmpty()) {
                allPartBndDef += ",";
            }

            String bndVal = rngBndList.get(i);
            allPartBndDef += String.format(ONE_RANGE_DEF_TEMPLATE, String.format(PART_NAME_TEMPLATE, i), bndVal);
        }
        String partDefStr = String
            .format(NEW_PART_DEF_TEMPLATE, strategy, partCol, String.format(NON_HAHSH_DEF_TEMPLATE, allPartBndDef));
        return partDefStr;
    }

    @Test
    public void testDdl() {
        buildCreateTblWithGsiSql(logTblName);
    }

    public static String buildCreateGsiSql(String logTblName, String gsiName) {
        String gsiDefStr = buildGsiDefWithSimpleRangePartDef(logTblName, gsiName, gsiPartColInfos.get(0));
        String createIndexSql = String.format(CREATE_GSI_TEMPLATE, gsiDefStr);
        return createIndexSql;
    }

    public static String buildDropGsiSql(String logTblName, String gsiName) {
        String dropGsiStr = String.format(DROP_GSI_TEMPLATE, gsiName, logTblName);
        return dropGsiStr;
    }

    public static String buildCreateTblWithGsiSql(String logTblName) {

        List<String> gsiDefList = new ArrayList<>();
        int gsiCnt = 2;
        for (int i = 0; i < gsiCnt; i++) {
            String gsiDefStr = buildGsiDefWithSimpleRangePartDef(logTblName, String.format(GSI_NAME_TEMPLATE, i),
                gsiPartColInfos.get(i));
            gsiDefList.add(gsiDefStr);
        }
        String primaryTblPartDef = buildSimpleRangePartDef(bigintPartCol);
        String createTblWithGsiSql =
            ExecuteTableSelect.getFullTypeTableDefWithGsi(logTblName, primaryTblPartDef, gsiDefList);
        return createTblWithGsiSql;

    }
}
