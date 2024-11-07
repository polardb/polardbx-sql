package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.HexBin;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.XPlanUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.rpc.XUtil;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.JsonBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.druid.sql.parser.CharTypes.bytesToHex;
import static com.alibaba.polardbx.druid.sql.parser.CharTypes.hexToBytes;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author fangwu
 */
public class DRDSRelJsonTest extends BaseRuleTest {

    @Test
    public void testXPlanInLogicalView() {
        String expect = "{\n"
            + "  \"rels\": [\n"
            + "    {\n"
            + "      \"id\": \"0\",\n"
            + "      \"relOp\": \"LogicalView\",\n"
            + "      \"table\": [\n"
            + "        \"optest\",\n"
            + "        \"stu\"\n"
            + "      ],\n"
            + "      \"tableNames\": [\n"
            + "        \"stu\"\n"
            + "      ],\n"
            + "      \"pushDownOpt\": {\n"
            + "        \"pushrels\": [\n"
            + "          {\n"
            + "            \"id\": \"0\",\n"
            + "            \"relOp\": \"LogicalTableScan\",\n"
            + "            \"table\": [\n"
            + "              \"optest\",\n"
            + "              \"stu\"\n"
            + "            ],\n"
            + "            \"flashback\": null,\n"
            + "            \"inputs\": []\n"
            + "          },\n"
            + "          {\n"
            + "            \"id\": \"1\",\n"
            + "            \"relOp\": \"LogicalFilter\",\n"
            + "            \"condition\": {\n"
            + "              \"op\": \"SqlBinaryOperator=\",\n"
            + "              \"operands\": [\n"
            + "                {\n"
            + "                  \"input\": 2,\n"
            + "                  \"name\": \"$2\",\n"
            + "                  \"type\": {\n"
            + "                    \"type\": \"TINYINT\",\n"
            + "                    \"nullable\": true,\n"
            + "                    \"precision\": 1\n"
            + "                  }\n"
            + "                },\n"
            + "                {\n"
            + "                  \"input\": 0,\n"
            + "                  \"name\": \"$0\",\n"
            + "                  \"type\": {\n"
            + "                    \"type\": \"INTEGER\",\n"
            + "                    \"nullable\": true\n"
            + "                  }\n"
            + "                }\n"
            + "              ],\n"
            + "              \"type\": {\n"
            + "                \"type\": \"BIGINT\",\n"
            + "                \"nullable\": true\n"
            + "              }\n"
            + "            }\n"
            + "          }\n"
            + "        ]\n"
            + "      },\n"
            + "      \"schemaName\": \"optest\",\n"
            + "      \"partitions\": [],\n"
            + "      \"flashback\": null,\n"
            + "      \"xplan\": {\n"
            + "        \"template\": \"080532710A59080322550A1208021A0E0A0C1204080950001A0408095001120A08084A060A026964102D120C08084A080A046E616D65102D121108084A0D0A096F7065726174696F6E102D121208084A0E0A0A616374696F6E44617465102D1214080532100A023D3D120408095002120408095000\",\n"
            + "        \"schemaNames\": [\n"
            + "          \"optest\"\n"
            + "        ],\n"
            + "        \"tableNames\": [\n"
            + "          \"stu\"\n"
            + "        ],\n"
            + "        \"indexName\": \"PRIMARY\",\n"
            + "        \"paramInfo\": [\n"
            + "          {\n"
            + "            \"type\": \"TableName\",\n"
            + "            \"id\": 0,\n"
            + "            \"isBit\": false,\n"
            + "            \"isNullAble\": false\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"SchemaName\",\n"
            + "            \"id\": 0,\n"
            + "            \"isBit\": false,\n"
            + "            \"isNullAble\": false\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        expect = expect.replaceAll("\n", "").replaceAll("[\t ]", "");
        boolean enableXPlan = XConnectionManager.getInstance().isEnableXplanTableScan();
        try {
            XConnectionManager.getInstance().setEnableXplanTableScan(true);
            LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
                schema.getTableForMember(Arrays.asList("optest", "stu")));
            LogicalView logicalView = LogicalView.create(scan, scan.getTable());
            final RexBuilder rexBuilder = relOptCluster.getRexBuilder();
            LogicalFilter filter = LogicalFilter.create(logicalView,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "operation", true),
                    rexBuilder.makeInputRef(logicalView, 0)));
            logicalView.push(filter);
            PlannerContext.getPlannerContext(logicalView).setParams(new Parameters());
            logicalView.getXPlan();
            final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
            logicalView.explain(writer);
            String s = writer.asString();

            assertThat(s.replaceAll("\n", "").replaceAll("[\t ]", ""), is(expect));

            final DRDSRelJsonReader reader = new DRDSRelJsonReader(relOptCluster, schema, null, false);
            RelNode node;
            try {
                node = reader.read(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            assertTrue(node instanceof LogicalView);
            assertTrue(((LogicalView) node).getXPlan().getTemplate().hasFilter());
        } finally {
            XConnectionManager.getInstance().setEnableXplanTableScan(enableXPlan);
        }
    }

    @Test
    public void testXPlanTemplateWriter() {
        String checkHex =
            "0801125D0A29121208084A0E0A0A746573745F7461626C65102D1A1308084A0F0A0B746573745F736368656D61102D12140A1208084A0E0A0A746573745F696E646578102D1A1A0A180A1008084A0C0A08746573745F6B6579102D120408095000";
        String schema = "test_schema";
        String table = "test_table";
        String index = "test_index";
        String key = "test_key";
        PolarxExecPlan.AnyPlan plan = genXPlan(schema, table, index, key);
        List<SqlIdentifier> tableNames = new ArrayList<>();
        tableNames.add(new SqlIdentifier(ImmutableList.of(schema, table), SqlParserPos.ZERO));
        List<XPlanUtil.ScalarParamInfo> paramInfos = Lists.newArrayList();
        RelDataType bitType = DataTypeUtil.jdbcTypeToRelDataType(-7, "BIT", 1, 0, 1, true);
        RelDataType tinyIntType = DataTypeUtil.jdbcTypeToRelDataType(-7, "TINYINT", 3, 0, 1, true);
        XPlanUtil.ScalarParamInfo paramInfo1 =
            new XPlanUtil.ScalarParamInfo(XPlanUtil.ScalarParamInfo.Type.DynamicParam, 0, bitType,
                true);
        XPlanUtil.ScalarParamInfo paramInfo2 =
            new XPlanUtil.ScalarParamInfo(XPlanUtil.ScalarParamInfo.Type.TableName, 0, tinyIntType,
                true);
        paramInfos.add(paramInfo1);
        paramInfos.add(paramInfo2);

        XPlanTemplate xPlanTemplate = new XPlanTemplate(plan, tableNames, paramInfos, index);

        DRDSRelJson drdsRelJson = new DRDSRelJson(new JsonBuilder(), true);
        Map o = (Map) drdsRelJson.toJson(xPlanTemplate);
        System.out.println(o);

        byte[] template = hexToBytes((String) o.get("template"));
        List<String> tableNamesCheck = (List<String>) o.get("tableNames");
        List<String> schemaNamesCheck = (List<String>) o.get("schemaNames");
        String indexNameCheck = (String) o.get("indexName");
        List<Map> paramInfosCheck = (List<Map>) o.get("paramInfo");

        assertTrue(checkHex.equalsIgnoreCase(HexBin.encode(template)));
        assertTrue(tableNamesCheck.size() == 1 && tableNamesCheck.get(0).equalsIgnoreCase(table));
        assertTrue(schemaNamesCheck.size() == 1 && schemaNamesCheck.get(0).equalsIgnoreCase(schema));
        assertTrue(indexNameCheck.equalsIgnoreCase(index));
        assertTrue(paramInfosCheck.size() == 2);
        assertTrue((paramInfosCheck.get(0).get("type").toString()
            .equalsIgnoreCase(XPlanUtil.ScalarParamInfo.Type.DynamicParam.name())));
        assertTrue(((Integer) (paramInfosCheck.get(0).get("id")) == 0));
        assertTrue((Boolean) (paramInfosCheck.get(0).get("isBit")));
        assertTrue((Boolean) (paramInfosCheck.get(0).get("isNullAble")));

        assertTrue((paramInfosCheck.get(1).get("type").toString()
            .equalsIgnoreCase(XPlanUtil.ScalarParamInfo.Type.TableName.name())));
        assertTrue(((Integer) (paramInfosCheck.get(1).get("id")) == 0));
        assertTrue(!(Boolean) (paramInfosCheck.get(1).get("isBit")));
        assertTrue((Boolean) (paramInfosCheck.get(1).get("isNullAble")));
    }

    public static PolarxExecPlan.AnyPlan genXPlan(String schema, String table, String index, String key) {
        final PolarxExecPlan.TableInfo.Builder tableBuilder = PolarxExecPlan.TableInfo.newBuilder();
        tableBuilder.setSchemaName(XUtil.genUtf8StringScalar(schema));
        tableBuilder.setName(XUtil.genUtf8StringScalar(table));
        final PolarxExecPlan.IndexInfo.Builder indexBuilder = PolarxExecPlan.IndexInfo.newBuilder();
        indexBuilder.setName(XUtil.genUtf8StringScalar(index));
        final PolarxExecPlan.KeyExpr.Builder keyBuilder = PolarxExecPlan.KeyExpr.newBuilder();
        keyBuilder.setField(XUtil.genUtf8StringScalar(key));
        keyBuilder.setValue(XUtil.genPlaceholderScalar(0));
        final PolarxExecPlan.GetExpr.Builder exprBuilder = PolarxExecPlan.GetExpr.newBuilder();
        exprBuilder.addKeys(keyBuilder);
        final PolarxExecPlan.GetPlan.Builder getBuilder = PolarxExecPlan.GetPlan.newBuilder();
        getBuilder.setTableInfo(tableBuilder);
        getBuilder.setIndexInfo(indexBuilder);
        getBuilder.addKeys(exprBuilder);
        final PolarxExecPlan.AnyPlan.Builder anyBuilder = PolarxExecPlan.AnyPlan.newBuilder();
        anyBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.GET);
        anyBuilder.setGetPlan(getBuilder);
        return anyBuilder.build();
//            final PolarxExecPlan.TableProject.Builder projBuilder = PolarxExecPlan.TableProject.newBuilder();
//            projBuilder.setSubReadPlan(anyBuilder);
//            projBuilder.addFields(XUtil.genUtf8StringScalar(target));
//            final PolarxExecPlan.AnyPlan.Builder finalBuilder = PolarxExecPlan.AnyPlan.newBuilder();
//            finalBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.TABLE_PROJECT);
//            finalBuilder.setTableProject(projBuilder);
//            return finalBuilder.build();
    }
}
