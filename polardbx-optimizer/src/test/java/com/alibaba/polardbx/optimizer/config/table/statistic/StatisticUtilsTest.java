package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.sun.tools.javac.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class StatisticUtilsTest extends BasePlannerTest {
    private static final String SCHEMA_NAME = "StatisticUtilsTest";

    public static String[] ddls = {
        "CREATE TABLE Users (\n"
            + "    id INT AUTO_INCREMENT PRIMARY KEY,\n"
            + "    username VARCHAR(50) NOT NULL,\n"
            + "    email VARCHAR(100),\n"
            + "    UNIQUE KEY uk_email (email)\n"
            + ")",
        "CREATE TABLE Enrollments (\n"
            + "    enrollment_id INT AUTO_INCREMENT PRIMARY KEY,\n"
            + "    course_id INT NOT NULL,\n"
            + "    student_id INT NOT NULL,\n"
            + "    enroll_date DATE,\n"
            + "    UNIQUE KEY uk_course_student (course_id, student_id)\n"
            + ");\n",
        "CREATE TABLE Orders (\n"
            + "    order_id INT AUTO_INCREMENT PRIMARY KEY,\n"
            + "    product_code VARCHAR(50),\n"
            + "    customer_id INT,\n"
            + "    order_date DATE,\n"
            + "    UNIQUE KEY uk_product_customer (product_code, customer_id),\n"
            + "    local UNIQUE KEY l_uk_customer (customer_id)\n"
            + ")partition by hash(customer_id)\n"
            + "partitions 8;",
    };

    protected RelOptCluster relOptCluster;
    protected RelOptSchema schema;

    public StatisticUtilsTest() {
        super(SCHEMA_NAME);
    }

    @Before
    public void init() {
        useNewPartDb = true;
        relOptCluster = SqlConverter.getInstance(appName, new ExecutionContext()).createRelOptCluster();
        schema = SqlConverter.getInstance(appName, new ExecutionContext()).getCatalog();
        DbInfoManager.getInstance().addNewMockPartitionDb(SCHEMA_NAME);
        try {
            for (String ddl : ddls) {
                buildTable(SCHEMA_NAME, ddl);
            }
        } catch (SQLSyntaxErrorException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试用例: 当表元数据不存在时返回false。
     */
    @Test
    public void testDoesColumnNameMatchAnyUniqueKeyNoTableMetaData() {
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTable(anyString())).thenReturn(null);
        try (MockedStatic<OptimizerContext> mockedStatic = mockStatic(OptimizerContext.class)) {
            mockedStatic.when(() -> OptimizerContext.getContext(anyString())).thenReturn(optimizerContext);

            boolean result = StatisticUtils.doesColumnNameMatchAnyUniqueKey("schema", "logicalTableName", "columnName");

            assertFalse(result);
        }
    }

    /**
     * 测试用例: 表元数据存在但没有匹配的唯一键。
     */
    @Test
    public void testDoesColumnNameMatchAnyUniqueKeyNoMatchingUniqueKey() {
        final Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = buildGisBeanForTest("columnName1");

        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTable(anyString())).thenReturn(null);
        try (MockedStatic<OptimizerContext> mockedStatic = mockStatic(OptimizerContext.class)) {
            mockedStatic.when(() -> OptimizerContext.getContext(anyString())).thenReturn(optimizerContext);

            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable(anyString())).thenReturn(tableMeta);
            when(tableMeta.getGsiPublished()).thenReturn(gsiPublished);

            boolean result = StatisticUtils.doesColumnNameMatchAnyUniqueKey("schema", "logicalTableName", "columnName");
            assertFalse(result);
        }

    }

    @Test
    public void testDoesColumnNameMatchAnyUniqueKeyMatchingUniqueKey() {

        try (MockedStatic<OptimizerContext> mockedStatic = mockStatic(OptimizerContext.class)) {
            mockedStatic.when(() -> OptimizerContext.getContext(SCHEMA_NAME))
                .thenReturn(getContextByAppName(SCHEMA_NAME));

            boolean result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Users", "id,userName,email");
            assertTrue(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Enrollments", "course_id,student_id");
            assertFalse(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Orders", "product_code, customer_id");
            assertTrue(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Orders", " customer_id");
            assertTrue(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Orders", "");
            assertFalse(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Orders", null);
            assertFalse(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Orders", "product_code,order_date");
            assertFalse(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Users", "username,email");
            assertFalse(result);

            result = StatisticUtils.doesColumnNameMatchAnyUniqueKey(SCHEMA_NAME, "Enrollments",
                "course_id,enroll_date,columnName2");
            assertFalse(result);
        }
    }

    @Test
    public void testAreColumnsUniqueInTable() {
        assertFalse(StatisticUtils.areColumnsUniqueInTable(null, "logicalTableName", new String[] {"columnName"}));
        assertFalse(StatisticUtils.areColumnsUniqueInTable(SCHEMA_NAME, "", new String[] {"columnName"}));
        assertFalse(StatisticUtils.areColumnsUniqueInTable(SCHEMA_NAME, "logicalTableName", null));

        try (MockedStatic<OptimizerContext> mockedStatic = mockStatic(OptimizerContext.class)) {
            mockedStatic.when(() -> OptimizerContext.getContext(SCHEMA_NAME))
                .thenReturn(getContextByAppName(SCHEMA_NAME));

            assertFalse(
                StatisticUtils.areColumnsUniqueInTable(SCHEMA_NAME, "no_exist_table", new String[] {"columnName"}));
        }
    }

    @NotNull
    private Map<String, GsiMetaManager.GsiIndexMetaBean> buildGisBeanForTest(String columnName) {
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = new HashMap<>();

        GsiMetaManager.GsiIndexColumnMetaBean columnMetaBean =
            new GsiMetaManager.GsiIndexColumnMetaBean(0, columnName, null, 0, null, null, null, false);
        GsiMetaManager.GsiIndexMetaBean index1 =
            new GsiMetaManager.GsiIndexMetaBean(null, "schema", "logicalTableName", false, "schemaName", "index1",
                List.of(columnMetaBean), null, null, null, null, null, null,
                IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE, null);
        gsiPublished.put("index1", index1);
        return gsiPublished;
    }

    private Map<String, GsiMetaManager.GsiIndexMetaBean> buildGisBeanForTest2(String columnName1, String columnName2) {
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = new HashMap<>();

        GsiMetaManager.GsiIndexColumnMetaBean columnMetaBean1 =
            new GsiMetaManager.GsiIndexColumnMetaBean(0, columnName1, null, 0, null, null, null, false);
        GsiMetaManager.GsiIndexColumnMetaBean columnMetaBean2 =
            new GsiMetaManager.GsiIndexColumnMetaBean(0, columnName2, null, 0, null, null, null, false);

        GsiMetaManager.GsiIndexMetaBean index1 =
            new GsiMetaManager.GsiIndexMetaBean(null, "schema", "logicalTableName", false, "schemaName", "index1",
                List.of(columnMetaBean1, columnMetaBean2), null, null, null, null, null, null,
                IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE, null);
        gsiPublished.put("index1", index1);
        return gsiPublished;
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}
