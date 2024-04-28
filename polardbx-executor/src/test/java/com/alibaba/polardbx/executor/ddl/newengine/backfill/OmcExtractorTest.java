package com.alibaba.polardbx.executor.ddl.newengine.backfill;

import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class OmcExtractorTest {

    @Test
    public void testBuildInsertSelectForOMCBackfillWithChangeSetON() {
        ExecutionContext ec = mock(ExecutionContext.class);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder("wumu", true, ec);

        List<String> targetTableColumns = new ArrayList<>();
        targetTableColumns.add("a");
        targetTableColumns.add("b");
        targetTableColumns.add("c");

        List<String> sourceTableColumns = new ArrayList<>();
        sourceTableColumns.add("a");
        sourceTableColumns.add("b");
        sourceTableColumns.add("e");

        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add("a");
        primaryKeys.add("b");

        SqlInsert sqlInsert = builder.buildSqlInsertSelectForOMCBackfill(
            targetTableColumns, sourceTableColumns, primaryKeys, true, true, SqlSelect.LockMode.SHARED_LOCK, true);

        Assert.assertEquals(sqlInsert.toString(), "INSERT IGNORE\n"
            + "INTO ? (`a`, `b`, `c`)\n"
            + "(SELECT `a`, `b`, `e`\n"
            + "FROM ? AS `tb` FORCE INDEX(PRIMARY)\n"
            + "WHERE (((`a` > ?) OR ((`a` = ?) AND (`b` > ?))) AND ((`a` < ?) OR ((`a` = ?) AND (`b` <= ?)))) LOCK IN SHARE MODE)");
    }

    @Test
    public void testBuildInsertSelectForOMCBackfillWithChangeSetOFF() {
        ExecutionContext ec = mock(ExecutionContext.class);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder("wumu", true, ec);

        List<String> targetTableColumns = new ArrayList<>();
        targetTableColumns.add("a");
        targetTableColumns.add("b");
        targetTableColumns.add("c");

        List<String> sourceTableColumns = new ArrayList<>();
        sourceTableColumns.add("a");
        sourceTableColumns.add("b");
        sourceTableColumns.add("e");

        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add("a");
        primaryKeys.add("b");

        SqlInsert sqlInsert = builder.buildSqlInsertSelectForOMCBackfill(
            targetTableColumns, sourceTableColumns, primaryKeys, true, true, SqlSelect.LockMode.UNDEF, false);

        Assert.assertEquals(sqlInsert.toString(), "INSERT\n"
            + "INTO ? (`a`, `b`, `c`)\n"
            + "(SELECT `a`, `b`, `e`\n"
            + "FROM ? AS `tb` FORCE INDEX(PRIMARY)\n"
            + "WHERE (((`a` > ?) OR ((`a` = ?) AND (`b` > ?))) AND ((`a` < ?) OR ((`a` = ?) AND (`b` <= ?)))))");
    }
}
