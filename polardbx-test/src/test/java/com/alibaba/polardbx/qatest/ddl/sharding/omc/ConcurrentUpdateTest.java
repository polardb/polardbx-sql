package com.alibaba.polardbx.qatest.ddl.sharding.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.ddl.sharding.omc.ConcurrentDMLBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;

public class ConcurrentUpdateTest extends ConcurrentDMLBaseTest {

    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType && !isRDS80);
    }

    @Override
    public boolean usingNewPartDb() {
        return false;
    }

    @Test
    public void modifyWithUpdate1() throws Exception {
        String tableName = "omc_with_update_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set b=%d+1 where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpdate2() throws Exception {
        String tableName = "omc_with_update_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set a=%d-1 where b=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpdate3() throws Exception {
        String tableName = "omc_with_update_3";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set b=default where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpdate4() throws Exception {
        String tableName = "omc_with_update_4";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set b=default(b) where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpdate5() throws Exception {
        String tableName = "omc_with_update_5";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format("update %%s set b=a+1 where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colB == colA + 1;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpdate6() throws Exception {
        String tableName = "omc_with_update_6";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set b=a+1,a=%d,b=a+1,b=b+1 where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colB == colA + 2;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyMultiWithUpdate1() throws Exception {
        String tableName = "omc_multi_with_update_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c text";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set b=%d+1,c=b where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB) && (Float.parseFloat(colC) == colB);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyMultiWithUpdate2() throws Exception {
        String tableName = "omc_multi_with_update_2";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4, drop column d, add column e varchar(10) not null default 'abcdefg'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format("update %%s set b=a+1 where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == colA + 1)
                && (colD.equalsIgnoreCase("abcdefg") || colD.equalsIgnoreCase(colC));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpdate1() throws Exception {
        String tableName = "omc_with_update_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set b=%d+1 where a=%d", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set e=%d+1 where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpdate2() throws Exception {
        String tableName = "omc_with_update_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set a=%d-1 where b=%d", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set a=%d-1 where e=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpdate3() throws Exception {
        String tableName = "omc_with_update_3";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set b=default where a=%d", count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set e=default where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpdate4() throws Exception {
        String tableName = "omc_with_update_4";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set b=default(b) where a=%d", count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set e=default(e) where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpdate5() throws Exception {
        String tableName = "omc_with_update_5";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 = (count) -> String.format("update %%s set b=a+1 where a=%d", count);
        Function<Integer, String> generator2 = (count) -> String.format("update %%s set c=a+1 where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colB == colA + 1;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpdate6() throws Exception {
        String tableName = "omc_with_update_6";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set b=a+1,a=%d,b=a+1,b=b+1 where a=%d", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set e=a+1,a=%d,e=a+1,e=e+1 where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colB == colA + 2;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeMultiWithUpdate1() throws Exception {
        String tableName = "omc_multi_with_update_1";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c d char(10) default 'xxx', change column d c char(10) default 'yyy' after e";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set b=default(b),c=default(c),d=default(d) where a=%d", count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set e=default(e),c=default(c),d=default(d) where a=%d", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("yyy"))
                && (colD.equalsIgnoreCase("abc") || colD.equalsIgnoreCase("xxx"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeMultiWithUpdate2() throws Exception {
        String tableName = "omc_multi_with_update_2";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c d char(10), change column d c char(10)";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("update %%s set b=a+1,a=%d,b=a+1,b=b+1,c=a,d=b where a=%d", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("update %%s set e=a+1,a=%d,e=a+1,e=e+1,c=e,d=a where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == colA + 2) && (Float.parseFloat(colD) == Float.parseFloat(colC) + 2);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpdateNothingChanged() throws Exception {
        String tableName = "omc_with_update_no_change";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("update %%s set b=%d where a=%d", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) ->
            Objects.equals(colA, colB);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }
}
