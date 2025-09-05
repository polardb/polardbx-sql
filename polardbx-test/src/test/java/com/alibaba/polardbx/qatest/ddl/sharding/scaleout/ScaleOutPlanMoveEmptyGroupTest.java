package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

/**
 * @author luoyanxin
 */
@NotThreadSafe
public class ScaleOutPlanMoveEmptyGroupTest extends ScaleOutBaseTest {

    private static List<String> moveTableStatus =
        Stream.of(
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.toString()).collect(Collectors.toList());
    static boolean firstIn = true;

    public ScaleOutPlanMoveEmptyGroupTest() {
        super("ScaleOutPlanMoveEmptyGroupTest", "polardbx_meta_db_polardbx",
            moveTableStatus);
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUp(true, getTableDefinitions(), true);
            firstIn = false;
        }
        String tddlSql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testReload() {
        JdbcUtil.useDb(tddlConnection, logicalDatabase);
        String sql = "reload datasources";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}