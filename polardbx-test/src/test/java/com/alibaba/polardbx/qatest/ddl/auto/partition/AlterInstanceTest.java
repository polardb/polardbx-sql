package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by chengjin.
 *
 * @author chengjin
 */

@NotThreadSafe
public class AlterInstanceTest extends DDLBaseNewDBTestCase {

    private static final String alterToReadOnly = "alter instance set read_only=true";
    private static final String setToReadWrite = "set global instance_read_only=false";

    //    @Test
    public void testSetReadOnly() throws SQLException {
        // check global instance_read_only not set
        assertReadOnlyValue(false);
        //test drds db read_only
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterToReadOnly);
        // check global instance_read_only set success
        assertReadOnlyValue(true);
    }

    private void assertReadOnlyValue(boolean value) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("show variables like 'instance_read_only'", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(value + "", rs.getString(2));
    }

    //    @After
    public void afterTest() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, setToReadWrite);
    }
}
