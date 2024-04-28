package com.alibaba.polardbx.qatest.columnar.gms;

import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public abstract class BaseColumnarManagerTest {
    private static final Log log = LogFactory.getLog(BaseColumnarManagerTest.class);
    protected String caseName;
    protected String schemaName;
    protected List<String> metaDbSqls;
    protected DynamicColumnarManager columnarManager;

    public BaseColumnarManagerTest(String caseName, String schemaName, List<String> metaDbSqls) {
        this.caseName = caseName;
        this.schemaName = schemaName;
        this.metaDbSqls = metaDbSqls;
    }

    @BeforeClass
    public static void initMetaDbDataSources() {
        String addr =
            ConnectionManager.getInstance().getMetaAddress() + ":" + ConnectionManager.getInstance().getMetaPort();
        String dbName = PropertiesUtil.getMetaDB;
        String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
        String usr = ConnectionManager.getInstance().getMetaUser();
        String pwd = PropertiesUtil.configProp.getProperty(ConfigConstant.META_PASSWORD);
        MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);

    }

    protected void clearColumnarMeta() {
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            MetaDbUtil.execute(String.format("delete from `files` where table_schema = '%s'", schemaName), metaConn);
            MetaDbUtil.execute(
                String.format("delete from `columnar_file_mapping` where logical_schema = '%s'", schemaName), metaConn);
            MetaDbUtil.execute(
                String.format("delete from `columnar_checkpoints` where logical_schema = '%s'", schemaName), metaConn);
            MetaDbUtil.execute(
                String.format("delete from `columnar_appended_files` where logical_schema = '%s'", schemaName),
                metaConn);
            MetaDbUtil.execute(String.format("delete from `columnar_file_id_info` where type like '%s/%%'", schemaName),
                metaConn);
//            MetaDbUtil.execute("delete from `columnar_data_consistency_lock`", metaConn);
            MetaDbUtil.execute(String.format("delete from `indexes` where table_schema = '%s'", schemaName), metaConn);
        } catch (Throwable t) {

        }
    }

    @Test
    public void doOfflineTest() throws SQLException, InterruptedException {
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            for (String metaDbSql : metaDbSqls) {
                log.info("Execute sql on metaDB: " + metaDbSql);
                MetaDbUtil.execute(metaDbSql, metaConn);
            }
        }

        Thread columnarManagerThread = new Thread(() -> {
            this.columnarManager = new DynamicColumnarManager();
            this.columnarManager.init();
        });
        columnarManagerThread.start();

        Thread.sleep(15_000);
        columnarManagerThread.join();
        Assert.assertTrue(this.columnarManager.latestTso() > 0);
    }

    @Test
    @Ignore
    public void doInteractiveTest() {

//        ColumnarManager cm = new DynamicColumnarManager(schemaName);
//        Assert.assertTrue(cm.latestTso() > 0);
    }

    public static List<Object[]> loadTestCases(Class clazz) {
        URL url = clazz.getResource(clazz.getSimpleName() + ".class");
        File dir = new File(url.getPath().substring(0, url.getPath().indexOf(clazz.getSimpleName() + ".class")));
        File[] fileList = dir.listFiles();
        List<Object[]> cases = new ArrayList<>();

        for (File file : fileList) {
            if (file.isFile()) {
                if (file.getName().startsWith(clazz.getSimpleName() + ".") && file.getName().endsWith(".yml")) {

                    Yaml yaml = new Yaml();
                    Object o = yaml.load(clazz.getResourceAsStream(file.getName()));

                    if (o instanceof List) {
                        List<Map<String, String>> list = (List<Map<String, String>>) o;
                        for (Map<String, String> map : list) {
                            List<String> sqls = Arrays.asList(map.get("sqls").split("\n"));
//                            List<String> params = Arrays.asList(map.get("params").split("\n"));
//                            Assert.assertTrue(sqls.size() == params.size());
                            cases.add(new Object[] {
                                file.getName(), "test_c", sqls
                            });
                        }
                    }
                }
            }
        }

        return cases;
    }
}
