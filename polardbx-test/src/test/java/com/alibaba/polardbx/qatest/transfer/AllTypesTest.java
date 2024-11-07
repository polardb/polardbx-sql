package com.alibaba.polardbx.qatest.transfer;

import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.transfer.Runner;
import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.plugin.BasePlugin;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.TomlWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.Connection;

public class AllTypesTest {
    private final String path = ConfigConstant.RESOURCE_PATH + "all_types_test.toml";

    @Test
    public void allTypesTest() throws Exception {
        prepareConfig();
        TomlConfig.getInstance().init(path);
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists all_types_test");
            JdbcUtil.executeUpdate(connection, "create database all_types_test mode=auto");
        }
        // Prepare transfer table.
        Utils.prepare();

        // Run transfer test.
        Runner.runAllPlugins();

        if (BasePlugin.success()) {
            System.out.println("All types test success.");
        } else {
            throw new RuntimeException("All types test failed.");
        }
    }

    private void prepareConfig() throws IOException {
        // Delete old file if exists.
        try {
            Files.delete(Paths.get(path));
        } catch (NoSuchFileException ex) {
            // ignore.
        }
        // Create new config file.
        AllTypesConfig config = new AllTypesConfig();
        config.dsn =
            PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_USER)
                + ":"
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD)
                + "@tcp("
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS)
                + ":"
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT)
                + ")/all_types_test";
        config.timeout = PropertiesUtil.transferTestTime * 60;
        config.threads = PropertiesUtil.allTypesTestPrepareThreads;
        config.big_column = PropertiesUtil.allTypesTestBigColumn;
        config.row_count = PropertiesUtil.transferRowCount;
        config.conn_properties = PropertiesUtil.getConnectionProperties();
        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

    private static class AllTypesConfig {
        String dsn;
        String conn_properties;
        String test_type = "all-types-test";
        String runmode = "local";
        long row_count = 10000;
        String create_table_suffix = "PARTITION BY KEY(id) PARTITIONS 16";
        long report_interval = 5;
        long timeout;
        long threads = 16;
        boolean big_column = false;
        WriteOnly write_only = new WriteOnly();
        CheckColumnar check_columnar = new CheckColumnar();
    }

    private static class WriteOnly {
        boolean enabled = true;
        long threads = 5;
    }

    private static class CheckColumnar {
        boolean enabled = true;
        long threads = 1;
    }
}
