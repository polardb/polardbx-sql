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

public class TransferTest {
    private final String path = ConfigConstant.RESOURCE_PATH + "transfer_test.toml";

    @Test
    public void transferTest() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists transfer_test");
            JdbcUtil.executeUpdate(connection, "create database transfer_test mode=auto");
        }
        prepareConfig();
        TomlConfig.getInstance().init(path);

        // Prepare transfer table.
        Utils.prepare();

        // Run transfer test.
        Runner.runAllPlugins();

        if (BasePlugin.success()) {
            System.out.println("Transfer test success.");
        } else {
            throw new RuntimeException("Transfer test failed.");
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
        TransferConfig config = new TransferConfig();
        config.dsn =
            PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_USER)
                + ":"
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD)
                + "@tcp("
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS)
                + ":"
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT)
                + ")/transfer_test";
        config.timeout = PropertiesUtil.transferTestTime * 60;
        config.replica_read.replica_dsn = config.dsn;
        config.replica_flashback_query.replica_dsn = config.dsn;
        config.row_count = PropertiesUtil.transferRowCount;
        config.conn_properties = PropertiesUtil.getConnectionProperties();
        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

    private static class TransferConfig {
        String dsn;
        String conn_properties;
        String runmode = "local";
        long row_count = 100;
        long initial_balance = 1000;
        String create_table_suffix = "PARTITION BY KEY(id) PARTITIONS 16";
        long report_interval = 5;
        long timeout;
        TransferSimple transfer_simple = new TransferSimple();
        CheckBalance check_balance = new CheckBalance();
        ReplicaRead replica_read = new ReplicaRead();
        FlashbackQuery flashback_query = new FlashbackQuery();
        ReplicaFlashbackQuery replica_flashback_query = new ReplicaFlashbackQuery();
        CheckCdc check_cdc = new CheckCdc();
    }

    private static class TransferSimple {
        boolean enabled = true;
        long threads = 5;
        boolean inject_commit_failure = false;
        double inject_commit_failure_prob = 0.1;
    }

    private static class CheckBalance {
        boolean enabled = true;
        long threads = 2;
        String before_check_stmt = "set transaction_policy = TSO";
    }

    private static class ReplicaRead {
        boolean enabled = false;
        long threads = 2;
        String replica_read_hint = "/*+TDDL:SLAVE()*/";
        String replica_dsn;
        String session_var = "";
        boolean replica_strong_consistency = true;
    }

    private static class FlashbackQuery {
        boolean enabled = false;
        long threads = 2;
        long min_seconds = 10;
        long max_seconds = 20;
    }

    private static class ReplicaFlashbackQuery {
        boolean enabled = false;
        long threads = 2;
        long min_seconds = 10;
        long max_seconds = 20;
        String replica_read_hint = "/*+TDDL:SLAVE()*/";
        String replica_dsn;
    }

    private static class CheckCdc {
        boolean enabled = false;
        long threads = 2;
        String replica_dsn;
    }
}
