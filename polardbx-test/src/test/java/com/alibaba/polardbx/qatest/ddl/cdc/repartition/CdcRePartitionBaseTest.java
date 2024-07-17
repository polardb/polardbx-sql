package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.alibaba.polardbx.qatest.ddl.cdc.CdcBaseTest;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.ddl.cdc.util.CdcTestUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-13 10:16
 **/
public class CdcRePartitionBaseTest extends CdcBaseTest {

    // ====================== create subpartition with template sql ====================

    protected static final String CREATE_HASH_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ") \n"
        + "partition by hash (c,d) partitions 4\n"
        + "subpartition by hash (a,b) subpartitions 4;";

    protected static final String CREATE_HASH_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (c,d) partitions 4\n"
        + "subpartition by key (a,b) subpartitions 4;";

    protected static final String CREATE_HASH_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 4\n"
        + "subpartition by range (to_days(c)) (\n"
        + "  subpartition sp1 values less than ( to_days('2020-01-01') ),\n"
        + "  subpartition sp2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_HASH_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 4\n"
        + "subpartition by range columns (c,d) (\n"
        + "subpartition sp1 values less than ( '2020-01-01', 'abc' ),\n"
        + "subpartition sp2 values less than ( '2021-01-01', 'abc' )\n"
        + ");";

    protected static final String CREATE_HASH_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 4\n"
        + "subpartition by list (to_days(c)) (\n"
        + "subpartition sp1 values in ( to_days('2020-01-01'),to_days('2020-01-02') ),\n"
        + "subpartition sp2 values in ( to_days('2020-02-01'),to_days('2020-02-02') )\n"
        + ");";

    protected static final String CREATE_HASH_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 4\n"
        + "subpartition by list columns (c,d) (\n"
        + "subpartition sp1 values in ( ('2020-01-01','abc'),('2020-01-02','abc') ),\n"
        + "subpartition sp2 values in ( ('2020-02-01','abc'),('2020-02-02','abc') )\n"
        + ");";

    protected static final String CREATE_KEY_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (c,d) partitions 4\n"
        + "subpartition by hash (a,b) subpartitions 4;";

    protected static final String CREATE_KEY_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (c,d) partitions 4\n"
        + "subpartition by key (a,b) subpartitions 4;";

    protected static final String CREATE_KEY_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 4\n"
        + "subpartition by range (to_days(c)) (\n"
        + "subpartition sp1 values less than ( to_days('2020-01-01') ),\n"
        + "subpartition sp2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_KEY_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 4\n"
        + "subpartition by range columns (c,d) (\n"
        + "subpartition sp1 values less than ( '2020-01-01', 'abc' ),\n"
        + "subpartition sp2 values less than ( '2021-01-01', 'abc' )\n"
        + ");";

    protected static final String CREATE_KEY_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 4\n"
        + "subpartition by list (to_days(c)) (\n"
        + "subpartition sp1 values in ( to_days('2020-01-01'),to_days('2020-01-02') ),\n"
        + "subpartition sp2 values in ( to_days('2020-02-01'),to_days('2020-02-02') )\n"
        + ");";

    protected static final String CREATE_KEY_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 4\n"
        + "subpartition by list columns (c,d) (\n"
        + "subpartition sp1 values in ( ('2020-01-01','abc'),('2020-01-02','abc') ),\n"
        + "subpartition sp2 values in ( ('2020-02-01','abc'),('2020-02-02','abc') )\n"
        + ");";

    protected static final String CREATE_RANGE_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by hash (a,b) subpartitions 4\n"
        + "(\n"
        + " partition p1 values less than ( to_days('2020-01-01') ),\n"
        + " partition p2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_RANGE_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by key (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ),\n"
        + "partition p2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_RANGE_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "  subpartition sp1 values less than ( 1000),\n"
        + "  subpartition sp2 values less than ( 2000)\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ),\n"
        + "partition p2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_RANGE_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by range columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000, 10000),\n"
        + "subpartition sp2 values less than ( maxvalue, maxvalue )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ),\n"
        + "partition p2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_RANGE_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "subpartition sp1 values in ( 1000, 2000),\n"
        + "subpartition sp2 values in ( default )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ),\n"
        + "partition p2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_RANGE_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values in ( (1000, 2000) ),\n"
        + "subpartition sp2 values in ( default )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ),\n"
        + "partition p2 values less than ( to_days('2021-01-01') )\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by hash (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ),\n"
        + "partition p2 values less than ( '2021-01-01','abc' )\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by key (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ),\n"
        + "partition p2 values less than ( '2021-01-01','abc' )\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000),\n"
        + "subpartition sp2 values less than ( 2000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ),\n"
        + "partition p2 values less than ( '2021-01-01','abc' )\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL =
        "create table if not exists `%s` (\n"
            + "`a` bigint unsigned not null,\n"
            + "`b` bigint unsigned not null,\n"
            + "`c` datetime NOT NULL,\n"
            + "`d` varchar(16) NOT NULL,\n"
            + "`e` varchar(16) NOT NULL\n"
            + ")\n"
            + "partition by range columns (c,d)\n"
            + "subpartition by range columns (a,b)\n"
            + "(\n"
            + "subpartition sp1 values less than ( 1000, 2000),\n"
            + "subpartition sp2 values less than ( 3000, 4000 )\n"
            + ")\n"
            + "(\n"
            + "partition p1 values less than ( '2020-01-01','abc' ),\n"
            + "partition p2 values less than ( '2021-01-01','abc' )\n"
            + ");";

    protected static final String CREATE_RANGE_COLUMNS_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "subpartition sp1 values in ( 1000, 2000),\n"
        + "subpartition sp2 values in ( 3000, 4000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ),\n"
        + "partition p2 values less than ( '2021-01-01','abc' )\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values in ( (1000, 2000)),\n"
        + "subpartition sp2 values in ( (3000, 4000) )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ),\n"
        + "partition p2 values less than ( '2021-01-01','abc' )\n"
        + ");";

    protected static final String CREATE_LIST_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by hash (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') )\n"
        + ");";

    protected static final String CREATE_LIST_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by key (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') )\n"
        + ");";

    protected static final String CREATE_LIST_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000),\n"
        + "subpartition sp2 values less than ( 2000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') )\n"
        + ");";

    protected static final String CREATE_LIST_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by range columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000, 10000),\n"
        + "subpartition sp2 values less than ( 2000, 20000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') )\n"
        + ");";

    protected static final String CREATE_LIST_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "subpartition sp1 values in ( 1000, 2000),\n"
        + "subpartition sp2 values in ( 3000, 4000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') )\n"
        + ");";

    protected static final String CREATE_LIST_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values in ( (1000, 2000) ),\n"
        + "subpartition sp2 values in ( default )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') )\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by hash (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') )\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by key (a,b) subpartitions 4\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') )\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000),\n"
        + "subpartition sp2 values less than ( 2000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') )\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by range columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values less than ( 1000, 10000),\n"
        + "subpartition sp2 values less than ( 2000, 20000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') )\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "subpartition sp1 values in ( 1000, 2000),\n"
        + "subpartition sp2 values in ( 3000, 4000 )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') )\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "subpartition sp1 values in ( (1000, 2000) ),\n"
        + "subpartition sp2 values in ( (3000, 4000) )\n"
        + ")\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') )\n"
        + ");";

    // ====================== create subpartition without template sql ====================

    protected static final String CREATE_HASH_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (c,d) partitions 2\n"
        + "subpartition by hash (a,b)\n"
        + "(\n"
        + "  partition p1 subpartitions 2,\n"
        + "  partition p2 subpartitions 4\n"
        + ");";

    protected static final String CREATE_HASH_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (c,d) partitions 2\n"
        + "subpartition by key (a,b)\n"
        + "(\n"
        + "partition p1 subpartitions 2,\n"
        + "partition p2 subpartitions 4\n"
        + ");";

    protected static final String CREATE_HASH_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 2\n"
        + "subpartition by range (to_days(c))\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values less than ( to_days('2020-01-01') ),\n"
        + "subpartition p1sp2 values less than ( to_days('2021-01-01') )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values less than ( to_days('2020-01-01') ),\n"
        + "subpartition p2sp2 values less than ( to_days('2021-01-01') ),\n"
        + "subpartition p2sp3 values less than ( to_days('2022-01-01') )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_HASH_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 2\n"
        + "subpartition by range columns (c,d)\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values less than ( '2020-01-01', 'abc' ),\n"
        + "subpartition p1sp2 values less than ( '2021-01-01', 'abc' )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values less than ( '2020-01-01', 'abc' ),\n"
        + "subpartition p2sp2 values less than ( '2021-01-01', 'abc' ),\n"
        + "subpartition p2sp3 values less than ( '2022-01-01', 'abc' )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_HASH_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 2\n"
        + "subpartition by list (to_days(c))\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values in ( to_days('2020-01-01') ),\n"
        + "subpartition p1sp2 values in ( to_days('2021-01-01') )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values in ( to_days('2020-01-01') ),\n"
        + "subpartition p2sp2 values in ( to_days('2021-01-01') ),\n"
        + "subpartition p2sp3 values in ( to_days('2022-01-01') )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_HASH_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (a,b) partitions 2\n"
        + "subpartition by list columns (c,d)\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values in ( ('2020-01-01', 'abc') ),\n"
        + "subpartition p1sp2 values in ( ('2021-01-01', 'abc') )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values in ( ('2020-01-01', 'abc') ),\n"
        + "subpartition p2sp2 values in ( ('2021-01-01', 'abc') ),\n"
        + "subpartition p2sp3 values in ( ('2022-01-01', 'abc') )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_KEY_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (c,d) partitions 2\n"
        + "subpartition by hash (a,b)\n"
        + "(\n"
        + "partition p1 subpartitions 2,\n"
        + "partition p2 subpartitions 4\n"
        + ");";

    protected static final String CREATE_KEY_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (c,d) partitions 2\n"
        + "subpartition by key (a,b)\n"
        + "(\n"
        + "partition p1 subpartitions 2,\n"
        + "partition p2 subpartitions 4\n"
        + ");";

    protected static final String CREATE_KEY_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 2\n"
        + "subpartition by range (to_days(c))\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values less than ( to_days('2020-01-01') ),\n"
        + "subpartition p1sp2 values less than ( to_days('2021-01-01') )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values less than ( to_days('2020-01-01') ),\n"
        + "subpartition p2sp2 values less than ( to_days('2021-01-01') ),\n"
        + "subpartition p2sp3 values less than ( to_days('2022-01-01') )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_KEY_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 2\n"
        + "subpartition by range columns (c,d)\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values less than ( '2020-01-01', 'abc' ),\n"
        + "subpartition p1sp2 values less than ( '2021-01-01', 'abc' )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values less than ( '2020-01-01', 'abc' ),\n"
        + "subpartition p2sp2 values less than ( '2021-01-01', 'abc' ),\n"
        + "subpartition p2sp3 values less than ( '2022-01-01', 'abc' )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_KEY_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 2\n"
        + "subpartition by list (to_days(c))\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values in ( to_days('2020-01-01') ),\n"
        + "subpartition p1sp2 values in ( to_days('2021-01-01') )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values in ( to_days('2020-01-01') ),\n"
        + "subpartition p2sp2 values in ( to_days('2021-01-01') ),\n"
        + "subpartition p2sp3 values in ( to_days('2022-01-01') )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_KEY_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (a,b) partitions 2\n"
        + "subpartition by list columns (c,d)\n"
        + "(\n"
        + "partition p1\n"
        + "(\n"
        + "subpartition p1sp1 values in ( ('2020-01-01', 'abc') ),\n"
        + "subpartition p1sp2 values in ( ('2021-01-01', 'abc') )\n"
        + "),\n"
        + "partition p2\n"
        + "(\n"
        + "subpartition p2sp1 values in ( ('2020-01-01', 'abc') ),\n"
        + "subpartition p2sp2 values in ( ('2021-01-01', 'abc') ),\n"
        + "subpartition p2sp3 values in ( ('2022-01-01', 'abc') )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by hash (a,b)\n"
        + "(\n"
        + " partition p1 values less than ( to_days('2020-01-01') ) subpartitions 2,\n"
        + " partition p2 values less than ( to_days('2021-01-01') ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_RANGE_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by key (a,b)\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ) subpartitions 2,\n"
        + "partition p2 values less than ( to_days('2021-01-01') ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_RANGE_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') )\n"
        + "(\n"
        + "subpartition p1sp1 values less than ( 1000 ),\n"
        + "subpartition p1sp2 values less than ( 2000 )\n"
        + "),\n"
        + "partition p2 values less than ( to_days('2021-01-01') )\n"
        + "(\n"
        + "subpartition p2sp1 values less than ( 1000 ),\n"
        + "subpartition p2sp2 values less than ( 2000 ),\n"
        + "subpartition p2sp3 values less than ( 3000 )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by range columns (a,b)\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ) (\n"
        + "subpartition p1sp1 values less than ( 1000, 10000),\n"
        + "subpartition p1sp2 values less than ( 2000, 20000)\n"
        + "),\n"
        + "partition p2 values less than ( to_days('2021-01-01') ) (\n"
        + "subpartition p2sp1 values less than ( 1000, 10000),\n"
        + "subpartition p2sp2 values less than ( 2000, 10000),\n"
        + "subpartition p2sp3 values less than ( 3000, 30000)\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ) (\n"
        + "subpartition p1sp1 values in ( 1000, 2000),\n"
        + "subpartition p1sp2 values in ( 3000, 4000 )\n"
        + "),\n"
        + "partition p2 values less than ( to_days('2021-01-01') ) (\n"
        + "subpartition p2sp1 values in ( 1000, 2000),\n"
        + "subpartition p2sp2 values in ( 3000, 4000),\n"
        + "subpartition p2sp3 values in ( 5000, 6000 )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range (to_days(c))\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "partition p1 values less than ( to_days('2020-01-01') ) (\n"
        + "subpartition p1sp1 values in ( (1000, 2000) ),\n"
        + "subpartition p1sp2 values in ( (3000, 4000) )\n"
        + "),\n"
        + "partition p2 values less than ( to_days('2021-01-01') ) (\n"
        + "subpartition p2sp1 values in ( (1000, 2000) ),\n"
        + "subpartition p2sp2 values in ( (3000, 4000) ),\n"
        + "subpartition p2sp3 values in ( (5000, 6000) )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by hash (a,b)\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ) subpartitions 2,\n"
        + "partition p2 values less than ( '2021-01-01','abc' ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by key (a,b)\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ) subpartitions 2,\n"
        + "partition p2 values less than ( '2021-01-01','abc' ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ) (\n"
        + "subpartition p1sp1 values less than ( 1000),\n"
        + "subpartition p1sp2 values less than ( maxvalue )\n"
        + "),\n"
        + "partition p2 values less than ( '2021-01-01','abc' ) (\n"
        + "subpartition p2sp1 values less than ( 1000),\n"
        + "subpartition p2sp2 values less than ( 2000),\n"
        + "subpartition p2sp3 values less than ( maxvalue )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL =
        "create table if not exists `%s` (\n"
            + "a bigint unsigned not null,\n"
            + "b bigint unsigned not null,\n"
            + "c datetime NOT NULL,\n"
            + "d varchar(16) NOT NULL,\n"
            + "e varchar(16) NOT NULL\n"
            + ")\n"
            + "partition by range columns (c,d)\n"
            + "subpartition by range columns (a,b)\n"
            + "(\n"
            + "partition p1 values less than ( '2020-01-01','abc' ) (\n"
            + "subpartition p1sp1 values less than ( 1000, 2000),\n"
            + "subpartition p1sp2 values less than ( maxvalue, maxvalue )\n"
            + "),\n"
            + "partition p2 values less than ( '2021-01-01','abc' ) (\n"
            + "subpartition p2sp1 values less than ( 1000, 2000),\n"
            + "subpartition p2sp2 values less than ( 2000, 2000),\n"
            + "subpartition p2sp3 values less than ( maxvalue, maxvalue )\n"
            + ")\n"
            + ");";

    protected static final String CREATE_RANGE_COLUMNS_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by range columns (c,d)\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "partition p1 values less than ( '2020-01-01','abc' ) (\n"
        + "subpartition p1sp1 values in ( 1000, 2000),\n"
        + "subpartition p1sp2 values in ( default )\n"
        + "),\n"
        + "partition p2 values less than ( '2021-01-01','abc' ) (\n"
        + "subpartition p2sp1 values in ( 1000, 2000),\n"
        + "subpartition p2sp2 values in ( 3000, 4000),\n"
        + "subpartition p2sp3 values in ( default )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_RANGE_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL =
        "create table if not exists `%s` (\n"
            + "a bigint unsigned not null,\n"
            + "b bigint unsigned not null,\n"
            + "c datetime NOT NULL,\n"
            + "d varchar(16) NOT NULL,\n"
            + "e varchar(16) NOT NULL\n"
            + ")\n"
            + "partition by range columns (c,d)\n"
            + "subpartition by list columns (a,b)\n"
            + "(\n"
            + "partition p1 values less than ( '2020-01-01','abc' ) (\n"
            + "subpartition p1sp1 values in ( (1000, 2000)),\n"
            + "subpartition p1sp2 values in ( default )\n"
            + "),\n"
            + "partition p2 values less than ( '2021-01-01','abc' ) (\n"
            + "subpartition p2sp1 values in ( (1000, 2000)),\n"
            + "subpartition p2sp2 values in ( (2000, 2000)),\n"
            + "subpartition p2sp3 values in ( default )\n"
            + ")\n"
            + ");";

    protected static final String CREATE_LIST_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by hash (a,b)\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ) subpartitions 2,\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_LIST_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by key (a,b)\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ) subpartitions 2,\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_LIST_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ) (\n"
        + "subpartition p1sp1 values less than ( 1000),\n"
        + "subpartition p1sp2 values less than ( maxvalue )\n"
        + "),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') ) (\n"
        + "subpartition p2sp1 values less than ( 1000),\n"
        + "subpartition p2sp2 values less than ( 2000),\n"
        + "subpartition p2sp3 values less than ( maxvalue )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_LIST_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by range columns (a,b)\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ) (\n"
        + "subpartition p1sp1 values less than ( 1000, 10000),\n"
        + "subpartition p1sp2 values less than ( maxvalue, maxvalue )\n"
        + "),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') ) (\n"
        + "subpartition p2sp1 values less than ( 1000, 10000),\n"
        + "subpartition p2sp2 values less than ( 2000, 10000),\n"
        + "subpartition p2sp3 values less than ( maxvalue, maxvalue )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_LIST_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ) (\n"
        + "subpartition p1sp1 values in ( 1000, 2000),\n"
        + "subpartition p1sp2 values in ( 3000, 4000)\n"
        + "),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') ) (\n"
        + "subpartition p2sp1 values in ( 1000, 2000),\n"
        + "subpartition p2sp2 values in ( 3000, 4000),\n"
        + "subpartition p2sp3 values in ( 5000, 6000 )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_LIST_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list (to_days(c))\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "partition p1 values in ( to_days('2020-01-01'), to_days('2020-01-02') ) (\n"
        + "subpartition p1sp1 values in ( (1000, 2000) ),\n"
        + "subpartition p1sp2 values in ( default )\n"
        + "),\n"
        + "partition p2 values in ( to_days('2021-01-01'), to_days('2021-01-02') ) (\n"
        + "subpartition p2sp1 values in ( (1000, 2000) ),\n"
        + "subpartition p2sp2 values in ( (2000, 2000) ),\n"
        + "subpartition p2sp3 values in ( default )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by hash (a,b)\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ) subpartitions 2,\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by key (a,b)\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ) subpartitions 2,\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') ) subpartitions 3\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by range (a)\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ) (\n"
        + "subpartition p1sp1 values less than ( 1000),\n"
        + "subpartition p1sp2 values less than ( maxvalue )\n"
        + "),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') ) (\n"
        + "subpartition p2sp1 values less than ( 1000),\n"
        + "subpartition p2sp2 values less than ( 2000),\n"
        + "subpartition p2sp3 values less than ( maxvalue )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL =
        "create table if not exists `%s` (\n"
            + "a bigint unsigned not null,\n"
            + "b bigint unsigned not null,\n"
            + "c datetime NOT NULL,\n"
            + "d varchar(16) NOT NULL,\n"
            + "e varchar(16) NOT NULL\n"
            + ")\n"
            + "partition by list columns (c,d)\n"
            + "subpartition by range columns (a,b)\n"
            + "(\n"
            + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ) (\n"
            + "subpartition p1sp1 values less than ( 1000, 10000),\n"
            + "subpartition p1sp2 values less than ( maxvalue, maxvalue )\n"
            + "),\n"
            + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') ) (\n"
            + "subpartition p2sp1 values less than ( 1000, 10000),\n"
            + "subpartition p2sp2 values less than ( 2000, 10000),\n"
            + "subpartition p2sp3 values less than ( maxvalue, maxvalue )\n"
            + ")\n"
            + ");";

    protected static final String CREATE_LIST_COLUMNS_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by list (a)\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ) (\n"
        + "subpartition p1sp1 values in ( 1000, 2000),\n"
        + "subpartition p1sp2 values in ( default )\n"
        + "),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') ) (\n"
        + "subpartition p2sp1 values in ( 1000, 2000),\n"
        + "subpartition p2sp2 values in ( 3000, 4000),\n"
        + "subpartition p2sp3 values in ( default )\n"
        + ")\n"
        + ");";

    protected static final String CREATE_LIST_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "a bigint unsigned not null,\n"
        + "b bigint unsigned not null,\n"
        + "c datetime NOT NULL,\n"
        + "d varchar(16) NOT NULL,\n"
        + "e varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by list columns (c,d)\n"
        + "subpartition by list columns (a,b)\n"
        + "(\n"
        + "partition p1 values in ( ('2020-01-01', 'abc'), ('2020-01-02', 'abc') ) (\n"
        + "subpartition p1sp1 values in ( (1000, 2000) ),\n"
        + "subpartition p1sp2 values in ( default )\n"
        + "),\n"
        + "partition p2 values in ( ('2021-01-01', 'abc'), ('2021-01-02', 'abc') ) (\n"
        + "subpartition p2sp1 values in ( (1000, 2000) ),\n"
        + "subpartition p2sp2 values in ( (3000, 4000) ),\n"
        + "subpartition p2sp3 values in ( default )\n"
        + ")\n"
        + ");";

    protected final Set<SubPartitionType> HashPartitionSet = new HashSet<>(
        Arrays.asList(SubPartitionType.Hash_Hash, SubPartitionType.Hash_Key, SubPartitionType.Hash_List,
            SubPartitionType.Hash_Range, SubPartitionType.Hash_RangeColumns, SubPartitionType.Hash_ListColumns));
    protected final Set<SubPartitionType> KeyPartitionSet = new HashSet<>(
        Arrays.asList(SubPartitionType.Key_Hash, SubPartitionType.Key_Key, SubPartitionType.Key_List,
            SubPartitionType.Key_Range, SubPartitionType.Key_RangeColumns, SubPartitionType.Key_ListColumns));
    protected final Set<SubPartitionType> ListPartitionSet = new HashSet<>(
        Arrays.asList(SubPartitionType.List_Hash, SubPartitionType.List_Key, SubPartitionType.List_List,
            SubPartitionType.List_Range, SubPartitionType.List_RangeColumns, SubPartitionType.List_ListColumns));
    protected final Set<SubPartitionType> RangePartitionSet = new HashSet<>(
        Arrays.asList(SubPartitionType.Range_Hash, SubPartitionType.Range_Key, SubPartitionType.Range_List,
            SubPartitionType.Range_Range, SubPartitionType.Range_RangeColumns, SubPartitionType.Range_ListColumns));
    protected final Set<SubPartitionType> ListColumnsPartitionSet = new HashSet<>(
        Arrays.asList(SubPartitionType.ListColumns_Hash, SubPartitionType.ListColumns_Key,
            SubPartitionType.ListColumns_List,
            SubPartitionType.ListColumns_Range, SubPartitionType.ListColumns_RangeColumns,
            SubPartitionType.ListColumns_ListColumns));
    protected final Set<SubPartitionType> RangeColumnsPartitionSet = new HashSet<>(
        Arrays.asList(SubPartitionType.RangeColumns_Hash, SubPartitionType.RangeColumns_Key,
            SubPartitionType.RangeColumns_List,
            SubPartitionType.RangeColumns_Range, SubPartitionType.RangeColumns_RangeColumns,
            SubPartitionType.RangeColumns_ListColumns));

    protected String dbName;
    protected boolean replayImplicitMarkSql = true;

    protected String createTable(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType,
                                 boolean useTemplate)
        throws SQLException {
        String sql;
        String formatSql;
        String tokenHints = buildTokenHints();
        String tableName = "t_" + partitionType.name().toLowerCase()
            + (useTemplate ? "_template_" : "_not_template_") + System.currentTimeMillis();

        if (partitionType == SubPartitionType.Key_Key) {
            formatSql = useTemplate ? CREATE_KEY_KEY_TP_TABLE_SQL : CREATE_KEY_KEY_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Key_Hash) {
            formatSql = useTemplate ? CREATE_KEY_HASH_TP_TABLE_SQL : CREATE_KEY_HASH_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Key_List) {
            formatSql = useTemplate ? CREATE_KEY_LIST_TP_TABLE_SQL : CREATE_KEY_LIST_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Key_ListColumns) {
            formatSql = useTemplate ? CREATE_KEY_LIST_COLUMNS_TP_TABLE_SQL : CREATE_KEY_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Key_Range) {
            formatSql = useTemplate ? CREATE_KEY_RANGE_TP_TABLE_SQL : CREATE_KEY_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Key_RangeColumns) {
            formatSql = useTemplate ? CREATE_KEY_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_KEY_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == SubPartitionType.Hash_Key) {
            formatSql = useTemplate ? CREATE_HASH_KEY_TP_TABLE_SQL : CREATE_HASH_KEY_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Hash_Hash) {
            formatSql = useTemplate ? CREATE_HASH_HASH_TP_TABLE_SQL : CREATE_HASH_HASH_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Hash_List) {
            formatSql = useTemplate ? CREATE_HASH_LIST_TP_TABLE_SQL : CREATE_HASH_LIST_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Hash_ListColumns) {
            formatSql = useTemplate ? CREATE_HASH_LIST_COLUMNS_TP_TABLE_SQL : CREATE_HASH_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Hash_Range) {
            formatSql = useTemplate ? CREATE_HASH_RANGE_TP_TABLE_SQL : CREATE_HASH_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Hash_RangeColumns) {
            formatSql = useTemplate ? CREATE_HASH_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_HASH_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == SubPartitionType.List_Key) {
            formatSql = useTemplate ? CREATE_LIST_KEY_TP_TABLE_SQL : CREATE_LIST_KEY_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.List_Hash) {
            formatSql = useTemplate ? CREATE_LIST_HASH_TP_TABLE_SQL : CREATE_LIST_HASH_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.List_List) {
            formatSql = useTemplate ? CREATE_LIST_LIST_TP_TABLE_SQL : CREATE_LIST_LIST_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.List_ListColumns) {
            formatSql = useTemplate ? CREATE_LIST_LIST_COLUMNS_TP_TABLE_SQL : CREATE_LIST_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.List_Range) {
            formatSql = useTemplate ? CREATE_LIST_RANGE_TP_TABLE_SQL : CREATE_LIST_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.List_RangeColumns) {
            formatSql = useTemplate ? CREATE_LIST_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_LIST_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == SubPartitionType.ListColumns_Key) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_KEY_TP_TABLE_SQL : CREATE_LIST_COLUMNS_KEY_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.ListColumns_Hash) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_HASH_TP_TABLE_SQL : CREATE_LIST_COLUMNS_HASH_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.ListColumns_List) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_LIST_TP_TABLE_SQL : CREATE_LIST_COLUMNS_LIST_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.ListColumns_ListColumns) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL :
                CREATE_LIST_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.ListColumns_Range) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_RANGE_TP_TABLE_SQL : CREATE_LIST_COLUMNS_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.ListColumns_RangeColumns) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL :
                CREATE_LIST_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == SubPartitionType.Range_Key) {
            formatSql = useTemplate ? CREATE_RANGE_KEY_TP_TABLE_SQL : CREATE_RANGE_KEY_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Range_Hash) {
            formatSql = useTemplate ? CREATE_RANGE_HASH_TP_TABLE_SQL : CREATE_RANGE_HASH_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Range_List) {
            formatSql = useTemplate ? CREATE_RANGE_LIST_TP_TABLE_SQL : CREATE_RANGE_LIST_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Range_ListColumns) {
            formatSql = useTemplate ? CREATE_RANGE_LIST_COLUMNS_TP_TABLE_SQL : CREATE_RANGE_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Range_Range) {
            formatSql = useTemplate ? CREATE_RANGE_RANGE_TP_TABLE_SQL : CREATE_RANGE_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.Range_RangeColumns) {
            formatSql =
                useTemplate ? CREATE_RANGE_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_RANGE_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == SubPartitionType.RangeColumns_Key) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_KEY_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_KEY_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.RangeColumns_Hash) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_HASH_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_HASH_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.RangeColumns_List) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_LIST_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_LIST_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.RangeColumns_ListColumns) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL :
                CREATE_RANGE_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.RangeColumns_Range) {
            formatSql =
                useTemplate ? CREATE_RANGE_COLUMNS_RANGE_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == SubPartitionType.RangeColumns_RangeColumns) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL :
                CREATE_RANGE_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL;
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        sql = tokenHints + String.format(formatSql, tableName);

        // 暂时指定唯一table group, 从而避免sub job导致的hints丢失问题
        //String utg = ("mytg_" + UUID.randomUUID()).replace("-", "_");
        //stmt.execute("create tablegroup if not exists " + utg);
        //sql = sql.substring(0, sql.length() - 1) + " tablegroup=" + utg;

        // create and check
        checkContext.updateAndGetMarkList(dbName);
        logger.info("create table sql:" + sql);
        stmt.execute(sql);
        checkAfterCreateTable(checkContext, tableName, sql);

        return tableName;
    }

    protected void dropTable(Statement stmt, String tableName) throws SQLException {
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        String tokenHints = buildTokenHints();
        String sql = tokenHints + String.format("drop table %s", tableName);
        stmt.execute(sql);

        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);
    }

    protected boolean isSupportReorganizePartition(SubPartitionType partitionType) {
        return !KeyPartitionSet.contains(partitionType) && !HashPartitionSet.contains(partitionType);
    }

    protected boolean isSupportModifyPartition(SubPartitionType partitionType) {
        return RangePartitionSet.contains(partitionType) || RangeColumnsPartitionSet.contains(partitionType)
            || ListPartitionSet.contains(partitionType) || ListColumnsPartitionSet.contains(partitionType);
    }

    protected boolean isSupportModifyPartitionWithValues(SubPartitionType partitionType) {
        return ListPartitionSet.contains(partitionType) || ListColumnsPartitionSet.contains(partitionType);
    }

    protected void doAlterPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType,
                                    String sqlForTP, String sqlForNTP)
        throws SQLException {
        String tableName;

        if (StringUtils.isNotEmpty(sqlForTP)) {
            tableName = createTable(checkContext, stmt, partitionType, true);
            alterPartition(checkContext, stmt, sqlForTP, tableName);
            tableName = createTable(checkContext, stmt, partitionType, true);
            alterTableGroupPartition(checkContext, stmt, sqlForTP, tableName);
        }

        if (StringUtils.isNotEmpty(sqlForNTP)) {
            tableName = createTable(checkContext, stmt, partitionType, false);
            alterPartition(checkContext, stmt, sqlForNTP, tableName);
            tableName = createTable(checkContext, stmt, partitionType, false);
            alterTableGroupPartition(checkContext, stmt, sqlForNTP, tableName);
        }
    }

    private void alterPartition(DdlCheckContext checkContext, Statement stmt, String formatSql, String tableName)
        throws SQLException {
        checkContext.updateAndGetMarkList(dbName);

        String tokenHints = buildTokenHints();
        formatSql = String.format(formatSql, "table");
        String sql = tokenHints + String.format(formatSql, tableName);
        logger.info("alter sql:" + sql);
        stmt.execute(sql);

        checkAfterAlterTablePartition(checkContext, sql, tableName);
    }

    private void alterTableGroupPartition(DdlCheckContext checkContext, Statement stmt, String formatSql,
                                          String tableName)
        throws SQLException {
        checkContext.updateAndGetMarkList(dbName);

        String tokenHints = buildTokenHints();
        formatSql = String.format(formatSql, "tablegroup");
        String tableGroupName = queryTableGroup(dbName, tableName);
        String sql = tokenHints + String.format(formatSql, tableGroupName);
        logger.info("alter table group sql:" + sql);
        List<String> tables = queryTableCountInTableGroup(tableGroupName);
        stmt.execute(sql);

        checkAfterAlterTableGroup(checkContext, tableGroupName, sql, tables);
    }

    protected void checkAfterAlterTablePartition(DdlCheckContext ddlCheckContext, String sql,
                                                 String tableName) {
        commonCheckExistsAfterDdlWithCallback(ddlCheckContext, dbName, tableName, sql, p -> {
            List<DdlRecordInfo> afterList = p.getValue();

            assertSqlEquals(sql, CdcTestUtil.removeImplicitTgSyntax(afterList.get(0).getDdlSql()));
            Assert.assertEquals(queryTopology(tableName), afterList.get(0).getTopology());
            Assert.assertEquals("ALTER_TABLE", afterList.get(0).getSqlKind());

            checkImplicitTableGroup(ddlCheckContext, dbName, tableName,
                Sets.newHashSet(queryTableGroup(dbName, tableName)), afterList.get(0).getEffectiveSql());
        });
    }

    protected void checkAfterAlterTableGroup(DdlCheckContext ddlCheckContext, String tableGroup, String expectSql,
                                             List<String> tables) {
        List<DdlRecordInfo> ddlRecordInfosBefore = ddlCheckContext.getMarkList(dbName);
        List<DdlRecordInfo> ddlRecordInfosAfter = ddlCheckContext.updateAndGetMarkList(dbName);

        Assert.assertEquals(ddlRecordInfosBefore.size() + tables.size() + 1, ddlRecordInfosAfter.size());
        Assert.assertEquals(tableGroup, ddlRecordInfosAfter.get(0).getTableName());

        Map<String, AtomicInteger> map = new HashMap<>();
        for (int i = 0; i < tables.size() + 1; i++) {
            Assert.assertEquals("ALTER_TABLEGROUP", ddlRecordInfosAfter.get(i).getSqlKind());
            Assert.assertEquals(CdcTestUtil.getServerId4Check(serverId),
                ddlRecordInfosAfter.get(i).getDdlExtInfo().getServerId());
            assertSqlEquals(expectSql, ddlRecordInfosAfter.get(i).getEffectiveSql());

            if (i == 0) {
                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    ddlRecordInfosAfter.get(i).getVisibility());
                Assert.assertTrue(ddlRecordInfosAfter.get(i).getTopology().isEmpty());
                if (tableGroup.startsWith("tg")) {
                    Assert.assertFalse(ddlRecordInfosAfter.get(i).getDdlExtInfo().getManuallyCreatedTableGroup());
                } else {
                    Assert.assertTrue(ddlRecordInfosAfter.get(i).getDdlExtInfo().getManuallyCreatedTableGroup());
                }
            } else {
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    ddlRecordInfosAfter.get(i).getVisibility());
                Assert.assertEquals(queryTopology(ddlRecordInfosAfter.get(i).getTableName()),
                    ddlRecordInfosAfter.get(i).getTopology());
            }

            map.computeIfAbsent(ddlRecordInfosAfter.get(i).getTableName(), k -> new AtomicInteger()).incrementAndGet();
        }

        for (String table : tables) {
            Assert.assertEquals(1, map.get(table).get());
        }
    }

    protected void checkAfterAlterTableGroupWithoutActualMark(DdlCheckContext ddlCheckContext, String expectSql,
                                                              List<String> tables) {
        List<DdlRecordInfo> ddlRecordInfosBefore = ddlCheckContext.getMarkList(dbName);
        List<DdlRecordInfo> ddlRecordInfosAfter = ddlCheckContext.updateAndGetMarkList(dbName);

        Assert.assertEquals(ddlRecordInfosBefore.size() + tables.size(), ddlRecordInfosAfter.size());
        Map<String, AtomicInteger> map = new HashMap<>();
        for (int i = 0; i < tables.size(); i++) {
            Assert.assertEquals("ALTER_TABLEGROUP", ddlRecordInfosAfter.get(i).getSqlKind());
            Assert.assertEquals(CdcTestUtil.getServerId4Check(serverId),
                ddlRecordInfosAfter.get(i).getDdlExtInfo().getServerId());
            assertSqlEquals(expectSql, ddlRecordInfosAfter.get(i).getEffectiveSql());
            Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(), ddlRecordInfosAfter.get(i).getVisibility());
            Assert.assertEquals(queryTopology(ddlRecordInfosAfter.get(i).getTableName()),
                ddlRecordInfosAfter.get(i).getTopology());

            map.computeIfAbsent(ddlRecordInfosAfter.get(i).getTableName(), k -> new AtomicInteger()).incrementAndGet();
        }

        for (String table : tables) {
            Assert.assertEquals(1, map.get(table).get());
        }
    }

    protected void checkAfterCreateTable(DdlCheckContext checkContext, String tableName, String expectSql) {
        commonCheckExistsAfterDdlWithCallback(checkContext, dbName, tableName, expectSql, i -> {
            List<DdlRecordInfo> afterList = i.getValue();
            Assert.assertEquals("CREATE_TABLE", afterList.get(0).getSqlKind());
            Assert.assertEquals(queryTopology(tableName), afterList.get(0).getTopology());
            Assert.assertNotEquals(expectSql, afterList.get(0).getEffectiveSql());
            Assert.assertTrue(StringUtils.containsIgnoreCase(afterList.get(0).getEffectiveSql(), "implicit"));

            checkImplicitTableGroup(checkContext, dbName, tableName,
                Sets.newHashSet(queryTableGroup(dbName, tableName)), afterList.get(0).getEffectiveSql());
        });
    }

    @SneakyThrows
    protected void checkImplicitTableGroup(DdlCheckContext checkContext, String dbName, String tableName,
                                           Set<String> expectTableGroups, String markSql) {
        Set<String> tableGroups = new HashSet<>();
        implicitTableGroupChecker.checkSql(dbName, tableName, markSql, tableGroups);
        Assert.assertEquals(expectTableGroups, tableGroups);

        if (replayImplicitMarkSql) {
            try (Statement stmt = tddlConnection.createStatement()) {
                String newTableName = (String) checkContext.getMetadata()
                    .computeIfAbsent(tableName, k -> "t_replay_" + RandomUtils.nextLong());
                String replaySql = ImplicitTableGroupUtil.rewriteTableName(markSql, newTableName);

                logger.info("start to replay sql : " + replaySql);
                stmt.execute(replaySql);

                List<DdlRecordInfo> afterList = checkContext.updateAndGetMarkList(dbName);
                tableGroups = new HashSet<>();
                String newMarkSql = afterList.get(0).getEffectiveSql();
                implicitTableGroupChecker.checkSql(dbName, newTableName, markSql, tableGroups);
                assertSqlEquals(replaySql, newMarkSql);
                Assert.assertEquals(expectTableGroups, tableGroups);
            }
        }
    }

    protected String getMovePartitionSqlForTable(String tableName) throws SQLException {
        Map<String, String> map = getMasterGroupStorageMap();
        String fromStorage = null;
        String toStorage = null;
        String partition = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String temp = getFirstLevelPartitionByGroupName(entry.getKey(), tableName);
            if (StringUtils.isNotBlank(temp)) {
                fromStorage = entry.getValue();
                partition = temp;
                break;
            }
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!StringUtils.equals(fromStorage, entry.getValue())) {
                toStorage = entry.getValue();
            }
        }

        return String.format("ALTER TABLE %s MOVE PARTITIONS %s to '%s'", tableName, partition, toStorage);
    }

    protected String getMoveSubPartitionSqlForTable(String tableName) throws SQLException {
        Map<String, String> map = getMasterGroupStorageMap();
        String fromStorage = null;
        String toStorage = null;
        String partition = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String temp = getSecondLevelPartitionByGroupName(entry.getKey(), tableName);
            if (StringUtils.isNotBlank(temp)) {
                fromStorage = entry.getValue();
                partition = temp;
                break;
            }
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!StringUtils.equals(fromStorage, entry.getValue())) {
                toStorage = entry.getValue();
            }
        }

        return String.format("ALTER TABLE %s MOVE SUBPARTITIONS %s to '%s'", tableName, partition, toStorage);
    }

    protected List<String> queryTableCountInTableGroup(String tableGroupName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                "show full tablegroup where table_group_name = '" + tableGroupName + "'")) {
                while (rs.next()) {
                    ArrayList<String> result = new ArrayList<>();
                    String s = rs.getString("TABLES");
                    String[] array = StringUtils.split(s, ",");
                    for (String item : array) {
                        String[] split = StringUtil.split(item.trim(), ".");
                        result.add(split[0]);
                    }
                    return result;
                }
                return new ArrayList<>();
            }
        }
    }

    public enum SubPartitionType {
        Hash_Hash, Hash_Key, Hash_Range, Hash_RangeColumns, Hash_List, Hash_ListColumns,

        Key_Hash, Key_Key, Key_Range, Key_RangeColumns, Key_List, Key_ListColumns,

        Range_Hash, Range_Key, Range_Range, Range_RangeColumns, Range_List, Range_ListColumns,

        RangeColumns_Hash, RangeColumns_Key, RangeColumns_Range, RangeColumns_RangeColumns, RangeColumns_List,
        RangeColumns_ListColumns,

        List_Hash, List_Key, List_Range, List_RangeColumns, List_List, List_ListColumns,

        ListColumns_Hash, ListColumns_Key, ListColumns_Range, ListColumns_RangeColumns, ListColumns_List,
        ListColumns_ListColumns;
    }
}
