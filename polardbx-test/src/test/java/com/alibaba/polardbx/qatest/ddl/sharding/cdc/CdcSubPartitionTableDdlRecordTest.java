package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.ddl.sharding.cdc.CdcTestUtil.getServerId4Check;

/**
 * 针对子分区表的DDL打标测试
 * <p>
 * 纯一级分区级别变更操作:对一级分区进行的操作不涉及子分区
 * 非纯一级分区级别变更操作:对一级分区进行操作的时候定义子分区, 主要包括三种分区操作:split, add, reorganize
 *
 * @author yudong
 * @since 2023/1/31 15:15
 **/
public class CdcSubPartitionTableDdlRecordTest extends CdcBaseTest {

    // ====================== create subpartition with template sql ====================

    private static final String CREATE_HASH_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ") \n"
        + "partition by hash (c,d) partitions 4\n"
        + "subpartition by hash (a,b) subpartitions 4;";

    private static final String CREATE_HASH_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by hash (c,d) partitions 4\n"
        + "subpartition by key (a,b) subpartitions 4;";

    private static final String CREATE_HASH_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (c,d) partitions 4\n"
        + "subpartition by hash (a,b) subpartitions 4;";

    private static final String CREATE_KEY_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
        + "`a` bigint unsigned not null,\n"
        + "`b` bigint unsigned not null,\n"
        + "`c` datetime NOT NULL,\n"
        + "`d` varchar(16) NOT NULL,\n"
        + "`e` varchar(16) NOT NULL\n"
        + ")\n"
        + "partition by key (c,d) partitions 4\n"
        + "subpartition by key (a,b) subpartitions 4;";

    private static final String CREATE_KEY_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_HASH_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_KEY_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_RANGE_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_LIST_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_HASH_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_KEY_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_RANGE_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_HASH_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_KEY_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_RANGE_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_LIST_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private static final String CREATE_LIST_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL = "create table if not exists `%s` (\n"
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

    private final static String DB_NAME_PREFIX = "cdc_ddl_test_sub_partition_";
    private final static AtomicInteger DB_NAME_SUFFIX = new AtomicInteger(0);
    private final String dbName;
    private final String serverId;

    private final Set<PartitionType> HashPartitionSet = new HashSet<>(
        Arrays.asList(PartitionType.Hash_Hash, PartitionType.Hash_Key, PartitionType.Hash_List,
            PartitionType.Hash_Range, PartitionType.Hash_RangeColumns, PartitionType.Hash_ListColumns));
    private final Set<PartitionType> KeyPartitionSet = new HashSet<>(
        Arrays.asList(PartitionType.Key_Hash, PartitionType.Key_Key, PartitionType.Key_List,
            PartitionType.Key_Range, PartitionType.Key_RangeColumns, PartitionType.Key_ListColumns));
    private final Set<PartitionType> ListPartitionSet = new HashSet<>(
        Arrays.asList(PartitionType.List_Hash, PartitionType.List_Key, PartitionType.List_List,
            PartitionType.List_Range, PartitionType.List_RangeColumns, PartitionType.List_ListColumns));
    private final Set<PartitionType> RangePartitionSet = new HashSet<>(
        Arrays.asList(PartitionType.Range_Hash, PartitionType.Range_Key, PartitionType.Range_List,
            PartitionType.Range_Range, PartitionType.Range_RangeColumns, PartitionType.Range_ListColumns));
    private final Set<PartitionType> ListColumnsPartitionSet = new HashSet<>(
        Arrays.asList(PartitionType.ListColumns_Hash, PartitionType.ListColumns_Key, PartitionType.ListColumns_List,
            PartitionType.ListColumns_Range, PartitionType.ListColumns_RangeColumns,
            PartitionType.ListColumns_ListColumns));
    private final Set<PartitionType> RangeColumnsPartitionSet = new HashSet<>(
        Arrays.asList(PartitionType.RangeColumns_Hash, PartitionType.RangeColumns_Key, PartitionType.RangeColumns_List,
            PartitionType.RangeColumns_Range, PartitionType.RangeColumns_RangeColumns,
            PartitionType.RangeColumns_ListColumns));

    public CdcSubPartitionTableDdlRecordTest(String serverId) {
        this.dbName = DB_NAME_PREFIX + DB_NAME_SUFFIX.incrementAndGet();
        if (StringUtils.equals(serverId, "8989")) {
            this.serverId = serverId;
        } else {
            this.serverId = null;
        }
    }

    @Parameterized.Parameters
    public static List<String[]> getTestParameters() {
        return Arrays.asList(new String[][] {{"9999"}, {"8989"}});
    }

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        String sql;
        String tokenHints;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select database()");

            if (StringUtils.isNotBlank(serverId)) {
                sql = "set polardbx_server_id=" + serverId;
                stmt.execute(sql);
            }

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database if exists " + dbName;
            stmt.execute(sql);
            Thread.sleep(2000);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

            tokenHints = buildTokenHints();
            sql = tokenHints + "create database " + dbName + " mode = 'auto' ";
            stmt.execute(sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

            sql = "use " + dbName;
            stmt.execute(sql);

            // ================ 测试常规DDL操作 ================ //

            for (PartitionType partitionType : PartitionType.values()) {
                testNormalDdl(stmt, partitionType);
            }

            // ================ 测试对包含子分区的表做纯一级分区级变更操作 ================ //

            for (PartitionType partitionType : PartitionType.values()) {
                testSplitPartition(stmt, partitionType);
                testMergePartition(stmt, partitionType);
                testMovePartition(stmt, partitionType);

                if (isSupportReorganizePartition(partitionType)) {
                    testReorganizePartition(stmt, partitionType);
                }

                if (isSupportModifyPartition(partitionType)) {
                    testAddPartition(stmt, partitionType);
                    testDropPartition(stmt, partitionType);
                    testTruncatePartition(stmt, partitionType);
                }

                if (isSupportModifyPartitionWithValues(partitionType)) {
                    testModifyPartitionAddValues(stmt, partitionType);
                    testModifyPartitionDropValues(stmt, partitionType);
                }
            }

            // ================ 测试对包含子分区的表做纯子分区级变更操作 ================ //

            testSplitSubPartition(stmt, PartitionType.Key_Key);
            testMergeSubPartition(stmt, PartitionType.Key_Key);
            testMoveSubPartition(stmt, PartitionType.Key_Key);
            testAddSubPartition(stmt, PartitionType.Range_Range);
            testDropSubPartition(stmt, PartitionType.Range_Range);
            testModifySubPartitionDropSubPartition(stmt, PartitionType.Range_Range);
            testModifySubPartitionAddValues(stmt, PartitionType.List_List);
            testModifySubPartitionDropValues(stmt, PartitionType.List_List);
            testTruncateSubPartition(stmt, PartitionType.Key_Key);
            testReorganizeSubPartition(stmt, PartitionType.List_List);

            // ================ 测试对包含子分区的表做非纯一级分区级变更操作 ================ //

            testSplitPartitionWithSubPartition(stmt, PartitionType.Range_Range);
            testAddPartitionWithSubPartition(stmt, PartitionType.Range_Range);
            testReorganizePartitionWithSubPartition(stmt, PartitionType.Range_Range);

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database " + dbName;
            stmt.execute(sql);
            stmt.execute("use __cdc__");
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());
        }
    }

    private boolean isSupportReorganizePartition(PartitionType partitionType) {
        return !KeyPartitionSet.contains(partitionType) && !HashPartitionSet.contains(partitionType);
    }

    private boolean isSupportModifyPartition(PartitionType partitionType) {
        return RangePartitionSet.contains(partitionType) || RangeColumnsPartitionSet.contains(partitionType)
            || ListPartitionSet.contains(partitionType) || ListColumnsPartitionSet.contains(partitionType);
    }

    private boolean isSupportModifyPartitionWithValues(PartitionType partitionType) {
        return ListPartitionSet.contains(partitionType) || ListColumnsPartitionSet.contains(partitionType);
    }

    private void testNormalDdl(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test normal ddl with partition type " + partitionType);
        String tableName;
        tableName = createTable(stmt, partitionType, true);
        doNormalDdl(stmt, tableName);
        dropTable(stmt, tableName);
        tableName = createTable(stmt, partitionType, false);
        doNormalDdl(stmt, tableName);
        dropTable(stmt, tableName);
    }

    private void doNormalDdl(Statement stmt, String tableName) throws SQLException {
        String sql;
        String tokenHints;

        //--------------------------------------------------------------------------------
        //-----------------------------------Test Columns---------------------------------
        //--------------------------------------------------------------------------------
        // 测试 加列
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String.format("alter table %s add column add1 varchar(20) not null default '111'", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 加列，指定列顺序
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String
                .format("alter table %s add column add2 varchar(20) not null default '222' after b", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 同时加减列
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String.format("alter table %s add column add3 bigint default 0,drop column add2", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 更改列精度
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s modify add1 varchar(50) not null default '111'", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 更改列
        tokenHints = buildTokenHints();
        sql = tokenHints + String
            .format("alter table %s change column add1 add111 varchar(50) not null default '111'", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add111", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add3", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        //--------------------------------------------------------------------------------
        //-------------------------------Test Local Indexes-------------------------------
        //--------------------------------------------------------------------------------
        // 测试 加索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add index idx_test(`b`)", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 加唯一键索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add unique idx_job(`c`)", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // 测试 加索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("create index idx_gmt on %s(`d`)", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        // Test Step
        // 对于含有聚簇索引的表，引擎不支持一个语句里drop两个index，所以一个语句包含两个drop的sql就不用测试了
        // 否则会报错：optimize error by Do not support multi ALTER statements on table with clustered index
        // tokenHints = buildTokenHints();
        // sql = tokenHints + String.format("alter table %s drop index idx_test", tableName1);
        // stmt.execute(sql);
        // Assert.assertEquals(sql, getDdlRecordSql(tokenHints));

        // 测试 删除索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop index idx_gmt on %s", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        //--------------------------------------------------------------------------------
        //------------------------------------Test Truncate ------------------------------
        //--------------------------------------------------------------------------------
        // 如果是带有GSI的表，会报错: Does not support truncate table with global secondary index，so use t_ddl_test_zzz for test
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("truncate table %s", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        //--------------------------------------------------------------------------------
        //------------------------------------Test rename --------------------------------
        //--------------------------------------------------------------------------------
        // 如果是带有GSI的表，会报错: Does not support modify primary table 't_ddl_test' cause global secondary index exists,so use t_ddl_test_yyy for test

        tokenHints = buildTokenHints();
        String newTableName = tableName + "_new";
        sql = tokenHints + String.format("rename table %s to %s", tableName, newTableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());
        sql = "rename table " + newTableName + " to " + tableName;
        stmt.execute(sql);
    }

    // ===================== 纯一级分区级的变更操作 ==================== //

    private void testSplitPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test split table partition with partition type " + partitionType);
        String sql;

        if (KeyPartitionSet.contains(partitionType) || HashPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1";
        } else if (RangePartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + "( partition p11 values less than ( to_days('2010-01-01') ),\n"
                + "  partition p12 values less than ( to_days('2020-01-01') ) )";
        } else if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + " ( partition p11 values in ( to_days('2020-01-01') ),\n"
                + "   partition p12 values in ( to_days('2020-01-02') ) )";
        } else if (RangeColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + "( partition p11 values less than ( '2010-01-01', 'abc' ),\n"
                + "  partition p12 values less than ( '2020-01-01', 'abc') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + "( partition p11 values in ( ('2020-01-01', 'abc') ),\n"
                + "  partition p12 values in ( ('2020-01-02', 'abc') ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sql, sql);
    }

    private void testMergePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test merge table partition with partition type " + partitionType);

        String sql = "alter %s %%s merge partitions p1,p2 to p12";

        doAlterPartition(stmt, partitionType, sql, sql);
    }

    private void testMovePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test move table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;

        tableName = createTable(stmt, partitionType, true);
        tokenHints = buildTokenHints();
        sql = tokenHints + getMovePartitionSqlForTable(tableName);
        stmt.execute(sql);
        checkTableAfterAlterPartition(sql, tokenHints, tableName);

        tableName = createTable(stmt, partitionType, false);
        tokenHints = buildTokenHints();
        sql = tokenHints + getMovePartitionSqlForTable(tableName);
        stmt.execute(sql);
        checkTableAfterAlterPartition(sql, tokenHints, tableName);
    }

    private void testAddPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test add table partition with partition type " + partitionType);
        String sql;

        if (RangePartitionSet.contains(partitionType)) {
            sql = "alter %s %%s add partition ( partition p3 values less than ( to_days('2022-01-01') ) )";
        } else if (RangeColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s add partition ( partition p3 values less than ( '2022-01-01', 'abc' ) )";
        } else if (ListPartitionSet.contains(partitionType)) {
            sql =
                "alter %s %%s add partition ( partition p3 values in ( to_days('2022-01-01'),to_days('2022-01-02') ) )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s add partition ( partition p3 values in ( ('2022-01-01','abc'),('2022-01-02','abc') ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sql, sql);
    }

    private void testDropPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test drop table partition with partition type " + partitionType);
        String sql = "alter %s %%s drop partition p1";
        doAlterPartition(stmt, partitionType, sql, sql);
    }

    private void testModifyPartitionAddValues(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test modify table partition add values with partition type " + partitionType);
        String sql;

        if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 add values ( to_days('2020-02-01'), to_days('2020-02-02') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 add values ( ('2020-02-01', 'abc'),('2020-02-02', 'abc') )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sql, sql);
    }

    private void testModifyPartitionDropValues(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test modify table partition drop values with partition type " + partitionType);
        String sql;

        if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 drop values ( to_days('2020-01-01') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 drop values ( ('2020-01-01', 'abc') )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sql, sql);
    }

    private void testTruncatePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test truncate partition with partition type " + partitionType);
        String tableName;
        String sql;
        String formatSql = "alter %s %%s truncate partition p1";

        tableName = createTable(stmt, partitionType, true);
        String tokenHints = buildTokenHints();
        sql = String.format(formatSql, "table");
        sql = tokenHints + String.format(sql, tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints == null ? sql : tokenHints));
        Assert.assertEquals(getServerId4Check(serverId),
            getDdlExtInfo(tokenHints == null ? sql : tokenHints).getServerId());
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints == null ? sql : tokenHints));
    }

    private void testReorganizePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test reorganize table partition with partition type " + partitionType);
        String sql;

        if (RangePartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + "( partition p11 values less than ( to_days('2010-01-01') ),\n"
                + "  partition p12 values less than ( to_days('2020-01-01') ) )";
        } else if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + " ( partition p11 values in ( to_days('2020-01-01') ),\n"
                + "   partition p12 values in ( to_days('2020-01-02') ) )";
        } else if (RangeColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + "( partition p11 values less than ( '2010-01-01', 'abc' ),\n"
                + "  partition p12 values less than ( '2020-01-01', 'abc') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + "( partition p11 values in ( ('2020-01-01', 'abc') ),\n"
                + "  partition p12 values in ( ('2020-01-02', 'abc') ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sql, sql);
    }

    // ===================== 纯子分区级的变更操作 ==================== //

    private void testSplitSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test split table sub partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == PartitionType.Key_Key) {
            sqlForTP = "alter %s %%s split subpartition sp1";
            sqlForNTP = "alter %s %%s split subpartition p1sp1";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testMergeSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test merge table sub partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == PartitionType.Key_Key) {
            sqlForTP = "alter %s %%s merge subpartitions sp1,sp2 to sp12";
            sqlForNTP = "alter %s %%s merge subpartitions p1sp1,p1sp2 to p1sp12";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testMoveSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test move table sub partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;

        if (partitionType == PartitionType.Key_Key) {
            // 模版化定义的子分区不支持move操作
            tableName = createTable(stmt, partitionType, false);
            tokenHints = buildTokenHints();
            sql = tokenHints + getMoveSubPartitionSqlForTable(tableName);
            stmt.execute(sql);
            checkTableAfterAlterPartition(sql, tokenHints, tableName);
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testAddSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test add table partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == PartitionType.Range_Range) {
            sqlForTP = "alter %s %%s add subpartition ( subpartition sp3 values less than ( 3000 ) )";
            sqlForNTP =
                "alter %s %%s modify partition p1 add subpartition ( subpartition p1sp3 values less than ( 3000 ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testDropSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test drop sub partition with partition type " + partitionType);
        String sqlForTP;

        if (partitionType == PartitionType.Range_Range) {
            // 从特定分区删除非模版化子分区，使用modify partition drop subpartition
            sqlForTP = "alter %s %%s drop subpartition sp2";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, null);
    }

    private void testModifySubPartitionDropSubPartition(Statement stmt, PartitionType partitionType)
        throws SQLException {
        logger.info("start to test modify partition drop sub partition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == PartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s modify partition p1 drop subpartition p1sp1";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, null, sqlForNTP);
    }

    private void testModifySubPartitionAddValues(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test modify table sub partition add values with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == PartitionType.List_List) {
            sqlForTP = "alter %s %%s modify subpartition sp1 add values ( 100, 200 )";
            sqlForNTP = "alter %s %%s modify subpartition p1sp1 add values ( 100, 200 )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testModifySubPartitionDropValues(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test modify table sub partition drop values with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == PartitionType.List_List) {
            sqlForTP = "alter %s %%s modify subpartition sp1 drop values ( 2000 )";
            sqlForNTP = "alter %s %%s modify subpartition p1sp1 drop values ( 2000 )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testTruncateSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test truncate sub partition with partition type " + partitionType);
        String sql;
        String tableName;

        if (partitionType == PartitionType.Key_Key) {
            // 模版化定义的子分区不支持截断
            String formatSql = "alter %s %%s truncate subpartition p1sp1";
            tableName = createTable(stmt, partitionType, true);
            String tokenHints = buildTokenHints();
            sql = String.format(formatSql, "table");
            sql = tokenHints + String.format(sql, tableName);
            stmt.execute(sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints == null ? sql : tokenHints));
            Assert
                .assertEquals(getServerId4Check(serverId),
                    getDdlExtInfo(tokenHints == null ? sql : tokenHints).getServerId());
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints == null ? sql : tokenHints));
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testReorganizeSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test reorganize table sub partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == PartitionType.List_List) {
            sqlForTP = "alter %s %%s reorganize subpartition sp1 into "
                + "( subpartition sp11 values in (1000), subpartition sp12 values in (2000) )";
            sqlForNTP = "alter %s %%s reorganize subpartition p1sp1 into "
                + "( subpartition p1sp11 values in (1000), subpartition p1sp12 values in (2000) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, sqlForTP, sqlForNTP);
    }

    // ===================== 非纯一级分区级的变更操作 ==================== //

    private void testSplitPartitionWithSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test split partition with sub partition definition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == PartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s split partition p1 into (\n"
                + "partition p11 values less than ( to_days('2010-01-01') ) subpartitions 4 (\n"
                + "  subpartition p11sp1 values less than (500),\n"
                + "  subpartition p11sp2 values less than (1000),\n"
                + "  subpartition p11sp3 values less than (1500),\n"
                + "  subpartition p11sp4 values less than (2000)\n"
                + "),\n"
                + "partition p12 values less than ( to_days('2020-01-01') ) subpartitions 3 (\n"
                + "  subpartition p12sp1 values less than (500),\n"
                + "  subpartition p12sp2 values less than (1000),\n"
                + "  subpartition p12sp3 values less than (2000)\n"
                + ")\n"
                + ")";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, null, sqlForNTP);
    }

    private void testAddPartitionWithSubPartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test add partition with sub partition definition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == PartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s add partition (\n"
                + "partition p3 values less than ( to_days('2022-01-01') ) subpartitions 2 (\n"
                + "  subpartition p3sp1 values less than ( 1000 ),\n"
                + "  subpartition p3sp2 values less than ( 2000 )\n"
                + ")\n"
                + ")";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, null, sqlForNTP);
    }

    private void testReorganizePartitionWithSubPartition(Statement stmt, PartitionType partitionType)
        throws SQLException {
        logger.info(
            "start to test reorganize partition with sub partition definition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == PartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s reorganize partition p1 into (\n"
                + "partition p11 values less than ( to_days('2010-01-01') ) subpartitions 4 (\n"
                + "  subpartition p11sp1 values less than (500),\n"
                + "  subpartition p11sp2 values less than (1000),\n"
                + "  subpartition p11sp3 values less than (1500),\n"
                + "  subpartition p11sp4 values less than (2000)\n"
                + "),\n"
                + "partition p12 values less than ( to_days('2020-01-01') ) subpartitions 3 (\n"
                + "  subpartition p12sp1 values less than (500),\n"
                + "  subpartition p12sp2 values less than (1000),\n"
                + "  subpartition p12sp3 values less than (2000)\n"
                + ")\n"
                + ")";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(stmt, partitionType, null, sqlForNTP);
    }

    // ===================== helper functions ===================== //

    private String createTable(Statement stmt, PartitionType partitionType, boolean useTemplate)
        throws SQLException {
        String sql;
        String formatSql;
        String tokenHints = buildTokenHints();
        String tableName = "t_" + partitionType.name().toLowerCase() + (useTemplate ? "_template_" : "_not_template_")
            + System.currentTimeMillis();

        if (partitionType == PartitionType.Key_Key) {
            formatSql = useTemplate ? CREATE_KEY_KEY_TP_TABLE_SQL : CREATE_KEY_KEY_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Key_Hash) {
            formatSql = useTemplate ? CREATE_KEY_HASH_TP_TABLE_SQL : CREATE_KEY_HASH_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Key_List) {
            formatSql = useTemplate ? CREATE_KEY_LIST_TP_TABLE_SQL : CREATE_KEY_LIST_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Key_ListColumns) {
            formatSql = useTemplate ? CREATE_KEY_LIST_COLUMNS_TP_TABLE_SQL : CREATE_KEY_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Key_Range) {
            formatSql = useTemplate ? CREATE_KEY_RANGE_TP_TABLE_SQL : CREATE_KEY_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Key_RangeColumns) {
            formatSql = useTemplate ? CREATE_KEY_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_KEY_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == PartitionType.Hash_Key) {
            formatSql = useTemplate ? CREATE_HASH_KEY_TP_TABLE_SQL : CREATE_HASH_KEY_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Hash_Hash) {
            formatSql = useTemplate ? CREATE_HASH_HASH_TP_TABLE_SQL : CREATE_HASH_HASH_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Hash_List) {
            formatSql = useTemplate ? CREATE_HASH_LIST_TP_TABLE_SQL : CREATE_HASH_LIST_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Hash_ListColumns) {
            formatSql = useTemplate ? CREATE_HASH_LIST_COLUMNS_TP_TABLE_SQL : CREATE_HASH_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Hash_Range) {
            formatSql = useTemplate ? CREATE_HASH_RANGE_TP_TABLE_SQL : CREATE_HASH_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Hash_RangeColumns) {
            formatSql = useTemplate ? CREATE_HASH_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_HASH_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == PartitionType.List_Key) {
            formatSql = useTemplate ? CREATE_LIST_KEY_TP_TABLE_SQL : CREATE_LIST_KEY_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.List_Hash) {
            formatSql = useTemplate ? CREATE_LIST_HASH_TP_TABLE_SQL : CREATE_LIST_HASH_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.List_List) {
            formatSql = useTemplate ? CREATE_LIST_LIST_TP_TABLE_SQL : CREATE_LIST_LIST_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.List_ListColumns) {
            formatSql = useTemplate ? CREATE_LIST_LIST_COLUMNS_TP_TABLE_SQL : CREATE_LIST_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.List_Range) {
            formatSql = useTemplate ? CREATE_LIST_RANGE_TP_TABLE_SQL : CREATE_LIST_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.List_RangeColumns) {
            formatSql = useTemplate ? CREATE_LIST_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_LIST_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == PartitionType.ListColumns_Key) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_KEY_TP_TABLE_SQL : CREATE_LIST_COLUMNS_KEY_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.ListColumns_Hash) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_HASH_TP_TABLE_SQL : CREATE_LIST_COLUMNS_HASH_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.ListColumns_List) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_LIST_TP_TABLE_SQL : CREATE_LIST_COLUMNS_LIST_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.ListColumns_ListColumns) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL :
                CREATE_LIST_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.ListColumns_Range) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_RANGE_TP_TABLE_SQL : CREATE_LIST_COLUMNS_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.ListColumns_RangeColumns) {
            formatSql = useTemplate ? CREATE_LIST_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL :
                CREATE_LIST_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == PartitionType.Range_Key) {
            formatSql = useTemplate ? CREATE_RANGE_KEY_TP_TABLE_SQL : CREATE_RANGE_KEY_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Range_Hash) {
            formatSql = useTemplate ? CREATE_RANGE_HASH_TP_TABLE_SQL : CREATE_RANGE_HASH_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Range_List) {
            formatSql = useTemplate ? CREATE_RANGE_LIST_TP_TABLE_SQL : CREATE_RANGE_LIST_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Range_ListColumns) {
            formatSql = useTemplate ? CREATE_RANGE_LIST_COLUMNS_TP_TABLE_SQL : CREATE_RANGE_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Range_Range) {
            formatSql = useTemplate ? CREATE_RANGE_RANGE_TP_TABLE_SQL : CREATE_RANGE_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.Range_RangeColumns) {
            formatSql =
                useTemplate ? CREATE_RANGE_RANGE_COLUMNS_TP_TABLE_SQL : CREATE_RANGE_RANGE_COLUMNS_NTP_TABLE_SQL;

        } else if (partitionType == PartitionType.RangeColumns_Key) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_KEY_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_KEY_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.RangeColumns_Hash) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_HASH_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_HASH_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.RangeColumns_List) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_LIST_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_LIST_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.RangeColumns_ListColumns) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_LIST_COLUMNS_TP_TABLE_SQL :
                CREATE_RANGE_COLUMNS_LIST_COLUMNS_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.RangeColumns_Range) {
            formatSql =
                useTemplate ? CREATE_RANGE_COLUMNS_RANGE_TP_TABLE_SQL : CREATE_RANGE_COLUMNS_RANGE_NTP_TABLE_SQL;
        } else if (partitionType == PartitionType.RangeColumns_RangeColumns) {
            formatSql = useTemplate ? CREATE_RANGE_COLUMNS_RANGE_COLUMNS_TP_TABLE_SQL :
                CREATE_RANGE_COLUMNS_RANGE_COLUMNS_NTP_TABLE_SQL;
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        sql = tokenHints + String.format(formatSql, tableName);

        // 暂时指定唯一table group, 从而避免sub job导致的hints丢失问题
        String utg = ("tg_" + UUID.randomUUID()).replace("-", "_");
        stmt.execute("create tablegroup if not exists " + utg);
        sql = sql.substring(0, sql.length() - 1) + " tablegroup=" + utg;

        logger.info("create table sql:" + sql);
        stmt.execute(sql);
        // 打标的建表语句和传入的建表语句并不完全一样，此处只演示是否是create语句
        Assert.assertTrue(StringUtils.startsWith(getDdlRecordSql(tokenHints), tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());

        return tableName;
    }

    private void dropTable(Statement stmt, String tableName) throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + String.format("drop table %s", tableName);
        stmt.execute(sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());
    }

    private void alterPartition(Statement stmt, String formatSql, String tableName) throws SQLException {
        String tokenHints = buildTokenHints();
        formatSql = String.format(formatSql, "table");
        String sql = tokenHints + String.format(formatSql, tableName);
        logger.info("alter sql:" + sql);
        stmt.execute(sql);
        checkTableAfterAlterPartition(sql, tokenHints, tableName);
    }

    private void alterTableGroupPartition(Statement stmt, String formatSql, String tableName) throws SQLException {
        String tokenHints = buildTokenHints();
        formatSql = String.format(formatSql, "tablegroup");
        String tableGroupName = queryTableGroup(tableName);
        String sql = tokenHints + String.format(formatSql, tableGroupName);
        logger.info("alter table group sql:" + sql);
        int tableCount = queryTableCountInTableGroup(tableGroupName);
        stmt.execute(sql);
        checkTableGroupDdl(sql, tokenHints, tableName, tableCount);
    }

    private void checkTableAfterAlterPartition(String sql, String tokenHints, String tableName) throws SQLException {
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints == null ? sql : tokenHints));
        Assert.assertEquals(getServerId4Check(serverId),
            getDdlExtInfo(tokenHints == null ? sql : tokenHints).getServerId());
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints == null ? sql : tokenHints));
        Assert.assertEquals(getDdlRecordTopology(tokenHints == null ? sql : tokenHints, tableName),
            queryTopology(tableName));
    }

    private void checkTableGroupDdl(String sql, String tokenHints, String tableName, int tableCount)
        throws SQLException {
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(getServerId4Check(serverId), getDdlExtInfo(tokenHints).getServerId());
        Assert.assertEquals(tableCount, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(getDdlRecordTopology(tokenHints, tableName), queryTopology(tableName));
    }

    private String getMovePartitionSqlForTable(String tableName) throws SQLException {
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

    private String getMoveSubPartitionSqlForTable(String tableName) throws SQLException {
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

    private String queryTableGroup(String tableName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("show full tablegroup")) {
                while (rs.next()) {
                    String schemaName = rs.getString("TABLE_SCHEMA");
                    String tables = rs.getString("TABLES");
                    String tableGroup = rs.getString("TABLE_GROUP_NAME");
                    String[] tableNames = StringUtils.split(tables, ",");
                    List<String> tableList = Lists.newArrayList(tableNames);

                    if (dbName.equals(schemaName) && tableList.contains(tableName)) {
                        return tableGroup;
                    }
                }
            }
        }

        throw new RuntimeException("can`t find table group for table " + tableName);
    }

    private int queryTableCountInTableGroup(String tableGroupName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                "show tablegroup where table_group_name = '" + tableGroupName + "'")) {
                while (rs.next()) {
                    return rs.getInt("TABLE_COUNT");
                }
                return 0;
            }
        }
    }

    private void doAlterPartition(Statement stmt, PartitionType partitionType, String sqlForTP, String sqlForNTP)
        throws SQLException {
        String tableName;

        if (StringUtils.isNotEmpty(sqlForTP)) {
            tableName = createTable(stmt, partitionType, true);
            alterPartition(stmt, sqlForTP, tableName);
            tableName = createTable(stmt, partitionType, true);
            alterTableGroupPartition(stmt, sqlForTP, tableName);
        }

        if (StringUtils.isNotEmpty(sqlForNTP)) {
            tableName = createTable(stmt, partitionType, false);
            alterPartition(stmt, sqlForNTP, tableName);
            tableName = createTable(stmt, partitionType, false);
            alterTableGroupPartition(stmt, sqlForNTP, tableName);
        }
    }

    enum PartitionType {
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
