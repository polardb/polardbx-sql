package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@NotThreadSafe
@RunWith(Parameterized.class)
public class AlterTableCompatShuffleTest extends AlterTableCompatBaseTest {

    private String tableName;
    protected String createTable;
    protected String alterTable;
    protected String error;

    public AlterTableCompatShuffleTest(String createTable, String alterTable, String error) {
        this.createTable = createTable;
        this.alterTable = alterTable;
        this.error = error;
    }

    @Parameterized.Parameters(name = "{index}: create={0}, alter={1}, error={2}")
    public static List<String[]> initParameters() {
        String[][] statements = {
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column a int after b, modify column b int after c,  modify column c int after d, modify column d int  after a",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column a int after b, modify column b int after c, modify column d int  after a,  modify column c int after d",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column a int after b,  modify column c int after d, modify column b int after c, modify column d int  after a",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column a int after b,  modify column c int after d, modify column d int  after a, modify column b int after c",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column a int after b, modify column d int  after a, modify column b int after c,  modify column c int after d",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column a int after b, modify column d int  after a,  modify column c int after d, modify column b int after c",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column b int after c, modify column a int after b,  modify column c int after d, modify column d int  after a",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column b int after c, modify column a int after b, modify column d int  after a,  modify column c int after d",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column b int after c,  modify column c int after d, modify column a int after b, modify column d int  after a",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column b int after c,  modify column c int after d, modify column d int  after a, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column b int after c, modify column d int  after a, modify column a int after b,  modify column c int after d",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column b int after c, modify column d int  after a,  modify column c int after d, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s  modify column c int after d, modify column a int after b, modify column b int after c, modify column d int  after a",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s  modify column c int after d, modify column a int after b, modify column d int  after a, modify column b int after c",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s  modify column c int after d, modify column b int after c, modify column a int after b, modify column d int  after a",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s  modify column c int after d, modify column b int after c, modify column d int  after a, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s  modify column c int after d, modify column d int  after a, modify column a int after b, modify column b int after c",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s  modify column c int after d, modify column d int  after a, modify column b int after c, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column d int  after a, modify column a int after b, modify column b int after c,  modify column c int after d",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column d int  after a, modify column a int after b,  modify column c int after d, modify column b int after c",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column d int  after a, modify column b int after c, modify column a int after b,  modify column c int after d",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column d int  after a, modify column b int after c,  modify column c int after d, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column d int  after a,  modify column c int after d, modify column a int after b, modify column b int after c",
                "OK"
            },
            {
                "create table %s(a int, b int, c int, d int)",
                "alter table %s modify column d int  after a,  modify column c int after d, modify column b int after c, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s modify column a int after b, drop column b, change column c b int, add column c int",
                "OK"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s modify column a int after b, drop column b, add column c int, change column c b int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s modify column a int after b, change column c b int, drop column b, add column c int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s modify column a int after b, change column c b int, add column c int, drop column b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s modify column a int after b, add column c int, drop column b, change column c b int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s modify column a int after b, add column c int, change column c b int, drop column b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s drop column b, modify column a int after b, change column c b int, add column c int",
                "Unknown column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s drop column b, modify column a int after b, add column c int, change column c b int",
                "Unknown column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s drop column b, change column c b int, modify column a int after b, add column c int",
                "OK"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s drop column b, change column c b int, add column c int, modify column a int after b",
                "OK"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s drop column b, add column c int, modify column a int after b, change column c b int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s drop column b, add column c int, change column c b int, modify column a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c b int, modify column a int after b, drop column b, add column c int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c b int, modify column a int after b, add column c int, drop column b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c b int, drop column b, modify column a int after b, add column c int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c b int, drop column b, add column c int, modify column a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c b int, add column c int, modify column a int after b, drop column b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c b int, add column c int, drop column b, modify column a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column c int, modify column a int after b, drop column b, change column c b int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column c int, modify column a int after b, change column c b int, drop column b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column c int, drop column b, modify column a int after b, change column c b int",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column c int, drop column b, change column c b int, modify column a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column c int, change column c b int, modify column a int after b, drop column b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column c int, change column c b int, drop column b, modify column a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column a b int after c, change column b c double after d, change column c d varchar(10) after a, change column d a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column a b int after c, change column b c double after d, change column d a int after b, change column c d varchar(10) after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column a b int after c, change column c d varchar(10) after a, change column b c double after d, change column d a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column a b int after c, change column c d varchar(10) after a, change column d a int after b, change column b c double after d",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column a b int after c, change column d a int after b, change column b c double after d, change column c d varchar(10) after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column a b int after c, change column d a int after b, change column c d varchar(10) after a, change column b c double after d",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column b c double after d, change column a b int after c, change column c d varchar(10) after a, change column d a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column b c double after d, change column a b int after c, change column d a int after b, change column c d varchar(10) after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column b c double after d, change column c d varchar(10) after a, change column a b int after c, change column d a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column b c double after d, change column c d varchar(10) after a, change column d a int after b, change column a b int after c",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column b c double after d, change column d a int after b, change column a b int after c, change column c d varchar(10) after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column b c double after d, change column d a int after b, change column c d varchar(10) after a, change column a b int after c",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c d varchar(10) after a, change column a b int after c, change column b c double after d, change column d a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c d varchar(10) after a, change column a b int after c, change column d a int after b, change column b c double after d",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c d varchar(10) after a, change column b c double after d, change column a b int after c, change column d a int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c d varchar(10) after a, change column b c double after d, change column d a int after b, change column a b int after c",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c d varchar(10) after a, change column d a int after b, change column a b int after c, change column b c double after d",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column c d varchar(10) after a, change column d a int after b, change column b c double after d, change column a b int after c",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column d a int after b, change column a b int after c, change column b c double after d, change column c d varchar(10) after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column d a int after b, change column a b int after c, change column c d varchar(10) after a, change column b c double after d",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column d a int after b, change column b c double after d, change column a b int after c, change column c d varchar(10) after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column d a int after b, change column b c double after d, change column c d varchar(10) after a, change column a b int after c",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column d a int after b, change column c d varchar(10) after a, change column a b int after c, change column b c double after d",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s change column d a int after b, change column c d varchar(10) after a, change column b c double after d, change column a b int after c",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column a int, add column b double, change column a x int after a, change column b y int after b",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column a int, add column b double, change column b y int after b, change column a x int after a",
                "Duplicate column"
            },
            {
                "create table %s(a int, b double, c varchar(10), d bigint)",
                "alter table %s add column a int, change column a x int after a, add column b double, change column b y int after b",
                "Duplicate column"
            }
        };
        return Arrays.asList(statements);
    }

    @Before
    public void init() {
        this.tableName = randomTableName("test_compat", 4);
    }

    @Test
    public void testCreateAndAlterTable() {
        executeAndCheck(createTable, tableName);
        if (error.equalsIgnoreCase("OK")) {
            executeAndCheck(alterTable, tableName);
        } else {
            executeAndFail(alterTable, tableName, error);
        }
        dropTableIfExists(tableName);
    }

}
