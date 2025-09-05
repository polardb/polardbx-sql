/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public enum SqlSpecialIdentifier {

    AUTHORS, BINLOG, BLOCK, CODE, COLLATION, COLUMNS, FIELDS, COMMITTED, CONTEXT, CONTRIBUTORS, COUNT, CPU, ENGINE,
    ENGINES, ERRORS, EVENT, EVENTS, FULL, FUNCTION, GLOBAL, GRANTS, HOSTS, INDEXES, INNODB, IPC, ISOLATION, LOCAL,
    MASTER, MEMORY, MUTEX, NAMES, OPEN, PAGE, PERFORMANCE_SCHEMA, PLUGINS, POLICY, PRIVILEGES, PROCESSLIST, PROFILE,
    PROFILES, REPEATABLE, SERIALIZABLE, SESSION, SLAVE, SOURCE, STATUS, STORAGE, SWAPS, TABLES, TRANSACTION, TRIGGERS,
    UNCOMMITTED, VARIABLES, VIEW, WARNINGS, SEQUENCES, TOPOLOGY, PARITIONS, BROADCASTS, RULE, TRACE, DATASOURCES,
    CHARSET, PARTITIONS, PREPARE, EXECUTE, DEALLOCATE, DBLOCK, OFF, PHYSICAL_SLOW, STATS, ONLY, STC, HTC, HIS, DS, DDL,
    VERSION, PHYSICAL_PROCESSLIST, DB, OUTLINES, BINARY, LOGS, CHARACTER, SET, FROM, PROCEDURE, FOR, QUERY, LIMIT,
    OFFSET, CHANGESET, LOCALITY, PHYSICAL_DDL,
    RELAYLOG, IN, TABLE, CREATE, DATABASE, IF, NOT, EXISTS, TRIGGER, KEYS, SLOW, RECYCLEBIN, DATABASES, CCL_RULE, INFO,
    CONVERT, WITH, STREAMS, CDC, DUMP, CHECK_TABLEGROUP;
    private static final Map<String, SqlSpecialIdentifier> specialIdentifiers = new HashMap<>();

    static {
        specialIdentifiers.put("PHYSICAL_SLOW", SqlSpecialIdentifier.PHYSICAL_SLOW);

        specialIdentifiers.put("AUTHORS", SqlSpecialIdentifier.AUTHORS);
        specialIdentifiers.put("BINLOG", SqlSpecialIdentifier.BINLOG);
        specialIdentifiers.put("COLLATION", SqlSpecialIdentifier.COLLATION);
        specialIdentifiers.put("COLUMNS", SqlSpecialIdentifier.COLUMNS);
        specialIdentifiers.put("CHANGESET", SqlSpecialIdentifier.CHANGESET);
        specialIdentifiers.put("PARTITIONS", SqlSpecialIdentifier.PARTITIONS);
        specialIdentifiers.put("FIELDS", SqlSpecialIdentifier.FIELDS);
        specialIdentifiers.put("CONTRIBUTORS", SqlSpecialIdentifier.CONTRIBUTORS);
        specialIdentifiers.put("EVENT", SqlSpecialIdentifier.EVENT);
        specialIdentifiers.put("FUNCTION", SqlSpecialIdentifier.FUNCTION);
        specialIdentifiers.put("VIEW", SqlSpecialIdentifier.VIEW);
        specialIdentifiers.put("ENGINE", SqlSpecialIdentifier.ENGINE);
        specialIdentifiers.put("ENGINES", SqlSpecialIdentifier.ENGINES);
        specialIdentifiers.put("ERRORS", SqlSpecialIdentifier.ERRORS);
        specialIdentifiers.put("EVENTS", SqlSpecialIdentifier.EVENTS);
        specialIdentifiers.put("FULL", SqlSpecialIdentifier.FULL);
        specialIdentifiers.put("GLOBAL", SqlSpecialIdentifier.GLOBAL);
        specialIdentifiers.put("GRANTS", SqlSpecialIdentifier.GRANTS);
        specialIdentifiers.put("MASTER", SqlSpecialIdentifier.MASTER);
        specialIdentifiers.put("OPEN", SqlSpecialIdentifier.OPEN);
        specialIdentifiers.put("PLUGINS", SqlSpecialIdentifier.PLUGINS);
        specialIdentifiers.put("CODE", SqlSpecialIdentifier.CODE);
        specialIdentifiers.put("STATUS", SqlSpecialIdentifier.STATUS);
        specialIdentifiers.put("PRIVILEGES", SqlSpecialIdentifier.PRIVILEGES);
        specialIdentifiers.put("PROCESSLIST", SqlSpecialIdentifier.PROCESSLIST);
        specialIdentifiers.put("PHYSICAL_PROCESSLIST", SqlSpecialIdentifier.PHYSICAL_PROCESSLIST);
        specialIdentifiers.put("DBLOCK", SqlSpecialIdentifier.DBLOCK);
        specialIdentifiers.put("PROFILE", SqlSpecialIdentifier.PROFILE);
        specialIdentifiers.put("PROFILES", SqlSpecialIdentifier.PROFILES);
        specialIdentifiers.put("SESSION", SqlSpecialIdentifier.SESSION);
        specialIdentifiers.put("SLAVE", SqlSpecialIdentifier.SLAVE);
        specialIdentifiers.put("STORAGE", SqlSpecialIdentifier.STORAGE);
        specialIdentifiers.put("TABLES", SqlSpecialIdentifier.TABLES);
        specialIdentifiers.put("TRIGGERS", SqlSpecialIdentifier.TRIGGERS);
        specialIdentifiers.put("VARIABLES", SqlSpecialIdentifier.VARIABLES);
        specialIdentifiers.put("WARNINGS", SqlSpecialIdentifier.WARNINGS);
        specialIdentifiers.put("INNODB", SqlSpecialIdentifier.INNODB);
        specialIdentifiers.put("PERFORMANCE_SCHEMA", SqlSpecialIdentifier.PERFORMANCE_SCHEMA);
        specialIdentifiers.put("MUTEX", SqlSpecialIdentifier.MUTEX);
        specialIdentifiers.put("COUNT", SqlSpecialIdentifier.COUNT);
        specialIdentifiers.put("BLOCK", SqlSpecialIdentifier.BLOCK);
        specialIdentifiers.put("CONTEXT", SqlSpecialIdentifier.CONTEXT);
        specialIdentifiers.put("CPU", SqlSpecialIdentifier.CPU);
        specialIdentifiers.put("MEMORY", SqlSpecialIdentifier.MEMORY);
        specialIdentifiers.put("PAGE", SqlSpecialIdentifier.PAGE);
        specialIdentifiers.put("SOURCE", SqlSpecialIdentifier.SOURCE);
        specialIdentifiers.put("SWAPS", SqlSpecialIdentifier.SWAPS);
        specialIdentifiers.put("IPC", SqlSpecialIdentifier.IPC);
        specialIdentifiers.put("LOCAL", SqlSpecialIdentifier.LOCAL);
        specialIdentifiers.put("LOCALITY", SqlSpecialIdentifier.LOCALITY);
        specialIdentifiers.put("PHYSICAL_DDL", SqlSpecialIdentifier.PHYSICAL_DDL);
        specialIdentifiers.put("HOSTS", SqlSpecialIdentifier.HOSTS);
        specialIdentifiers.put("INDEXES", SqlSpecialIdentifier.INDEXES);
        specialIdentifiers.put("TRANSACTION", SqlSpecialIdentifier.TRANSACTION);
        specialIdentifiers.put("UNCOMMITTED", SqlSpecialIdentifier.UNCOMMITTED);
        specialIdentifiers.put("COMMITTED", SqlSpecialIdentifier.COMMITTED);
        specialIdentifiers.put("REPEATABLE", SqlSpecialIdentifier.REPEATABLE);
        specialIdentifiers.put("SERIALIZABLE", SqlSpecialIdentifier.SERIALIZABLE);
        specialIdentifiers.put("NAMES", SqlSpecialIdentifier.NAMES);
        specialIdentifiers.put("SEQUENCES", SqlSpecialIdentifier.SEQUENCES);
        specialIdentifiers.put("TOPOLOGY", SqlSpecialIdentifier.TOPOLOGY);
        specialIdentifiers.put("BROADCASTS", SqlSpecialIdentifier.BROADCASTS);
        specialIdentifiers.put("RULE", SqlSpecialIdentifier.RULE);
        specialIdentifiers.put("TRACE", SqlSpecialIdentifier.TRACE);
        specialIdentifiers.put("DATASOURCES", SqlSpecialIdentifier.DATASOURCES);
        specialIdentifiers.put("CHARSET", SqlSpecialIdentifier.CHARSET);
        specialIdentifiers.put("ISOLATION", SqlSpecialIdentifier.ISOLATION);
        specialIdentifiers.put("POLICY", SqlSpecialIdentifier.POLICY);
        specialIdentifiers.put("PREPARE", SqlSpecialIdentifier.PREPARE);
        specialIdentifiers.put("EXECUTE", SqlSpecialIdentifier.EXECUTE);
        specialIdentifiers.put("DEALLOCATE", SqlSpecialIdentifier.DEALLOCATE);
        specialIdentifiers.put("OFF", SqlSpecialIdentifier.OFF);
        specialIdentifiers.put("STATS", SqlSpecialIdentifier.STATS);
        specialIdentifiers.put("ONLY", SqlSpecialIdentifier.ONLY);
        specialIdentifiers.put("STC", SqlSpecialIdentifier.STC);
        specialIdentifiers.put("HTC", SqlSpecialIdentifier.HTC);
        specialIdentifiers.put("HIS", SqlSpecialIdentifier.HIS);
        specialIdentifiers.put("DS", SqlSpecialIdentifier.DS);
        specialIdentifiers.put("DDL", SqlSpecialIdentifier.DDL);
        specialIdentifiers.put("VERSION", SqlSpecialIdentifier.VERSION);
        specialIdentifiers.put("DB", SqlSpecialIdentifier.DB);
        specialIdentifiers.put("OUTLINES", SqlSpecialIdentifier.OUTLINES);
        specialIdentifiers.put("BINARY", SqlSpecialIdentifier.BINARY);
        specialIdentifiers.put("LOGS", SqlSpecialIdentifier.LOGS);
        specialIdentifiers.put("CHARACTER", SqlSpecialIdentifier.CHARACTER);
        specialIdentifiers.put("SET", SqlSpecialIdentifier.SET);
        specialIdentifiers.put("FROM", SqlSpecialIdentifier.FROM);
        specialIdentifiers.put("PROCEDURE", SqlSpecialIdentifier.PROCEDURE);
        specialIdentifiers.put("FOR", SqlSpecialIdentifier.FOR);
        specialIdentifiers.put("QUERY", SqlSpecialIdentifier.QUERY);
        specialIdentifiers.put("LIMIT", SqlSpecialIdentifier.LIMIT);
        specialIdentifiers.put("OFFSET", SqlSpecialIdentifier.OFFSET);
        specialIdentifiers.put("RELAYLOG", SqlSpecialIdentifier.RELAYLOG);
        specialIdentifiers.put("IN", SqlSpecialIdentifier.IN);
        specialIdentifiers.put("TABLE", SqlSpecialIdentifier.TABLE);
        specialIdentifiers.put("CREATE", SqlSpecialIdentifier.CREATE);
        specialIdentifiers.put("DATABASE", SqlSpecialIdentifier.DATABASE);
        specialIdentifiers.put("IF", SqlSpecialIdentifier.IF);
        specialIdentifiers.put("NOT", SqlSpecialIdentifier.NOT);
        specialIdentifiers.put("EXISTS", SqlSpecialIdentifier.EXISTS);
        specialIdentifiers.put("TRIGGER", SqlSpecialIdentifier.TRIGGER);
        specialIdentifiers.put("KEYS", SqlSpecialIdentifier.KEYS);
        specialIdentifiers.put("SLOW", SqlSpecialIdentifier.SLOW);
        specialIdentifiers.put("RECYCLEBIN", SqlSpecialIdentifier.RECYCLEBIN);
        specialIdentifiers.put("DATABASES", SqlSpecialIdentifier.DATABASES);
        specialIdentifiers.put("CCL_RULE", SqlSpecialIdentifier.CCL_RULE);
        specialIdentifiers.put("INFO", SqlSpecialIdentifier.INFO);
        specialIdentifiers.put("CONVERT", SqlSpecialIdentifier.CONVERT);
        specialIdentifiers.put("CDC", SqlSpecialIdentifier.CDC);
        specialIdentifiers.put("DUMP", SqlSpecialIdentifier.DUMP);
    }

    public static SqlSpecialIdentifier get(String value) {
        if (null == value) {
            return null;
        }

        return specialIdentifiers.get(value);
    }
}
