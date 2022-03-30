package com.alibaba.polardbx.executor.utils.transaction;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet.Transaction.FAKE_GROUP_FOR_DDL;
import static java.lang.Math.min;

/**
 * A parser that parses deadlock information
 *
 * @author wuzhe
 */
public class DeadlockParser {

    private static final Pattern DEADLOCK_LOG_PATTERN = Pattern.compile(
        "([-]+\\nLATEST DETECTED DEADLOCK.*?WE ROLL BACK TRANSACTION \\([0-9]+\\))", Pattern.DOTALL);

    private static final Pattern DB_TABLE_PATTERN = Pattern.compile(" of table `(.*)`\\.`(.*)` ");

    private static final Pattern DB_TABLE_PATTERN2 = Pattern.compile("`(.*)`\\.`(.*)`");

    public static final String NO_DEADLOCKS_DETECTED = "No deadlocks detected.";

    private static final String NO_PRIVILEGE =
        "The latest deadlock is detected in schemas or tables for which you have no privileges.";

    public static final String GLOBAL_DEADLOCK = "GLOBAL DEADLOCK";

    public static final String MDL_DEADLOCK = "MDL DEADLOCK";

    /**
     * Parse the status information get by "show engine innodb status", and only return the deadlock log.
     *
     * <p>
     * If no deadlock is detected, return "No deadlocks detected.".
     * <p>
     * If no privilege for this deadlock log, return
     * "The latest deadlock is detected in schemas or tables for which you have no privileges."
     * <p>
     * Otherwise, return the deadlock log by parsing the status information.
     *
     * @param status the status information get by "show engine innodb status"
     * @param executionContext used for privilege check
     * @return the deadlock log.
     */
    public static String parseLocalDeadlock(String status,
                                            ExecutionContext executionContext) {
        /*
        1. Use the following pattern to parse the {status} and get the deadlock log only:

            ------------------------
            LATEST DETECTED DEADLOCK
            ------------------------

            (deadlock information is presented here.)

            *** WE ROLL BACK TRANSACTION (x)
         */
        final Matcher matcher = DEADLOCK_LOG_PATTERN.matcher(status);
        String deadlockLog = matcher.find() ? matcher.group(1) : null;

        if (deadlockLog == null) {
            return NO_DEADLOCKS_DETECTED;
        }

        // Remove the connection string in the original deadlock log
        deadlockLog = removeConnectionString(deadlockLog);

        // Add a "\n" to make the deadlock log look better
        deadlockLog = "\n" + deadlockLog;

        /*
        2. Inject logical table in deadlock log,
        and collect all logical tables for privilege check
        */
        final Map<String, String> physicalToLogical = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        deadlockLog = injectLogicalTables(deadlockLog, physicalToLogical);

        /*
        3. Privilege check
        */
        return checkPrivilege(deadlockLog, physicalToLogical.values(), executionContext);
    }

    /**
     * inject information of logical tables into deadlock log
     *
     * @param deadlockLog contains only physical table information
     * @param physicalToLogical is updated in this method,
     * Map: `physical DB`.`physical table` -> `logical schema`.`logical table`
     */
    public static String injectLogicalTables(String deadlockLog, Map<String, String> physicalToLogical) {
        /*
        1. Extract all physical DBs and physical tables from the deadlock log:

        An example in the local/global deadlock log:
            index PRIMARY of table `db1_single`.`single1_fl4x` trx id xxx lock_mode X
        An example in the MDL deadlock log:
            metadata lock of table `db1_000001`.`tb1_u9bk` lock type SHARED_WRITE

        So we use the pattern " of table `(.*)`\\.`(.*)`" to extract it
         */
        // Map: physical db -> logical tables
        final Map<String, Set<String>> physicalTableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Matcher matcher = DB_TABLE_PATTERN.matcher(deadlockLog);
        while (matcher.find()) {
            // There has to be only two groups,
            // one is {physical DB name}, the other is {physical table name}
            if (matcher.groupCount() == 2) {
                final String physicalDb = matcher.group(1);
                final String physicalTable = matcher.group(2);
                Set<String> physicalTables = physicalTableMap
                    .computeIfAbsent(physicalDb, k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
                if (SystemTables.contains(physicalTable)) {
                    // If deadlock happens on system tables, do not show the log to users
                    return NO_DEADLOCKS_DETECTED;
                } else {
                    physicalTables.add(physicalTable);
                }
            }
        }

        /*
        2. For each physical DB and table, we first find the corresponding schema and logical table,
        then we inject the schema name and logical table name into the original deadlock log
         */
        // Convert `physical db`.`physical tb` to `logical schema`.`logical table`
        TransactionUtils.convertPhysicalToLogical(physicalTableMap, physicalToLogical);
        for (Map.Entry<String, String> physicalAndLogical : physicalToLogical.entrySet()) {
            // Inject the logical table into the deadlock log
            final String physicalAndLogicalString =
                String.format("%s(%s)", physicalAndLogical.getKey(), physicalAndLogical.getValue());
            deadlockLog = deadlockLog.replaceAll(physicalAndLogical.getKey(), physicalAndLogicalString);
        }

        return deadlockLog;
    }

    /**
     * Parse the transactions causing the deadlock, and return a simple deadlock log and a full deadlock log.
     * <p>
     * We do not perform a privilege check here as local deadlock does since this method is called
     * when a deadlock is detected rather than when a SHOW GLOBAL DEADLOCKS statement is called.
     *
     * @param transactions transactions causing the deadlock
     * @return Pair(simple deadlock log printer, full deadlock log printer)
     */
    public static Pair<StringBuilder, StringBuilder> parseGlobalDeadlock(List<TrxLookupSet.Transaction> transactions) {
        final StringBuilder simpleLog = new StringBuilder();
        final StringBuilder fullLog = new StringBuilder();

        // Print deadlock header
        fullLog.append(String.format("%s, %s involved transactions\n",
            new Timestamp(System.currentTimeMillis()), transactions.size()));
        fullLog.append("------------------------\n")
            .append("LATEST DETECTED DEADLOCK\n")
            .append("------------------------\n");

        // Print simple deadlock header
        simpleLog.append("Deadlock detected: ");

        for (int i = 0; i < transactions.size(); i++) {
            final TrxLookupSet.Transaction trx = transactions.get(i);

            // Print transaction info for full deadlock log
            printTransactionInfo(trx, i, false, fullLog);

            // Print simple deadlock log
            final String originalSql = trx.getSql();
            // Simple log will be flushed to disk, so make it small
            final String simpleSql =
                (originalSql == null) ? "" : originalSql.substring(0, min(128, originalSql.length()));
            simpleLog.append("trxId:")
                .append(trx.getTransactionId())
                .append("#sql:\"")
                .append(simpleSql)
                .append("\"");

            if (i < transactions.size() - 1) {
                // Append a "waiting" in the end except for the last transaction
                simpleLog.append(" waiting ");
            }
        }

        return new Pair<>(simpleLog, fullLog);
    }

    /**
     * Parse the transactions/DDL causing the MDL deadlock, and return a simple deadlock log and a full deadlock log.
     * <p>
     * We do not perform a privilege check here as local deadlock does since this method is called
     * when an MDL deadlock is detected rather than when a SHOW GLOBAL DEADLOCKS statement is called.
     *
     * @param transactions transactions causing the deadlock
     * @return Pair(simple deadlock log printer, full deadlock log printer)
     */
    public static Pair<StringBuilder, StringBuilder> parseMdlDeadlock(List<TrxLookupSet.Transaction> transactions) {
        final StringBuilder simpleLog = new StringBuilder();
        final StringBuilder fullLog = new StringBuilder();

        // Print deadlock header
        fullLog.append(String.format("%s, %s involved transactions/DDLs\n",
            new Timestamp(System.currentTimeMillis()), transactions.size()));
        fullLog.append("-----------------------------\n")
            .append("LATEST DETECTED MDL DEADLOCK\n")
            .append("-----------------------------\n");
        simpleLog.append("MDL Deadlock detected: ");

        for (int i = 0; i < transactions.size(); i++) {
            final TrxLookupSet.Transaction trx = transactions.get(i);

            // Print transaction/ddl info for full deadlock log
            if (trx.isDdl()) {
                // For ddl statement
                printDdlInfo(trx, i, fullLog);
            } else {
                // For normal transaction
                printTransactionInfo(trx, i, true, fullLog);
            }

            // Print simple deadlock log
            final String originalSql =
                trx.isDdl() ? trx.getLocalTransaction(FAKE_GROUP_FOR_DDL).getPhysicalSql() : trx.getSql();
            // Simple log will be flushed to disk, so we make it small
            final String simpleSql =
                (originalSql == null) ? "" : originalSql.substring(0, min(128, originalSql.length()));
            simpleLog.append(trx.isDdl() ? "ddlMysqlPid:" : "trxId:")
                .append(trx.getTransactionId())
                .append("#sql:\"")
                .append(simpleSql)
                .append("\"");

            if (i < transactions.size() - 1) {
                // Append a "waiting" in the end except for the last transaction
                simpleLog.append(" waiting ");
            }
        }

        return new Pair<>(simpleLog, fullLog);
    }

    /**
     * print transaction information
     *
     * @param trx the transaction to be printed
     * @param i trx is the ith transaction to be printed
     * @param isMdl is true if printing information for MDL deadlock
     * @param sb is the printer
     */
    private static void printTransactionInfo(TrxLookupSet.Transaction trx,
                                             int i,
                                             boolean isMdl,
                                             StringBuilder sb) {
        // Print global transaction info
        sb.append(String.format("*** (%s) GLOBAL TRANSACTION:\n", i + 1));
        final String startTime =
            (trx.getStartTime() == null) ? "unknown" : new Timestamp(trx.getStartTime()).toString();
        sb.append(String.format("TRANSACTION %s, front end connection id %s, start from %s, executing SQL:\n%s\n",
            Long.toHexString(trx.getTransactionId()), trx.getFrontendConnId(), startTime, trx.getSql()));
        sb.append("\n");

        // Print each local transaction
        final Collection<LocalTransaction> localTransactions = trx.getAllLocalTransactions();
        final Iterator<LocalTransaction> iterator = localTransactions.iterator();
        for (int j = 0; j < localTransactions.size(); j++) {
            final LocalTransaction localTransaction = iterator.next();
            sb.append(String.format("*** (%s.%s) LOCAL TRANSACTION:\n", i + 1, j + 1));
            sb.append(String.format("execute on group %s, ", localTransaction.getGroup()));
            if (!isMdl) {
                sb.append(String.format("operation state %s\n", localTransaction.getOperationState()));
                sb.append(String.format("tables in use %s, locked %s\n",
                    localTransaction.getTablesInUse(), localTransaction.getTablesLocked()));
                if ("LOCK WAIT".equalsIgnoreCase(localTransaction.getState())) {
                    sb.append("LOCK WAIT ");
                }
                sb.append(String.format("%s lock struct(s), heap size %s, %s row lock(s), ",
                    localTransaction.getLockStructs(),
                    localTransaction.getHeapSize(),
                    localTransaction.getRowLocks()));
            }
            // Remove the connection string of DN in the physical SQL
            final String physicalSql = removeConnectionString(localTransaction.getPhysicalSql());
            sb.append(String.format("executing SQL:\n%s\n", physicalSql));
            sb.append("\n");

            // Print info of holding locks
            final Set<TrxLock> holdingLocks = localTransaction.getHoldingTrxLocks();
            if (CollectionUtils.isNotEmpty(holdingLocks)) {
                sb.append(String.format("*** (%s.%s) HOLDS THE LOCK(S):\n", i + 1, j + 1));
                for (TrxLock holdingLock : holdingLocks) {
                    printLockInfo(holdingLock, trx.getTransactionId(), isMdl, sb);
                }
                sb.append("\n");
            }

            // Print info of the waiting lock
            final TrxLock waitingLock = localTransaction.getWaitingTrxLock();
            if (null != waitingLock) {
                sb.append(String.format("*** (%s.%s) WAITING FOR THIS LOCK TO BE GRANTED:\n", i + 1, j + 1));
                printLockInfo(waitingLock, trx.getTransactionId(), isMdl, sb);
                sb.append("\n");
            }
        }
    }

    /**
     * print ddl information
     *
     * @param ddl the ddl to be printed
     * @param i ddl is the ith transaction/ddl to be printed
     * @param sb is the printer
     */
    private static void printDdlInfo(TrxLookupSet.Transaction ddl,
                                     int i,
                                     StringBuilder sb) {
        sb.append(String.format("*** (%s) DDL:\n", i + 1));
        final String startTime =
            (ddl.getStartTime() == null) ? "unknown" : new Timestamp(ddl.getStartTime()).toString();
        sb.append(String.format("start from %s\n", startTime));
        sb.append("\n");

        // It should have only one local transaction, i.e., the DDL statement
        final LocalTransaction ddlTrx = ddl.getLocalTransaction(FAKE_GROUP_FOR_DDL);
        sb.append(String.format("*** (%s.%s) PHYSICAL DDL:\n", i + 1, 1));
        // Remove the connection string of DN in the physical SQL
        final String physicalSql = removeConnectionString(ddlTrx.getPhysicalSql());
        sb.append(String.format("executing SQL:\n%s\n", physicalSql));
        sb.append("\n");

        // Print holding locks info
        final Set<TrxLock> holdingLocks = ddlTrx.getHoldingTrxLocks();
        if (CollectionUtils.isNotEmpty(holdingLocks)) {
            sb.append(String.format("*** (%s.%s) HOLDS THE LOCK(S):\n", i + 1, 1));
            for (TrxLock holdingLock : holdingLocks) {
                printLockInfo(holdingLock, ddl.getTransactionId(), true, sb);
            }
            sb.append("\n");
        }

        // Print the waiting lock info
        final TrxLock waitingLock = ddlTrx.getWaitingTrxLock();
        if (null != waitingLock) {
            sb.append(String.format("*** (%s.%s) WAITING FOR THIS LOCK TO BE GRANTED:\n", i + 1, 1));
            printLockInfo(waitingLock, ddl.getTransactionId(), true, sb);
            sb.append("\n");
        }
    }

    /**
     * print lock information
     *
     * @param lock is the lock to be printed
     * @param trxId is used for print lock info
     * @param isMdl is true if this lock is a metadata lock
     * @param sb is the printer
     */
    private static void printLockInfo(TrxLock lock,
                                      Long trxId,
                                      boolean isMdl,
                                      StringBuilder sb) {
        if (null == lock) {
            return;
        }

        final String physicalDbAndTb = lock.getPhysicalTable();

        if (isMdl) {
            // For metadata lock
            sb.append(String.format("metadata lock of table %s lock type %s\n", physicalDbAndTb, lock.getType()));
        } else {
            // For normal transaction lock
            sb.append(String
                .format("%s LOCKS space id %s page no %s index %s of table %s trx id %s lock_mode %s\n",
                    lock.getType(),
                    lock.getSpace(),
                    lock.getPage(),
                    lock.getIndex(),
                    physicalDbAndTb,
                    Long.toHexString(trxId),
                    lock.getMode()));
            sb.append(String.format("lock_data: %s\n", lock.getData()));
        }
    }

    /**
     * call me when handling SHOW GLOBAL/LOCAL DEADLOCKS
     *
     * @param deadlockLog is the original deadlock log
     * @param logicalDbAndTables is the logical schemas and tables involved in the deadlock
     * @param ec is used for privilege check
     * @return deadlock log after checking privilege
     */
    public static String checkPrivilege(String deadlockLog,
                                        Collection<String> logicalDbAndTables,
                                        ExecutionContext ec) {
        // Skip privilege check when no deadlocks have been detected
        if (NO_DEADLOCKS_DETECTED.equalsIgnoreCase(deadlockLog)) {
            return deadlockLog;
        }

        for (String logicalDbAndTable : logicalDbAndTables) {
            final Matcher matcher = DB_TABLE_PATTERN2.matcher(logicalDbAndTable);
            if (matcher.find()) {
                // There has to be only two groups,
                // one is {logical schema name}, the other is {logical table name}
                if (matcher.groupCount() == 2) {
                    final String schema = matcher.group(1);
                    final String logicalTable = matcher.group(2);
                    if (CanAccessTable.verifyPrivileges(schema, logicalTable, ec)) {
                        // If one of the table involved in the deadlock can be accessed,
                        // then the deadlock log can be accessed
                        return deadlockLog;
                    }
                }
            }
        }

        // None of the tables can be accessed, and a NO_PRIVILEGE message is returned
        return NO_PRIVILEGE;
    }

    /**
     * The original deadlock log may contain the connection string of DN,
     * which looks like "\/*DRDS /127.0.0.1/13147a578b000000-2/0// *\/".
     * We want to remove "DRDS /127.0.0.1/".
     */
    private static String removeConnectionString(String originalText) {
        if (null == originalText) {
            return null;
        }
        return originalText.replaceAll("DRDS /[0-9]{0,3}\\.[0-9]{0,3}\\.[0-9]{0,3}\\.[0-9]{0,3}", "");
    }
}
