/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Enumerates the possible types of {@link SqlNode}.
 *
 * <p>The values are immutable, canonical constants, so you can use Kinds to
 * find particular types of expressions quickly. To identity a call to a common
 * operator such as '=', use {@link org.apache.calcite.sql.SqlNode#isA}:</p>
 *
 * <blockquote>
 * exp.{@link org.apache.calcite.sql.SqlNode#isA isA}({@link #EQUALS})
 * </blockquote>
 *
 * <p>Only commonly-used nodes have their own type; other nodes are of type
 * {@link #OTHER}. Some of the values, such as {@link #SET_QUERY}, represent
 * aggregates.</p>
 *
 * <p>To quickly choose between a number of options, use a switch statement:</p>
 *
 * <blockquote>
 * <pre>switch (exp.getKind()) {
 * case {@link #EQUALS}:
 *     ...;
 * case {@link #NOT_EQUALS}:
 *     ...;
 * default:
 *     throw new AssertionError("unexpected");
 * }</pre>
 * </blockquote>
 *
 * <p>Note that we do not even have to check that a {@code SqlNode} is a
 * {@link SqlCall}.</p>
 *
 * <p>To identify a category of expressions, use {@code SqlNode.isA} with
 * an aggregate SqlKind. The following expression will return <code>true</code>
 * for calls to '=' and '&gt;=', but <code>false</code> for the constant '5', or
 * a call to '+':</p>
 *
 * <blockquote>
 * <pre>exp.isA({@link #COMPARISON SqlKind.COMPARISON})</pre>
 * </blockquote>
 *
 * <p>RexNode also has a {@code getKind} method; {@code SqlKind} values are
 * preserved during translation from {@code SqlNode} to {@code RexNode}, where
 * applicable.</p>
 *
 * <p>There is no water-tight definition of "common", but that's OK. There will
 * always be operators that don't have their own kind, and for these we use the
 * {@code SqlOperator}. But for really the common ones, e.g. the many places
 * where we are looking for {@code AND}, {@code OR} and {@code EQUALS}, the enum
 * helps.</p>
 *
 * <p>(If we were using Scala, {@link SqlOperator} would be a case
 * class, and we wouldn't need {@code SqlKind}. But we're not.)</p>
 */
public enum SqlKind {
    //~ Static fields/initializers ---------------------------------------------

    // the basics

    /**
     * Expression not covered by any other {@link SqlKind} value.
     *
     * @see #OTHER_FUNCTION
     */
    OTHER,

    /**
     * SELECT statement or sub-query.
     */
    SELECT,

    /**
     * JOIN operator or compound FROM clause.
     *
     * <p>A FROM clause with more than one table is represented as if it were a
     * join. For example, "FROM x, y, z" is represented as
     * "JOIN(x, JOIN(x, y))".</p>
     */
    JOIN,

    /**
     * Identifier
     */
    IDENTIFIER,

    /**
     * A literal.
     */
    LITERAL,

    /**
     * Function that is not a special function.
     *
     * @see #FUNCTION
     */
    OTHER_FUNCTION,

    /**
     * EXPLAIN statement
     */
    EXPLAIN,

    /**
     * DESCRIBE SCHEMA statement
     */
    DESCRIBE_SCHEMA,

    /**
     * DESCRIBE TABLE statement
     */
    DESCRIBE_TABLE,

    /**
     * INSERT statement
     */
    INSERT,

    /**
     * REPLACE statement
     */
    REPLACE,

    /**
     * DELETE statement
     */
    DELETE,

    /**
     * UPDATE statement
     */
    UPDATE,

    /**
     * Multi UPDATE statement
     */
    MULTI_UPDATE,

    /**
     * "ALTER scope SET option = value" statement.
     */
    SET_OPTION,

    /**
     * A dynamic parameter.
     */
    DYNAMIC_PARAM,

    /**
     * ORDER BY clause.
     *
     * @see #DESCENDING
     * @see #NULLS_FIRST
     * @see #NULLS_LAST
     */
    ORDER_BY,

    /**
     * WITH clause.
     */
    WITH,

    /**
     * Item in WITH clause.
     */
    WITH_ITEM,

    /**
     * Union
     */
    UNION,

    /**
     * Except
     */
    EXCEPT,

    /**
     * Intersect
     */
    INTERSECT,

    /**
     * AS operator
     */
    AS,
    /**
     * AS OF operator
     */
    AS_OF,

    /**
     * ARGUMENT_ASSIGNMENT operator, {@code =>}
     */
    ARGUMENT_ASSIGNMENT,

    /**
     * DEFAULT operator
     */
    DEFAULT,

    /**
     * OVER operator
     */
    OVER,

    /**
     * FILTER operator
     */
    FILTER,

    /**
     * Window specification
     */
    WINDOW,

    /**
     * MERGE statement
     */
    MERGE,

    /**
     * TABLESAMPLE operator
     */
    TABLESAMPLE,

    /**
     * MATCH_RECOGNIZE clause
     */
    MATCH_RECOGNIZE,
    // binary operators

    /**
     * The arithmetic multiplication operator, "*".
     */
    TIMES,

    /**
     * The arithmetic division operator, "/".
     */
    DIVIDE,

    /**
     * The arithmetic remainder operator, "MOD" (and "%" in some dialects).
     */
    MOD,

    /**
     * The arithmetic plus operator, "+".
     *
     * @see #PLUS_PREFIX
     */
    PLUS,

    /**
     * The arithmetic minus operator, "-".
     *
     * @see #MINUS_PREFIX
     */
    MINUS,

    REVERT,

    /**
     * the alternation operator in a pattern expression within a match_recognize clause
     */
    PATTERN_ALTER,

    /**
     * the concatenation operator in a pattern expression within a match_recognize clause
     */
    PATTERN_CONCAT,

    // comparison operators

    /**
     * The "IN" operator.
     */
    IN,

    /**
     * The "NOT IN" operator.
     *
     * <p>Only occurs in SqlNode trees. Is expanded to NOT(IN ...) before
     * entering RelNode land.
     */
    NOT_IN("NOT IN"),

    /**
     * The "NOT EXISTS" operator.
     *
     * <p>Only occurs in SqlNode trees. Is expanded to NOT(EXISTS ...) before
     * entering RelNode land.
     */
    NOT_EXISTS("NOT EXISTS"),

    /**
     * The less-than operator, "&lt;".
     */
    LESS_THAN("<"),

    /**
     * The greater-than operator, "&gt;".
     */
    GREATER_THAN(">"),

    /**
     * The less-than-or-equal operator, "&lt;=".
     */
    LESS_THAN_OR_EQUAL("<="),

    /**
     * The greater-than-or-equal operator, "&gt;=".
     */
    GREATER_THAN_OR_EQUAL(">="),

    /**
     * The equals operator, "=".
     */
    EQUALS("="),

    /**
     * The not-equals operator, "&#33;=" or "&lt;&gt;".
     * The latter is standard, and preferred.
     */
    NOT_EQUALS("<>"),

    /**
     * The is-distinct-from operator.
     */
    IS_DISTINCT_FROM,

    /**
     * The is-not-distinct-from operator.
     */
    IS_NOT_DISTINCT_FROM,

    /**
     * The logical "OR" operator.
     */
    OR,

    /**
     * The logical "AND" operator.
     */
    AND,

    // other infix

    /**
     * Dot
     */
    DOT,

    /**
     * The "OVERLAPS" operator for periods.
     */
    OVERLAPS,

    /**
     * The "CONTAINS" operator for periods.
     */
    CONTAINS,

    /**
     * The "PRECEDES" operator for periods.
     */
    PRECEDES,

    /**
     * The "IMMEDIATELY PRECEDES" operator for periods.
     */
    IMMEDIATELY_PRECEDES("IMMEDIATELY PRECEDES"),

    /**
     * The "SUCCEEDS" operator for periods.
     */
    SUCCEEDS,

    /**
     * The "IMMEDIATELY SUCCEEDS" operator for periods.
     */
    IMMEDIATELY_SUCCEEDS("IMMEDIATELY SUCCEEDS"),

    /**
     * The "EQUALS" operator for periods.
     */
    PERIOD_EQUALS("EQUALS"),

    /**
     * The "LIKE" operator.
     */
    LIKE,

    /**
     * The "SIMILAR" operator.
     */
    SIMILAR,

    /**
     * The "BETWEEN" operator.
     */
    BETWEEN,

    /**
     * The "NOT_BETWEEN" operator.
     */
    NOT_BETWEEN,

    /**
     * A "CASE" expression.
     */
    CASE,

    /**
     * The "NULLIF" operator.
     */
    NULLIF,

    /**
     * The "COALESCE" operator.
     */
    COALESCE,

    /**
     * The "DECODE" function (Oracle).
     */
    DECODE,

    /**
     * The "NVL" function (Oracle).
     */
    NVL,

    /**
     * The "GREATEST" function (Oracle).
     */
    GREATEST,

    /**
     * The "LEAST" function (Oracle).
     */
    LEAST,

    /**
     * The "TIMESTAMP_ADD" function (ODBC, SQL Server, MySQL).
     */
    TIMESTAMP_ADD,

    /**
     * The "TIMESTAMP_DIFF" function (ODBC, SQL Server, MySQL).
     */
    TIMESTAMP_DIFF,

    // prefix operators

    /**
     * The logical "NOT" operator.
     */
    NOT,

    /**
     * The unary plus operator, as in "+1".
     *
     * @see #PLUS
     */
    PLUS_PREFIX,

    /**
     * The unary minus operator, as in "-1".
     *
     * @see #MINUS
     */
    MINUS_PREFIX,

    /**
     * The unary minus operator, as in "-1".
     *
     * @see #MINUS
     */
    INVERT_PREFIX,

    /**
     * The "EXISTS" operator.
     */
    EXISTS,

    /**
     * The "SOME" quantification operator (also called "ANY").
     */
    SOME,

    /**
     * The "ALL" quantification operator.
     */
    ALL,

    /**
     * The "VALUES" operator.
     */
    VALUES,

    /**
     * Explicit table, e.g. <code>select * from (TABLE t)</code> or <code>TABLE
     * t</code>. See also {@link #COLLECTION_TABLE}.
     */
    EXPLICIT_TABLE,

    /**
     * Scalar query; that is, a sub-query used in an expression context, and
     * returning one row and one column.
     */
    SCALAR_QUERY,

    /**
     * ProcedureCall
     */
    PROCEDURE_CALL,

    /**
     * NewSpecification
     */
    NEW_SPECIFICATION,

    /**
     * Special functions in MATCH_RECOGNIZE.
     */
    FINAL,

    RUNNING,

    PREV,

    NEXT,

    FIRST,

    LAST,

    CLASSIFIER,

    MATCH_NUMBER,

    /**
     * The "SKIP TO FIRST" qualifier of restarting point in a MATCH_RECOGNIZE
     * clause.
     */
    SKIP_TO_FIRST,

    /**
     * The "SKIP TO LAST" qualifier of restarting point in a MATCH_RECOGNIZE
     * clause.
     */
    SKIP_TO_LAST,

    // postfix operators

    /**
     * DESC in ORDER BY. A parse tree, not a true expression.
     */
    DESCENDING,

    ASCENDING,

    /**
     * NULLS FIRST clause in ORDER BY. A parse tree, not a true expression.
     */
    NULLS_FIRST,

    /**
     * NULLS LAST clause in ORDER BY. A parse tree, not a true expression.
     */
    NULLS_LAST,

    /**
     * The "IS TRUE" operator.
     */
    IS_TRUE,

    /**
     * The "IS FALSE" operator.
     */
    IS_FALSE,

    /**
     * The "IS NOT TRUE" operator.
     */
    IS_NOT_TRUE,

    /**
     * The "IS NOT FALSE" operator.
     */
    IS_NOT_FALSE,

    /**
     * The "IS UNKNOWN" operator.
     */
    IS_UNKNOWN,

    /**
     * The "IS NULL" operator.
     */
    IS_NULL,

    /**
     * The "IS NOT NULL" operator.
     */
    IS_NOT_NULL,

    /**
     * The "PRECEDING" qualifier of an interval end-point in a window
     * specification.
     */
    PRECEDING,

    /**
     * The "FOLLOWING" qualifier of an interval end-point in a window
     * specification.
     */
    FOLLOWING,

    /**
     * The field access operator, ".".
     *
     * <p>(Only used at the RexNode level; at
     * SqlNode level, a field-access is part of an identifier.)</p>
     */
    FIELD_ACCESS,

    /**
     * Reference to an input field.
     *
     * <p>(Only used at the RexNode level.)</p>
     */
    INPUT_REF,

    /**
     * Reference to an input field, with a qualified name and an identifier
     *
     * <p>(Only used at the RexNode level.)</p>
     */
    TABLE_INPUT_REF,

    /**
     * Reference to an input field, with pattern var as modifier
     *
     * <p>(Only used at the RexNode level.)</p>
     */
    PATTERN_INPUT_REF,
    /**
     * Reference to a sub-expression computed within the current relational
     * operator.
     *
     * <p>(Only used at the RexNode level.)</p>
     */
    LOCAL_REF,

    /**
     * Reference to correlation variable.
     *
     * <p>(Only used at the RexNode level.)</p>
     */
    CORREL_VARIABLE,

    /**
     * the repetition quantifier of a pattern factor in a match_recognize clause.
     */
    PATTERN_QUANTIFIER,

    // functions

    /**
     * The row-constructor function. May be explicit or implicit:
     * {@code VALUES 1, ROW (2)}.
     */
    ROW,

    /**
     * The non-standard constructor used to pass a
     * COLUMN_LIST parameter to a user-defined transform.
     */
    COLUMN_LIST,

    /**
     * The "CAST" operator.
     */
    CAST,

    /**
     * The "NEXT VALUE OF sequence" operator.
     */
    NEXT_VALUE,

    /**
     * The "CURRENT VALUE OF sequence" operator.
     */
    CURRENT_VALUE,

    /**
     * The "FLOOR" function
     */
    FLOOR,

    /**
     * The "CEIL" function
     */
    CEIL,

    /**
     * The "TRIM" function.
     */
    TRIM,

    /**
     * The "LTRIM" function (Oracle).
     */
    LTRIM,

    /**
     * The "RTRIM" function (Oracle).
     */
    RTRIM,

    /**
     * The "EXTRACT" function.
     */
    EXTRACT,

    /**
     * Call to a function using JDBC function syntax.
     */
    JDBC_FN,

    /**
     * The MULTISET value constructor.
     */
    MULTISET_VALUE_CONSTRUCTOR,

    /**
     * The MULTISET query constructor.
     */
    MULTISET_QUERY_CONSTRUCTOR,

    /**
     * The "UNNEST" operator.
     */
    UNNEST,

    /**
     * The "LATERAL" qualifier to relations in the FROM clause.
     */
    LATERAL,

    /**
     * Table operator which converts user-defined transform into a relation, for
     * example, <code>select * from TABLE(udx(x, y, z))</code>. See also the
     * {@link #EXPLICIT_TABLE} prefix operator.
     */
    COLLECTION_TABLE,

    /**
     * N row will transform to logicalValue
     */
    N_ROW,

    /**
     * Array Value Constructor, e.g. {@code Array[1, 2, 3]}.
     */
    ARRAY_VALUE_CONSTRUCTOR,

    /**
     * Array Query Constructor, e.g. {@code Array(select deptno from dept)}.
     */
    ARRAY_QUERY_CONSTRUCTOR,

    /**
     * Map Value Constructor, e.g. {@code Map['washington', 1, 'obama', 44]}.
     */
    MAP_VALUE_CONSTRUCTOR,

    /**
     * Map Query Constructor, e.g. {@code MAP (SELECT empno, deptno FROM emp)}.
     */
    MAP_QUERY_CONSTRUCTOR,

    /**
     * CURSOR constructor, for example, <code>select * from
     * TABLE(udx(CURSOR(select ...), x, y, z))</code>
     */
    CURSOR,

    // internal operators (evaluated in validator) 200-299

    /**
     * Literal chain operator (for composite string literals).
     * An internal operator that does not appear in SQL syntax.
     */
    LITERAL_CHAIN,

    /**
     * Escape operator (always part of LIKE or SIMILAR TO expression).
     * An internal operator that does not appear in SQL syntax.
     */
    ESCAPE,

    /**
     * The internal REINTERPRET operator (meaning a reinterpret cast).
     * An internal operator that does not appear in SQL syntax.
     */
    REINTERPRET,

    /**
     * The internal {@code EXTEND} operator that qualifies a table name in the
     * {@code FROM} clause.
     */
    EXTEND,

    /**
     * The internal {@code CUBE} operator that occurs within a {@code GROUP BY}
     * clause.
     */
    CUBE,

    /**
     * The internal {@code ROLLUP} operator that occurs within a {@code GROUP BY}
     * clause.
     */
    ROLLUP,

    /**
     * The internal {@code GROUPING SETS} operator that occurs within a
     * {@code GROUP BY} clause.
     */
    GROUPING_SETS,

    /**
     * The {@code GROUPING(e, ...)} function.
     */
    GROUPING,

    /**
     * @deprecated Use {@link #GROUPING}.
     */
    @Deprecated // to be removed before 2.0
        GROUPING_ID,

    /**
     * The {@code GROUP_ID()} function.
     */
    GROUP_ID,

    /**
     * the internal permute function in match_recognize cluse
     */
    PATTERN_PERMUTE,

    /**
     * the special patterns to exclude enclosing pattern from output in match_recognize clause
     */
    PATTERN_EXCLUDED,

    // Aggregate functions

    /**
     * The {@code COUNT} aggregate function.
     */
    COUNT,

    /**
     * The {@code SUM} aggregate function.
     */
    SUM,

    /**
     * The {@code SUM0} aggregate function.
     */
    SUM0,

    /**
     * The {@code MIN} aggregate function.
     */
    MIN,

    /**
     * The {@code MAX} aggregate function.
     */
    MAX,

    /**
     * The {@code LEAD} aggregate function.
     */
    LEAD,

    /**
     * The {@code LAG} aggregate function.
     */
    LAG,

    /**
     * The {@code FIRST_VALUE} aggregate function.
     */
    FIRST_VALUE,

    /**
     * The {@code LAST_VALUE} aggregate function.
     */
    LAST_VALUE,

    /**
     * The {@code LAST_VALUE} aggregate function.
     */
    NTH_VALUE,
    /**
     * The {@code LAST_VALUE} aggregate function.
     */
    N_TILE,

    /**
     * The {@code COVAR_POP} aggregate function.
     */
    COVAR_POP,

    /**
     * The {@code COVAR_SAMP} aggregate function.
     */
    COVAR_SAMP,

    /**
     * The {@code REGR_SXX} aggregate function.
     */
    REGR_SXX,

    /**
     * The {@code REGR_SYY} aggregate function.
     */
    REGR_SYY,

    /**
     * The {@code AVG} aggregate function.
     */
    AVG,

    /**
     * The {@code STDDEV_POP} aggregate function.
     */
    STDDEV_POP,

    /**
     * The {@code STDDEV_SAMP} aggregate function.
     */
    STDDEV_SAMP,

    /**
     * The {@code VAR_POP} aggregate function.
     */
    VAR_POP,

    /**
     * The {@code VAR_SAMP} aggregate function.
     */
    VAR_SAMP,

    /**
     * The {@code NTILE} aggregate function.
     */
    NTILE,

    /**
     * The {@code COLLECT} aggregate function.
     */
    COLLECT,

    /**
     * The {@code FUSION} aggregate function.
     */
    FUSION,

    /**
     * The {@code SINGLE_VALUE} aggregate function.
     */
    SINGLE_VALUE,

    /**
     * The {code __FIRST_VALUE} internal aggregate function.
     */
    __FIRST_VALUE,

    /**
     * The {@code ROW_NUMBER} window function.
     */
    ROW_NUMBER,

    /**
     * The {@code RANK} window function.
     */
    RANK,

    /**
     * The {@code PERCENT_RANK} window function.
     */
    PERCENT_RANK,

    /**
     * The {@code DENSE_RANK} window function.
     */
    DENSE_RANK,

    /**
     * The {@code ROW_NUMBER} window function.
     */
    CUME_DIST,

    /**
     * The {@code  GROUP_CONCAT} function.
     */
    GROUP_CONCAT,
    /**
     * {@code  BIT_OR}
     */
    BIT_OR,
    /**
     * {@code  BIT_AND}
     */
    BIT_AND,
    /**
     * TDDL ADD {@code  BIT_XOR}
     */
    BIT_XOR,
    // Group functions

    /**
     * The {@code TUMBLE} group function.
     */
    TUMBLE,

    /**
     * The {@code TUMBLE_START} auxiliary function of
     * the {@link #TUMBLE} group function.
     */
    TUMBLE_START,

    /**
     * The {@code TUMBLE_END} auxiliary function of
     * the {@link #TUMBLE} group function.
     */
    TUMBLE_END,

    /**
     * The {@code HOP} group function.
     */
    HOP,

    /**
     * The {@code HOP_START} auxiliary function of
     * the {@link #HOP} group function.
     */
    HOP_START,

    /**
     * The {@code HOP_END} auxiliary function of
     * the {@link #HOP} group function.
     */
    HOP_END,

    /**
     * The {@code SESSION} group function.
     */
    SESSION,

    /**
     * The {@code SESSION_START} auxiliary function of
     * the {@link #SESSION} group function.
     */
    SESSION_START,

    /**
     * The {@code SESSION_END} auxiliary function of
     * the {@link #SESSION} group function.
     */
    SESSION_END,

    /**
     * Column declaration.
     */
    COLUMN_DECL,

    /**
     * Column reference.
     */
    COLUMN_REFERENCE,

    /**
     * Column reference option.
     */
    COLUMN_REFERENCE_OPTION,

    /**
     * {@code CHECK} constraint.
     */
    CHECK,

    /**
     * {@code UNIQUE} constraint.
     */
    UNIQUE,

    /**
     * {@code PRIMARY KEY} constraint.
     */
    PRIMARY_KEY,

    /**
     * {@code FOREIGN KEY} constraint.
     */
    FOREIGN_KEY,

    /**
     * Index definition
     */
    INDEX_DEF,

    /**
     * Index column name
     */
    INDEX_COLUMN_NAME,

    /**
     * Index option
     */
    INDEX_OPTION,

    /**
     * Table options
     */
    TABLE_OPTIONS,

    /**
     * Add index
     */
    ADD_INDEX,

    /**
     * Add unique index
     */
    ADD_UNIQUE_INDEX,

    /**
     * Add full text index
     */
    ADD_FULL_TEXT_INDEX,

    /**
     * Add spatial index
     */
    ADD_SPATIAL_INDEX,

    /**
     * Add foreign key
     */
    ADD_FOREIGN_KEY,

    // DDL and session control statements follow. The list is not exhaustive: feel
    // free to add more.

    /**
     * {@code COMMIT} session control statement.
     */
    COMMIT,

    /**
     * {@code ROLLBACK} session control statement.
     */
    ROLLBACK,

    /**
     * {@code ALTER SESSION} DDL statement.
     */
    ALTER_SESSION,

    /**
     * {@code CREATE SCHEMA} DDL statement.
     */
    CREATE_SCHEMA,

    /**
     * {@code CREATE FOREIGN SCHEMA} DDL statement.
     */
    CREATE_FOREIGN_SCHEMA,

    /**
     * {@code DROP SCHEMA} DDL statement.
     */
    DROP_SCHEMA,

    /**
     * {@code CREATE TABLE} DDL statement.
     */
    CREATE_TABLE,

    /**
     * {@code ALTER TABLE} DDL statement.
     */
    ALTER_TABLE,

    /**
     * {@code DROP TABLE} DDL statement.
     */
    DROP_TABLE,

    /**
     * {@code CREATE VIEW} DDL statement.
     */
    CREATE_VIEW,

    /**
     * {@code CREATE VIEW} VIRTUAL_VIEW.
     */
    VIRTUAL_VIEW,

    /**
     * {@code ALTER VIEW} DDL statement.
     */
    ALTER_VIEW,

    /**
     * {@code DROP VIEW} DDL statement.
     */
    DROP_VIEW,

    RENAME_TABLE,

    DROP_TYPE,

    /**
     * {@code CREATE MATERIALIZED VIEW} DDL statement.
     */
    CREATE_MATERIALIZED_VIEW,

    /**
     * {@code ALTER MATERIALIZED VIEW} DDL statement.
     */
    ALTER_MATERIALIZED_VIEW,

    /**
     * {@code DROP MATERIALIZED VIEW} DDL statement.
     */
    DROP_MATERIALIZED_VIEW,

    /**
     * {@code CREATE SEQUENCE} DDL statement.
     */
    CREATE_SEQUENCE,

    /**
     * {@code ALTER SEQUENCE} DDL statement.
     */
    ALTER_SEQUENCE,

    /**
     * {@code DROP SEQUENCE} DDL statement.
     */
    DROP_SEQUENCE,

    RENAME_SEQUENCE,

    /**
     * {@code CREATE INDEX} DDL statement.
     */
    CREATE_INDEX,

    /**
     * {@code ALTER INDEX} DDL statement.
     */
    ALTER_INDEX,

    /**
     * {@code DROP INDEX} DDL statement.
     */
    DROP_INDEX,

    /**
     * {@code DROP FILE} DDL statement.
     */
    DROP_FILE,

    /**
     * {@code DROP PRIMARY KEY} DDL statement.
     */
    DROP_PRIMARY_KEY,

    /**
     * {@code ADD PRIMARY KEY} DDL statement.
     */
    ADD_PRIMARY_KEY,

    /**
     * {@code RENAME INDEX} DDL statement.
     */
    ALTER_RENAME_INDEX,

    /**
     * {@code ALTER COLUMN DEFAULT VAL} DDL statement.
     */
    ALTER_COLUMN_DEFAULT_VAL,

    /**
     * {@code CHANGE COLUMN} DDL statement.
     */
    CHANGE_COLUMN,

    /**
     * {@code MODIFY COLUMN} DDL statement.
     */
    MODIFY_COLUMN,

    /**
     * {@code DROP COLUMN} DDL statement.
     */
    DROP_COLUMN,

    /**
     * {@code ADD COLUMN} DDL statement.
     */
    ADD_COLUMN,

    /**
     * {@code ENABLE KEYS} DDL statement.
     */
    ENABLE_KEYS,

    /**
     * {@code CONVERT TO CHARACTER SET} DDL statement.
     */
    CONVERT_TO_CHARACTER_SET,

    FLASHBACK_TABLE,

    PURGE,

    SAVEPOINT,

    /**
     * {@code CREATE DATABASE} DDL statement.
     */
    CREATE_DATABASE,

    CREATE_CCL_RULE,

    CREATE_CCL_TRIGGER,

    CREATE_SCHEDULE,

    DROP_SCHEDULE,

    /**
     * {@code DROP DATABASE} DDL statement.
     */
    DROP_DATABASE,

    DROP_CCL_RULE,

    DROP_CCL_TRIGGER,

    /**
     * ALTER SYSTEM CHANGE_CONSENSUS_LEADER
     */
    CHANGE_CONSENSUS_ROLE,

    /**
     * ALTER SYSTEM SET CONFIG xx=xx
     */
    ALTER_SYSTEM_SET_CONFIG,

    /**
     * ALTER SYSTEM GET CONFIG xxx
     */
    ALTER_SYSTEM_GET_CONFIG,

    /**
     * DDL statement not handled above.
     *
     * <p><b>Note to other projects</b>: If you are extending Calcite's SQL parser
     * and have your own object types you no doubt want to define CREATE and DROP
     * commands for them. Use OTHER_DDL in the short term, but we are happy to add
     * new enum values for your object types. Just ask!
     */
    OTHER_DDL,

    SHOW,

    SHOW_DATASOURCES,

    SHOW_TABLES,

    SHOW_TABLE_INFO,

    SHOW_CREATE_DATABASE,

    SHOW_CREATE_TABLE,

    SHOW_CREATE_VIEW,

    SHOW_PROCEDURE_STATUS,

    SHOW_VARIABLES,

    SHOW_PROCESSLIST,

    SHOW_TABLE_STATUS,

    SHOW_SLOW,

    SHOW_STC,

    SHOW_HTC,

    SHOW_PARTITIONS,

    SHOW_TOPOLOGY,

    SHOW_FILES,

    SHOW_CACHE_STATS,

    SHOW_BROADCASTS,

    SHOW_DS,

    SHOW_DB_STATUS,

    SHOW_STATS,

    SHOW_TRACE,

    SHOW_SEQUENCES,

    SHOW_RULE,

    SHOW_RULE_STATUS,

    SHOW_DDL_STATUS,

    SHOW_DDL_JOBS,

    SHOW_DDL_RESULTS,

    SHOW_SCHEDULE_RESULTS,

    SHOW_GRANTS,

    SHOW_AUTHORS,

    SHOW_NODE,

    SHOW_RECYCLEBIN,

    SHOW_INDEX,

    SHOW_PROFILE,

    SHOW_GLOBAL_INDEX,

    SHOW_GLOBAL_DEADLOCKS,

    SHOW_LOCAL_DEADLOCKS,

    SHOW_METADATA_LOCK,

    SHOW_TRANS,

    SHOW_CCL_RULE,

    SHOW_CCL_TRIGGER,

    SHOW_BINARY_LOGS,

    SHOW_MASTER_STATUS,

    SHOW_BINLOG_EVENTS,

    CHANGE_MASTER,

    START_SLAVE,

    STOP_SLAVE,

    RESET_SLAVE,

    CHANGE_REPLICATION_FILTER,

    SHOW_SLAVE_STATUS,

    DESCRIBE_COLUMNS,

    LOCK_TABLE,

    UNLOCK_TABLE,

    CHECK_TABLE,

    CHECK_GLOBAL_INDEX, // As a special DDL.

    ANALYZE_TABLE,

    OPTIMIZE_TABLE,

    KILL,

    SQL_SET,

    SQL_SET_NAMES,

    SQL_SET_CHARACTER_SET,

    SQL_SET_TRANSACTION,

    SELECT_INDEX,

    TRUNCATE_TABLE,

    ALTER_RULE,

    NONE,

    INSPECT_RULE_VERSION,

    REFRESH_LOCAL_RULES,

    CLEAR_SEQ_CACHE,

    INSPECT_GROUP_SEQ_RANGE,

    CANCEL_DDL_JOB,

    RECOVER_DDL_JOB,

    CONTINUE_DDL_JOB,

    PAUSE_DDL_JOB,

    ROLLBACK_DDL_JOB,

    REMOVE_DDL_JOB,

    INSPECT_DDL_JOB_CACHE,

    CLEAR_DDL_JOB_CACHE,

    CLEAR_CCL_RULES,

    CLEAR_CCL_TRIGGERS,

    SLOW_SQL_CCL,

    CHANGE_DDL_JOB,

    BASELINE,

    MOVE_DATABASE,
    SHOW_MOVE_DATABASE,

    PARTITION_BY,

    RUNTIME_FILTER_BUILD,
    RUNTIME_FILTER,

    SYSTEM_VAR,

    ASSIGNMENT(":="),

    USER_VAR,

    SQL_SET_DEFAULT_ROLE,

    SQL_SET_ROLE,

    /**
     * partition management: add partition.
     */
    ADD_PARTITION,
    /**
     * partition management: modify partition
     */
    MODIFY_PARTITION,
    /**
     * partition management: drop partition
     */
    DROP_PARTITION,
    /**
     * partition management: truncate partition
     */
    TRUNCATE_PARTITION,
    /**
     * partition management: alter table group split partition
     */
    SPLIT_PARTITION,
    /**
     * partition management: alter table group merge partition
     */
    MERGE_PARTITION,

    /**
     * partition management: alter table group move partition
     */
    MOVE_PARTITION,

    /**
     * partition management: alter table group extract to partition by hot value(xxx)
     */
    EXTRACT_PARTITION,

    /**
     * partition management: Exchanging Partitions and Subpartitions with Tables
     */
    EXCHANGE_PARTITION,

    /**
     * partition management: alter table group split into partitions xxx by hot value(xxx[,..,xxx])
     */
    SPLIT_HOT_VALUE,

    /**
     * partition management: create new tablegroup
     */
    CREATE_TABLEGROUP,

    /**
     * partition management: create new tablegroup
     */
    DROP_TABLEGROUP,

    /**
     * partition rebalance
     */
    REBALANCE,

    /**
     * unarchive oss tables
     */
    UNARCHIVE,

    /**
     * partition group management: alter table group
     */
    ALTER_TABLEGROUP,

    /**
     * file storage management: create fileStorage
     */
    CREATE_FILESTORAGE,

    /**
     * file storage management: alter fileStorage
     */
    ALTER_FILESTORAGE,

    /**
     * file storage management: drop fileStorage
     */
    DROP_FILESTORAGE,

    /**
     * alter table set tablegroup
     */
    ALTER_TABLE_SET_TABLEGROUP,

    /**
     * partition management:alter tablegroup rename partition
     */
    ALTER_TABLEGROUP_RENAME_PARTITION,
    /**
     * refresh topology
     */
    REFRESH_TOPOLOGY,
    /**
     * allocate local partition
     */
    ALLOCATE_LOCAL_PARTITION,
    /**
     * expire local partition
     */
    EXPIRE_LOCAL_PARTITION,
    /**
     * REPARTITION_LOCAL_PARTITION
     */
    REPARTITION_LOCAL_PARTITION
    ;

    //~ Static fields/initializers ---------------------------------------------

    // Most of the static fields are categories, aggregating several kinds into
    // a set.

    /**
     * Category consisting of show-query node types.
     *
     * <p>Consists of:
     * {@link #SHOW_DATASOURCES      } ,
     * {@link #SHOW_NODE             } ,
     * {@link #SHOW_TABLES           } ,
     * {@link #SHOW_CREATE_TABLE     } ,
     * {@link #SHOW_PROCEDURE_STATUS } ,
     * {@link #SHOW_VARIABLES        } ,
     * {@link #SHOW_PROCESSLIST      } ,
     * {@link #SHOW_TABLE_STATUS     } ,
     * {@link #SHOW_SLOW             } ,
     * {@link #SHOW_STC              } ,
     * {@link #SHOW_HTC              } ,
     * {@link #SHOW_PARTITIONS       } ,
     * {@link #SHOW_TOPOLOGY         } ,
     * {@link #SHOW_FILES            } ,
     * {@link #SHOW_CACHE_STATS      } ,
     * {@link #SHOW_BROADCASTS       } ,
     * {@link #SHOW_DS               } ,
     * {@link #SHOW_DB_STATUS        } ,
     * {@link #SHOW_STATS            } ,
     * {@link #SHOW_TRACE            } ,
     * {@link #SHOW_SEQUENCES        } ,
     * {@link #SHOW_RULE             } ,
     * {@link #SHOW_RULE_STATUS      } ,
     * {@link #SHOW_DDL_STATUS       } ,
     * {@link #SHOW_DDL_JOBS         } ,
     * {@link #SHOW_DDL_RESULTS      } ,
     * {@link #SHOW_SCHEDULE_RESULTS      } ,
     * {@link #SHOW_GRANTS           } ,
     * {@link #DESCRIBE_COLUMNS      } ,
     * {@link #SHOW_AUTHORS          } ,
     */
    public static final EnumSet<SqlKind> LOGICAL_SHOW_QUERY = EnumSet.of(SHOW_DATASOURCES,
        SHOW_NODE,
        SHOW_TABLES,
        SHOW_CREATE_DATABASE,
        SHOW_CREATE_TABLE,
        SHOW_CREATE_VIEW,
        SHOW_PROCEDURE_STATUS,
        SHOW_VARIABLES,
        SHOW_PROCESSLIST,
        SHOW_TABLE_STATUS,
        SHOW_SLOW,
        SHOW_STC,
        SHOW_HTC,
        SHOW_PARTITIONS,
        SHOW_TOPOLOGY,
        SHOW_FILES,
        SHOW_CACHE_STATS,
        SHOW_BROADCASTS,
        SHOW_DS,
        SHOW_DB_STATUS,
        SHOW_TABLE_INFO,
        SHOW_STATS,
        SHOW_TRACE,
        SHOW_SEQUENCES,
        SHOW_RULE,
        SHOW_RULE_STATUS,
        SHOW_DDL_STATUS,
        SHOW_DDL_JOBS,
        SHOW_DDL_RESULTS,
        SHOW_SCHEDULE_RESULTS,
        SHOW_GRANTS,
        DESCRIBE_COLUMNS,
        SHOW_AUTHORS,
        SHOW_RECYCLEBIN,
        SHOW_INDEX,
        SHOW_PROFILE,
        SHOW_GLOBAL_INDEX,
        SHOW_METADATA_LOCK,
        SHOW_TRANS,
        SHOW_LOCAL_DEADLOCKS,
        SHOW_GLOBAL_DEADLOCKS);

    public static final EnumSet<SqlKind> LOGICAL_SHOW_WITH_TABLE = EnumSet.of(SHOW_CREATE_TABLE,
        SHOW_TOPOLOGY,
        SHOW_FILES,
        SHOW_RULE,
        SHOW_TABLE_INFO,
        DESCRIBE_COLUMNS,
        SHOW_PARTITIONS);
    public static final EnumSet<SqlKind> LOGICAL_SHOW_WITH_SCHEMA = EnumSet.of(SHOW_TABLES);

    public static final EnumSet<SqlKind> LOGICAL_SHOW_BINLOG =
        EnumSet.of(SHOW_BINARY_LOGS, SHOW_BINLOG_EVENTS, SHOW_MASTER_STATUS);

    public static final EnumSet<SqlKind> LOGICAL_REPLICATION = EnumSet.of(CHANGE_MASTER,
        START_SLAVE, STOP_SLAVE, CHANGE_REPLICATION_FILTER, SHOW_SLAVE_STATUS, RESET_SLAVE);

    public static final EnumSet<SqlKind> SHOW_QUERY = concat(EnumSet.of(SHOW), LOGICAL_SHOW_QUERY);

    public static final EnumSet<SqlKind> TABLE_MAINTENANCE_QUERY = EnumSet.of(CHECK_TABLE,
        CHECK_GLOBAL_INDEX,
        ANALYZE_TABLE,
        OPTIMIZE_TABLE);

    public static final EnumSet<SqlKind> OTHER_ADMIN_QUERY = EnumSet.of(KILL,
        CANCEL_DDL_JOB,
        RECOVER_DDL_JOB,
        CONTINUE_DDL_JOB,
        PAUSE_DDL_JOB,
        ROLLBACK_DDL_JOB,
        REMOVE_DDL_JOB,
        INSPECT_DDL_JOB_CACHE,
        CLEAR_DDL_JOB_CACHE,
        CHANGE_DDL_JOB,
        INSPECT_RULE_VERSION,
        REFRESH_LOCAL_RULES,
        CLEAR_SEQ_CACHE,
        INSPECT_GROUP_SEQ_RANGE,
        BASELINE,
        MOVE_DATABASE,
        REBALANCE
    );

    public static final EnumSet<SqlKind> SQL_SET_QUERY = EnumSet.of(SQL_SET,
        SQL_SET_DEFAULT_ROLE,
        SQL_SET_NAMES,
        SQL_SET_CHARACTER_SET,
        SQL_SET_TRANSACTION);

    public static final EnumSet<SqlKind> SQL_TABLE_LOCK = EnumSet.of(LOCK_TABLE);

    public static final EnumSet<SqlKind> SQL_TRANS = EnumSet.of(SAVEPOINT);

    /**
     * Category consisting of set-query node types.
     *
     * <p>Consists of:
     * {@link #EXCEPT},
     * {@link #INTERSECT},
     * {@link #UNION}.
     */
    public static final EnumSet<SqlKind> SET_QUERY =
        EnumSet.of(UNION, INTERSECT, EXCEPT);

    /**
     * Category consisting of all built-in aggregate functions.
     */
    public static final EnumSet<SqlKind> AGGREGATE =
        EnumSet.of(COUNT, SUM, SUM0, MIN, MAX, LEAD, LAG, FIRST_VALUE,
            LAST_VALUE, COVAR_POP, COVAR_SAMP, REGR_SXX, REGR_SYY,
            AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, NTILE, COLLECT,
            FUSION, SINGLE_VALUE, ROW_NUMBER, RANK, PERCENT_RANK, DENSE_RANK,
            CUME_DIST, GROUP_CONCAT, NTH_VALUE);

    public static final EnumSet<SqlKind> WINDOW_FUNCTION =
        EnumSet.of(LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTILE, ROW_NUMBER,
            NTH_VALUE, RANK, PERCENT_RANK, DENSE_RANK, CUME_DIST);

    /**
     * Category consisting of all DML operators.
     *
     * <p>Consists of:
     * {@link #INSERT},
     * {@link #UPDATE},
     * {@link #DELETE},
     * {@link #MERGE},
     * {@link #PROCEDURE_CALL}.
     *
     * <p>NOTE jvs 1-June-2006: For now we treat procedure calls as DML;
     * this makes it easy for JDBC clients to call execute or
     * executeUpdate and not have to process dummy cursor results.  If
     * in the future we support procedures which return results sets,
     * we'll need to refine this.
     */
    public static final EnumSet<SqlKind> DML =
        EnumSet.of(INSERT, REPLACE, DELETE, UPDATE, MERGE, PROCEDURE_CALL, MULTI_UPDATE);

    /**
     * Category consisting of all DDL operators.
     */

    public static final EnumSet<SqlKind> ALTER_ADD_INDEX = EnumSet.of(ADD_INDEX,
        ADD_UNIQUE_INDEX,
        ADD_FULL_TEXT_INDEX,
        ADD_SPATIAL_INDEX,
        ADD_FOREIGN_KEY);

    public static final EnumSet<SqlKind> ALTER_ALTER_COLUMN = EnumSet.of(ALTER_COLUMN_DEFAULT_VAL,
        CHANGE_COLUMN,
        MODIFY_COLUMN,
        DROP_COLUMN);

    public static final EnumSet<SqlKind> CHECK_ALTER_WITH_GSI = concat(ALTER_ADD_INDEX,
        ALTER_ALTER_COLUMN,
        EnumSet.of(DROP_INDEX, ADD_COLUMN, ENABLE_KEYS, DROP_PRIMARY_KEY, CONVERT_TO_CHARACTER_SET));

    public static final EnumSet<SqlKind> DDL = concat(ALTER_ALTER_COLUMN, ALTER_ADD_INDEX,
        EnumSet.of(COMMIT, ROLLBACK, ALTER_SESSION,
            CREATE_SCHEMA, CREATE_FOREIGN_SCHEMA, DROP_SCHEMA,
            CREATE_TABLE, ALTER_TABLE, DROP_TABLE,
            CREATE_VIEW, ALTER_VIEW, DROP_VIEW,
            CREATE_MATERIALIZED_VIEW, ALTER_MATERIALIZED_VIEW,
            DROP_MATERIALIZED_VIEW,
            CREATE_SEQUENCE, ALTER_SEQUENCE, DROP_SEQUENCE,
            CREATE_INDEX, ALTER_INDEX, DROP_INDEX, ALTER_RENAME_INDEX, RENAME_TABLE,
            SET_OPTION, OTHER_DDL, TRUNCATE_TABLE, RENAME_SEQUENCE, ALTER_RULE, ENABLE_KEYS, CREATE_DATABASE,
            DROP_DATABASE,
            MOVE_DATABASE, CHECK_GLOBAL_INDEX, ALTER_TABLEGROUP, CREATE_TABLEGROUP,
            CHANGE_CONSENSUS_ROLE, ALTER_SYSTEM_SET_CONFIG, ALTER_TABLE_SET_TABLEGROUP,
            REFRESH_TOPOLOGY, DROP_TABLEGROUP, ALTER_FILESTORAGE, DROP_FILESTORAGE, CREATE_FILESTORAGE
        ));

    public static final EnumSet<SqlKind> DDL_SUPPORTED_BY_NEW_ENGINE =
        EnumSet.of(RENAME_TABLE, TRUNCATE_TABLE, DROP_TABLE, CREATE_INDEX, DROP_INDEX, ALTER_TABLE, CREATE_TABLE,
            ALTER_TABLEGROUP, ALTER_TABLE_SET_TABLEGROUP, REFRESH_TOPOLOGY, CHECK_GLOBAL_INDEX, ALTER_RULE,
            MOVE_DATABASE, ALTER_FILESTORAGE, DROP_FILESTORAGE, CREATE_FILESTORAGE);

    public static final EnumSet<SqlKind> SUPPORT_DDL =
        EnumSet.of(CREATE_TABLE, ALTER_TABLE, DROP_TABLE,
            CREATE_VIEW, ALTER_VIEW, DROP_VIEW,
            CREATE_MATERIALIZED_VIEW, ALTER_MATERIALIZED_VIEW,
            DROP_MATERIALIZED_VIEW,
            CREATE_SEQUENCE, ALTER_SEQUENCE, DROP_SEQUENCE,
            CREATE_INDEX, ALTER_INDEX, DROP_INDEX, ALTER_RENAME_INDEX, RENAME_TABLE, TRUNCATE_TABLE, RENAME_SEQUENCE,
            CREATE_DATABASE,
            DROP_DATABASE, CHECK_GLOBAL_INDEX, MOVE_DATABASE,
            CHANGE_CONSENSUS_ROLE, ALTER_SYSTEM_SET_CONFIG,
            REBALANCE, ALLOCATE_LOCAL_PARTITION, REPARTITION_LOCAL_PARTITION);

    public static final EnumSet<SqlKind> SUPPORT_SHADOW_DDL =
        EnumSet.of(CREATE_TABLE, ALTER_TABLE, DROP_TABLE,
            CREATE_INDEX, ALTER_INDEX, DROP_INDEX, ALTER_RENAME_INDEX, RENAME_TABLE, TRUNCATE_TABLE);

    public static final EnumSet<SqlKind> SEQUENCE_DDL =
        EnumSet.of(CREATE_SEQUENCE, ALTER_SEQUENCE, DROP_SEQUENCE, RENAME_SEQUENCE);

    public static final EnumSet<SqlKind> SUPPORT_CCL =
        EnumSet.of(CREATE_CCL_RULE, DROP_CCL_RULE, SHOW_CCL_RULE, CLEAR_CCL_RULES, CREATE_CCL_TRIGGER, DROP_CCL_TRIGGER,
            SHOW_CCL_TRIGGER, CLEAR_CCL_TRIGGERS, SLOW_SQL_CCL);

    public static final EnumSet<SqlKind> SUPPORT_SCHEDULE =
        EnumSet.of(CREATE_SCHEDULE, DROP_SCHEDULE);

    /**
     * Category consisting of all DAL operators.
     */
    public static final EnumSet<SqlKind> DAL = concat(SHOW_QUERY,
        TABLE_MAINTENANCE_QUERY,
        OTHER_ADMIN_QUERY,
        SQL_SET_QUERY,
        SQL_TABLE_LOCK,
        SQL_TRANS,
        SUPPORT_CCL,
        LOGICAL_REPLICATION,
        SUPPORT_SCHEDULE);

    /**
     * Category consisting of query node types.
     *
     * <p>Consists of:
     * {@link #SELECT},
     * {@link #EXCEPT},
     * {@link #INTERSECT},
     * {@link #UNION},
     * {@link #VALUES},
     * {@link #ORDER_BY},
     * {@link #EXPLICIT_TABLE}.
     */
    public static final EnumSet<SqlKind> QUERY =
        EnumSet.of(SELECT, UNION, INTERSECT, EXCEPT, VALUES, WITH, ORDER_BY,
            EXPLICIT_TABLE);

    /**
     * ;
     * Category consisting of all expression operators.
     *
     * <p>A node is an expression if it is NOT one of the following:
     * {@link #AS},
     * {@link #ARGUMENT_ASSIGNMENT},
     * {@link #DEFAULT},
     * {@link #DESCENDING},
     * {@link #SELECT},
     * {@link #JOIN},
     * {@link #OTHER_FUNCTION},
     * {@link #CAST},
     * {@link #TRIM},
     * {@link #LITERAL_CHAIN},
     * {@link #JDBC_FN},
     * {@link #PRECEDING},
     * {@link #FOLLOWING},
     * {@link #ORDER_BY},
     * {@link #COLLECTION_TABLE},
     * {@link #TABLESAMPLE},
     * or an aggregate function, DML or DDL.
     */
    public static final Set<SqlKind> EXPRESSION =
        EnumSet.complementOf(
            concat(
                EnumSet.of(AS, ARGUMENT_ASSIGNMENT, DEFAULT,
                    RUNNING, FINAL, LAST, FIRST, PREV, NEXT,
                    DESCENDING, CUBE, ROLLUP, GROUPING_SETS, EXTEND, LATERAL,
                    SELECT, JOIN, OTHER_FUNCTION, CAST, TRIM, FLOOR, CEIL,
                    TIMESTAMP_ADD, TIMESTAMP_DIFF, EXTRACT,
                    LITERAL_CHAIN, JDBC_FN, PRECEDING, FOLLOWING, ORDER_BY,
                    NULLS_FIRST, NULLS_LAST, COLLECTION_TABLE, TABLESAMPLE,
                    VALUES, WITH, WITH_ITEM, SKIP_TO_FIRST, SKIP_TO_LAST),
                AGGREGATE, DML, DDL, DAL));

    /**
     * Category of all SQL statement types.
     *
     * <p>Consists of all types in {@link #QUERY}, {@link #DML} and {@link #DDL} and {@link #DAL}.
     */
    public static final EnumSet<SqlKind> TOP_LEVEL = concat(QUERY, DML, DDL, DAL);

    /**
     * Category consisting of regular and special functions.
     *
     * <p>Consists of regular functions {@link #OTHER_FUNCTION} and special
     * functions {@link #ROW}, {@link #TRIM}, {@link #CAST}, {@link #JDBC_FN}.
     */
    public static final Set<SqlKind> FUNCTION =
        EnumSet.of(OTHER_FUNCTION, ROW, TRIM, LTRIM, RTRIM, CAST, JDBC_FN);

    /**
     * Category of SqlAvgAggFunction.
     *
     * <p>Consists of {@link #AVG}, {@link #STDDEV_POP}, {@link #STDDEV_SAMP},
     * {@link #VAR_POP}, {@link #VAR_SAMP}.
     */
    public static final Set<SqlKind> AVG_AGG_FUNCTIONS =
        EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP);

    public static final Set<SqlKind> MIN_MAX_AGG =
        EnumSet.of(MIN, MAX);

    /**
     * Category of comparison operators.
     *
     * <p>Consists of:
     * {@link #IN},
     * {@link #EQUALS},
     * {@link #NOT_EQUALS},
     * {@link #LESS_THAN},
     * {@link #GREATER_THAN},
     * {@link #LESS_THAN_OR_EQUAL},
     * {@link #GREATER_THAN_OR_EQUAL}.
     */
    public static final Set<SqlKind> COMPARISON =
        EnumSet.of(
            IN, EQUALS, NOT_EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /**
     * Lower-case name.
     */
    public final String lowerName = name().toLowerCase(Locale.ROOT);
    public final String sql;

    SqlKind() {
        sql = name();
    }

    SqlKind(String sql) {
        this.sql = sql;
    }

    /**
     * Returns the kind that corresponds to this operator but in the opposite
     * direction. Or returns this, if this kind is not reversible.
     *
     * <p>For example, {@code GREATER_THAN.reverse()} returns {@link #LESS_THAN}.
     */
    public SqlKind reverse() {
        switch (this) {
        case GREATER_THAN:
            return LESS_THAN;
        case GREATER_THAN_OR_EQUAL:
            return LESS_THAN_OR_EQUAL;
        case LESS_THAN:
            return GREATER_THAN;
        case LESS_THAN_OR_EQUAL:
            return GREATER_THAN_OR_EQUAL;
        default:
            return this;
        }
    }

    /**
     * Returns the kind that you get if you apply NOT to this kind.
     *
     * <p>For example, {@code IS_NOT_NULL.negate()} returns {@link #IS_NULL}.
     *
     * <p>For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE},
     * {@link #IS_NOT_FALSE}, nullable inputs need to be treated carefully.
     *
     * <p>{@code NOT(IS_TRUE(null))} = {@code NOT(false)} = {@code true},
     * while {@code IS_FALSE(null)} = {@code false},
     * so {@code NOT(IS_TRUE(X))} should be {@code IS_NOT_TRUE(X)}.
     * On the other hand,
     * {@code IS_TRUE(NOT(null))} = {@code IS_TRUE(null)} = {@code false}.
     *
     * <p>This is why negate() != negateNullSafe() for these operators.
     */
    public SqlKind negate() {
        switch (this) {
        case IS_TRUE:
            return IS_NOT_TRUE;
        case IS_FALSE:
            return IS_NOT_FALSE;
        case IS_NULL:
            return IS_NOT_NULL;
        case IS_NOT_TRUE:
            return IS_TRUE;
        case IS_NOT_FALSE:
            return IS_FALSE;
        case IS_NOT_NULL:
            return IS_NULL;
        case IS_DISTINCT_FROM:
            return IS_NOT_DISTINCT_FROM;
        case IS_NOT_DISTINCT_FROM:
            return IS_DISTINCT_FROM;
        default:
            return this;
        }
    }

    /**
     * Returns the kind that you get if you negate this kind.
     * To conform to null semantics, null value should not be compared.
     *
     * <p>For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE} and
     * {@link #IS_NOT_FALSE}, nullable inputs need to be treated carefully:
     *
     * <ul>
     * <li>NOT(IS_TRUE(null)) = NOT(false) = true
     * <li>IS_TRUE(NOT(null)) = IS_TRUE(null) = false
     * <li>IS_FALSE(null) = false
     * <li>IS_NOT_TRUE(null) = true
     * </ul>
     */
    public SqlKind negateNullSafe() {
        switch (this) {
        case EQUALS:
            return NOT_EQUALS;
        case NOT_EQUALS:
            return EQUALS;
        case LESS_THAN:
            return GREATER_THAN_OR_EQUAL;
        case GREATER_THAN:
            return LESS_THAN_OR_EQUAL;
        case LESS_THAN_OR_EQUAL:
            return GREATER_THAN;
        case GREATER_THAN_OR_EQUAL:
            return LESS_THAN;
        case IS_TRUE:
            return IS_FALSE;
        case IS_FALSE:
            return IS_TRUE;
        case IS_NOT_TRUE:
            return IS_NOT_FALSE;
        case IS_NOT_FALSE:
            return IS_NOT_TRUE;
        default:
            return this.negate();
        }
    }

    public static final Set<SqlKind> SARGABLE = EnumSet.of(IN,
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN_OR_EQUAL,
        BETWEEN,
        IS_NULL);

    public static final Set<SqlKind> INDEXABLE = EnumSet.of(IN,
        EQUALS,
        IS_NOT_DISTINCT_FROM,
        LESS_THAN,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN_OR_EQUAL,
        BETWEEN,
        IS_NULL,
        LIKE,
        AND,
        OR);

    /**
     * Returns whether this {@code SqlKind} belongs to a given category.
     *
     * <p>A category is a collection of kinds, not necessarily disjoint. For
     * example, QUERY is { SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY,
     * EXPLICIT_TABLE }.
     *
     * @param category Category
     * @return Whether this kind belongs to the given category
     */
    public final boolean belongsTo(Collection<SqlKind> category) {
        return category.contains(this);
    }

    @SafeVarargs
    private static <E extends Enum<E>> EnumSet<E> concat(EnumSet<E> set0,
                                                         EnumSet<E>... sets) {
        EnumSet<E> set = set0.clone();
        for (EnumSet<E> s : sets) {
            set.addAll(s);
        }
        return set;
    }

    /**
     * Category of binary arithmetic.
     *
     * <p>Consists of:
     * {@link #PLUS}
     * {@link #MINUS}
     * {@link #TIMES}
     * {@link #DIVIDE}
     * {@link #MOD}.
     */
    public static final Set<SqlKind> BINARY_ARITHMETIC =
        EnumSet.of(PLUS, MINUS, TIMES, DIVIDE, MOD);

    /**
     * Category of binary equality.
     *
     * <p>Consists of:
     * {@link #EQUALS}
     * {@link #NOT_EQUALS}
     */
    public static final Set<SqlKind> BINARY_EQUALITY =
        EnumSet.of(EQUALS, NOT_EQUALS);

    /**
     * Category of subquery without operands.
     *
     * <p>Consists of:
     * {@link #SCALAR_QUERY}
     * {@link #EXISTS}
     * {@link #NOT_EXISTS}
     */
    public static final Set<SqlKind> SUBQUERY_WITHOUT_OPERANDS =
        EnumSet.of(SCALAR_QUERY, EXISTS, NOT_EXISTS);

    /**
     * Category of nullaware subquery.
     *
     * <p>Consists of:
     * {@link #ALL}
     * {@link #NOT_IN}
     */
    public static final Set<SqlKind> SUBQUERY_NULLAWARE =
        EnumSet.of(ALL, NOT_IN);

    /**
     * Category of binary comparison.
     *
     * <p>Consists of:
     * {@link #EQUALS}
     * {@link #NOT_EQUALS}
     * {@link #GREATER_THAN}
     * {@link #GREATER_THAN_OR_EQUAL}
     * {@link #LESS_THAN}
     * {@link #LESS_THAN_OR_EQUAL}
     * {@link #IS_DISTINCT_FROM}
     * {@link #IS_NOT_DISTINCT_FROM}
     */
    public static final Set<SqlKind> BINARY_COMPARISON =
        EnumSet.of(
            EQUALS, NOT_EQUALS,
            GREATER_THAN, GREATER_THAN_OR_EQUAL,
            LESS_THAN, LESS_THAN_OR_EQUAL);

    public static final Set<SqlKind> STATISTICS_AFFINITY =
        EnumSet.of(
            EQUALS, BETWEEN,
            GREATER_THAN, GREATER_THAN_OR_EQUAL,
            LESS_THAN, LESS_THAN_OR_EQUAL);
}

// End SqlKind.java
