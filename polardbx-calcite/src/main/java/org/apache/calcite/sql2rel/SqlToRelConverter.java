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
package org.apache.calcite.sql2rel;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.TableInfo;
import org.apache.calcite.rel.core.TableModify.TableInfoNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.rel.ddl.AlterFileStorageAsOfTimestamp;
import org.apache.calcite.rel.ddl.AlterFileStorageBackup;
import org.apache.calcite.rel.ddl.AlterFileStoragePurgeBeforeTimestamp;
import org.apache.calcite.rel.ddl.AlterRule;
import org.apache.calcite.rel.ddl.AlterSystemSetConfig;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupAddPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupExtractPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupModifyPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupRenamePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rel.ddl.AlterTableRepartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.calcite.rel.ddl.ChangeConsensusRole;
import org.apache.calcite.rel.ddl.CreateDatabase;
import org.apache.calcite.rel.ddl.CreateIndex;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rel.ddl.CreateTableGroup;
import org.apache.calcite.rel.ddl.DropDatabase;
import org.apache.calcite.rel.ddl.DropFileStorage;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.rel.ddl.DropTableGroup;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.rel.ddl.MoveDatabase;
import org.apache.calcite.rel.ddl.RefreshTopology;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.rel.ddl.SequenceDdl;
import org.apache.calcite.rel.ddl.TruncateTable;
import org.apache.calcite.rel.ddl.UnArchive;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.GroupConcatCall;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.OutFileParams;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAlterFileStorage;
import org.apache.calcite.sql.SqlAlterRule;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterSystemSetConfig;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlChangeConsensusRole;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateDatabase;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateTableGroup;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDropDatabase;
import org.apache.calcite.sql.SqlDropFileStorage;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlDropTableGroup;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlMoveDatabase;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlRefreshTopology;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSequence;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.SqlUnArchive;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlUtil.SpecialIdentiFiers;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlDefaultOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.type.TableFunctionReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.AggregatingSelectScope;
import org.apache.calcite.sql.validate.CollectNamespace;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.MatchRecognizeScope;
import org.apache.calcite.sql.validate.ParameterScope;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationFirstValueException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_IN;
import static org.apache.calcite.sql.validate.SqlValidatorImpl.isImplicitKey;

/**
 * Converts a SQL parse tree (consisting of
 * {@link org.apache.calcite.sql.SqlNode} objects) into a relational algebra
 * expression (consisting of {@link org.apache.calcite.rel.RelNode} objects).
 * <p>
 * The public entry points are: {@link #convertQuery},
 * {@link #convertExpression(SqlNode)}.
 */

public class SqlToRelConverter {
    // ~ Static fields/initializers
    // ---------------------------------------------

    protected static final Logger SQL2REL_LOGGER = CalciteTrace.getSqlToRelTracer();

    private static final BigDecimal TWO = BigDecimal.valueOf(2L);

    private static final int CORRELATE_LOOP_UPPER_BAND = 1000;

    /**
     * Size of the smallest IN list that will be converted to a semijoin to a
     * static table.
     */
    public static final int DEFAULT_IN_SUB_QUERY_THRESHOLD = Integer.MAX_VALUE;

    public static final String DEFAULT_HINT_GROUP = "_____default____hint__group_";

    // ~ Instance fields
    // --------------------------------------------------------

    protected final SqlValidator validator;
    protected final RexBuilder rexBuilder;
    protected final Prepare.CatalogReader catalogReader;
    protected final RelOptCluster cluster;
    private SubQueryConverter subQueryConverter;
    protected final List<RelNode> leaves = new ArrayList<>();
    private final List<SqlDynamicParam> dynamicParamSqlNodes = new ArrayList<>();
    private final SqlOperatorTable opTab;
    protected final RelDataTypeFactory typeFactory;
    private final SqlNodeToRexConverter exprConverter;
    private int explainParamCount;
    public final SqlToRelConverter.Config config;
    protected final RelBuilder relBuilder;

    /**
     * Fields used in name resolution for correlated sub-queries.
     */
    private final Map<CorrelationId, DeferredLookup> mapCorrelToDeferred = new HashMap<>();

    /**
     * Stack of names of datasets requested by the <code>
     * TABLE(SAMPLE(&lt;datasetName&gt;, &lt;query&gt;))</code> construct.
     */
    private final Deque<String> datasetStack = new ArrayDeque<>();

    /**
     * Mapping of non-correlated sub-queries that have been converted to their
     * equivalent constants. Used to avoid re-evaluating the sub-query if it's
     * already been evaluated.
     */
    private final Map<SqlNode, RexNode> mapConvertedNonCorrSubqs = new HashMap<>();

    public final RelOptTable.ViewExpander viewExpander;

    protected final HintBlackboard hintBlackboard = new HintBlackboard();

    // ~ Constructors
    // -----------------------------------------------------------

    /**
     * Creates a converter.
     *
     * @param viewExpander Preparing statement
     * @param validator Validator
     * @param catalogReader Schema
     * @param planner Planner
     * @param rexBuilder Rex builder
     * @param convertletTable Expression converter
     */
    @Deprecated // to be removed before 2.0
    public SqlToRelConverter(RelOptTable.ViewExpander viewExpander, SqlValidator validator,
                             Prepare.CatalogReader catalogReader, RelOptPlanner planner, RexBuilder rexBuilder,
                             SqlRexConvertletTable convertletTable) {
        this(viewExpander,
            validator,
            catalogReader,
            RelOptCluster.create(planner, rexBuilder),
            convertletTable,
            Config.DEFAULT,
            Integer.MAX_VALUE);
    }

    @Deprecated // to be removed before 2.0
    public SqlToRelConverter(RelOptTable.ViewExpander viewExpander, SqlValidator validator,
                             Prepare.CatalogReader catalogReader, RelOptCluster cluster,
                             SqlRexConvertletTable convertletTable) {
        this(viewExpander, validator, catalogReader, cluster, convertletTable, Config.DEFAULT);
    }

    public SqlToRelConverter(RelOptTable.ViewExpander viewExpander, SqlValidator validator,
                             Prepare.CatalogReader catalogReader, RelOptCluster cluster,
                             SqlRexConvertletTable convertletTable, Config config) {
        this(viewExpander, validator, catalogReader, cluster, convertletTable, config, config.getInSubQueryThreshold());
    }

    /* Creates a converter. */
    public SqlToRelConverter(RelOptTable.ViewExpander viewExpander, SqlValidator validator,
                             Prepare.CatalogReader catalogReader, RelOptCluster cluster,
                             SqlRexConvertletTable convertletTable, Config config, int inSubQueryThreshold) {
        this.viewExpander = viewExpander;
        this.opTab = (validator == null) ? SqlStdOperatorTable.instance() : validator.getOperatorTable();
        this.validator = validator;
        this.catalogReader = catalogReader;
        this.subQueryConverter = new NoOpSubQueryConverter();
        this.rexBuilder = cluster.getRexBuilder();
        this.typeFactory = rexBuilder.getTypeFactory();
        this.cluster = Preconditions.checkNotNull(cluster);
        this.exprConverter = new SqlNodeToRexConverterImpl(convertletTable);
        this.explainParamCount = 0;
        this.config = new ConfigBuilder().withConfig(config).withInSubQueryThreshold(inSubQueryThreshold).build();
        this.relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    }

    // ~ Methods
    // ----------------------------------------------------------------

    /**
     * @return the RelOptCluster in use.
     */
    public RelOptCluster getCluster() {
        return cluster;
    }

    /**
     * Returns the row-expression builder.
     */
    public RexBuilder getRexBuilder() {
        return rexBuilder;
    }

    /**
     * Returns the number of dynamic parameters encountered during translation;
     * this must only be called after {@link #convertQuery}.
     *
     * @return number of dynamic parameters
     */
    public int getDynamicParamCount() {
        return dynamicParamSqlNodes.size();
    }

    /**
     * Returns the type inferred for a dynamic parameter.
     *
     * @param index 0-based index of dynamic parameter
     * @return inferred type, never null
     */
    public RelDataType getDynamicParamType(int index) {
        SqlNode sqlNode = dynamicParamSqlNodes.get(index);
        if (sqlNode == null) {
            throw Util.needToImplement("dynamic param type inference");
        }
        return validator.getValidatedNodeType(sqlNode);
    }

    /**
     * Returns the current count of the number of dynamic parameters in an
     * EXPLAIN PLAN statement.
     *
     * @param increment if true, increment the count
     * @return the current count before the optional increment
     */
    public int getDynamicParamCountInExplain(boolean increment) {
        int retVal = explainParamCount;
        if (increment) {
            ++explainParamCount;
        }
        return retVal;
    }

    /**
     * @return mapping of non-correlated sub-queries that have been converted to
     * the constants that they evaluate to
     */
    public Map<SqlNode, RexNode> getMapConvertedNonCorrSubqs() {
        return mapConvertedNonCorrSubqs;
    }

    /**
     * Adds to the current map of non-correlated converted sub-queries the
     * elements from another map that contains non-correlated sub-queries that
     * have been converted by another SqlToRelConverter.
     *
     * @param alreadyConvertedNonCorrSubqs the other map
     */
    public void addConvertedNonCorrSubqs(Map<SqlNode, RexNode> alreadyConvertedNonCorrSubqs) {
        mapConvertedNonCorrSubqs.putAll(alreadyConvertedNonCorrSubqs);
    }

    /**
     * Sets a new SubQueryConverter. To have any effect, this must be called
     * before any convert method.
     *
     * @param converter new SubQueryConverter
     */
    public void setSubQueryConverter(SubQueryConverter converter) {
        subQueryConverter = converter;
    }

    /**
     * Sets the number of dynamic parameters in the current EXPLAIN PLAN
     * statement.
     *
     * @param explainParamCount number of dynamic parameters in the statement
     */
    public void setDynamicParamCountInExplain(int explainParamCount) {
        assert config.isExplain();
        this.explainParamCount = explainParamCount;
    }

    private void checkConvertedType(SqlNode query, RelNode result) {
        if (query.isA(SqlKind.DML)) {
            return;
        }
        // Verify that conversion from SQL to relational algebra did
        // not perturb any type information. (We can't do this if the
        // SQL statement is something like an INSERT which has no
        // validator type information associated with its result,
        // hence the namespace check above.)
        final List<RelDataTypeField> validatedFields = validator.getValidatedNodeType(query).getFieldList();
        final RelDataType validatedRowType = validator.getTypeFactory().createStructType(Pair.right(validatedFields),
            SqlValidatorUtil.uniquify(Pair.left(validatedFields), catalogReader.nameMatcher().isCaseSensitive()));

        final List<RelDataTypeField> convertedFields = result.getRowType().getFieldList().subList(0,
            validatedFields.size());
        final RelDataType convertedRowType = validator.getTypeFactory().createStructType(convertedFields);

        if (!RelOptUtil
            .equal("validated row type", validatedRowType, "converted row type", convertedRowType, Litmus.IGNORE)) {
            throw new AssertionError(
                "Conversion to relational algebra failed to " + "preserve datatypes:\n" + "validated type:\n"
                    + validatedRowType.getFullTypeString() + "\nconverted type:\n"
                    + convertedRowType.getFullTypeString() + "\nrel:\n" + RelOptUtil.toString(result));
        }
    }

    public RelNode flattenTypes(RelNode rootRel, boolean restructure) {
        RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(rexBuilder,
            createToRelContext(),
            restructure);
        return typeFlattener.rewrite(rootRel);
    }

    /**
     * If sub-query is correlated and decorrelation is enabled, performs
     * decorrelation.
     *
     * @param query Query
     * @param rootRel Root relational expression
     * @return New root relational expression after decorrelation
     */
    public RelNode decorrelate(SqlNode query, RelNode rootRel) {
        if (!enableDecorrelation()) {
            return rootRel;
        }
        final RelNode result = decorrelateQuery(rootRel);
        if (result != rootRel) {
            checkConvertedType(query, result);
        }
        return result;
    }

    /**
     * Walks over a tree of relational expressions, replacing each
     * {@link RelNode} with a 'slimmed down' relational expression that projects
     * only the fields required by its consumer.
     * <p>
     * This may make things easier for the optimizer, by removing crud that
     * would expand the search space, but is difficult for the optimizer itself
     * to do it, because optimizer rules must preserve the number and type of
     * fields. Hence, this transform that operates on the entire tree, similar
     * to the {@link RelStructuredTypeFlattener type-flattening transform}.
     * <p>
     * Currently this functionality is disabled in farrago/luciddb; the default
     * implementation of this method does nothing.
     *
     * @param ordered Whether the relational expression must produce results in
     * a particular order (typically because it has an ORDER BY at top level)
     * @param rootRel Relational expression that is at the root of the tree
     * @return Trimmed relational expression
     */
    public RelNode trimUnusedFields(boolean ordered, RelNode rootRel) {
        // Trim fields that are not used by their consumer.
        if (isTrimUnusedFields()) {
            final RelFieldTrimmer trimmer = newFieldTrimmer();
            final List<RelCollation> collations = rootRel.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
            rootRel = trimmer.trim(rootRel);
            if (!ordered && collations != null && !collations.isEmpty()
                && !collations.equals(ImmutableList.of(RelCollations.EMPTY))) {
                final RelTraitSet traitSet = rootRel.getTraitSet().replace(RelCollationTraitDef.INSTANCE, collations);
                rootRel = rootRel.copy(traitSet, rootRel.getInputs());
            }
            if (SQL2REL_LOGGER.isDebugEnabled()) {
                SQL2REL_LOGGER.debug(RelOptUtil.dumpPlan("Plan after trimming unused fields",
                    rootRel,
                    SqlExplainFormat.TEXT,
                    SqlExplainLevel.EXPPLAN_ATTRIBUTES));
            }
        }
        return rootRel;
    }

    /**
     * Creates a RelFieldTrimmer.
     *
     * @return Field trimmer
     */
    protected RelFieldTrimmer newFieldTrimmer() {
        return new RelFieldTrimmer(validator, relBuilder);
    }

    /**
     * Converts an unvalidated query's parse tree into a relational expression.
     *
     * @param query Query to convert
     * @param needsValidation Whether to validate the query before converting;
     * <code>false</code> if the query has already been validated.
     * @param top Whether the query is top-level, say if its result will become
     * a JDBC result set; <code>false</code> if the query will be part of a
     * view.
     */
    public RelRoot convertQuery(SqlNode query, final boolean needsValidation, final boolean top) {
        if (needsValidation) {
            query = validator.validate(query);
        }

        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(cluster.getMetadataProvider()));
        RelNode result = convertQueryRecursive(query, top, null).rel;
        if (top) {
            if (isStream(query)) {
                result = new LogicalDelta(cluster, result.getTraitSet(), result);
            }
        } else if ((query instanceof TDDLSqlSelect) && ((TDDLSqlSelect) query).withLock()) {
            throw new NotSupportException("For update not in root ");
        }
        RelCollation collation = RelCollations.EMPTY;
        if (!query.isA(SqlKind.DML)) {
            if (isOrdered(query)) {
                collation = requiredCollation(result);
            }
        }
        // checkConvertedType(query, result);

        if (SQL2REL_LOGGER.isDebugEnabled()) {
            SQL2REL_LOGGER.debug(RelOptUtil.dumpPlan("Plan after converting SqlNode to RelNode",
                result,
                SqlExplainFormat.TEXT,
                SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        final RelDataType validatedRowType = validator.getValidatedNodeType(query);
        return RelRoot.of(result, validatedRowType, query.getKind()).withCollation(collation);
    }

    private static boolean isStream(SqlNode query) {
        return query instanceof SqlSelect && ((SqlSelect) query).isKeywordPresent(SqlSelectKeyword.STREAM);
    }

    public static boolean isOrdered(SqlNode query) {
        switch (query.getKind()) {
        case SELECT:
            return ((SqlSelect) query).getOrderList() != null && ((SqlSelect) query).getOrderList().size() > 0;
        case WITH:
            return isOrdered(((SqlWith) query).body);
        case ORDER_BY:
            return ((SqlOrderBy) query).orderList.size() > 0;
        default:
            return false;
        }
    }

    private RelCollation requiredCollation(RelNode r) {
        if (r instanceof Sort) {
            return ((Sort) r).collation;
        }
        if (r instanceof Project) {
            return requiredCollation(((Project) r).getInput());
        }
        if (r instanceof Delta) {
            return requiredCollation(((Delta) r).getInput());
        }
        if (r instanceof LogicalOutFile) {
            return requiredCollation(((LogicalOutFile) r).getInput());
        }
        throw new AssertionError();
    }

    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public RelNode convertSelect(SqlSelect select, boolean top) {
        if (!top && (select instanceof TDDLSqlSelect) && ((TDDLSqlSelect) select).withLock()) {
            throw new NotSupportException("For update not in root ");
        }
        final SqlValidatorScope selectScope = validator.getWhereScope(select);
        final Blackboard bb = createBlackboard(selectScope, null, top);
        convertSelectImpl(bb, select);
        return bb.root;
    }

    /**
     * Factory method for creating translation workspace.
     */
    protected Blackboard createBlackboard(SqlValidatorScope scope, Map<String, RexNode> nameToNodeMap, boolean top) {
        return new Blackboard(scope, nameToNodeMap, top);
    }

    protected Blackboard createBlackboard(SqlValidatorScope scope, Map<String, RexNode> nameToNodeMap, boolean top,
                                          Blackboard current) {
        return new Blackboard(scope, nameToNodeMap, top, current);
    }

    /**
     * Implementation of {@link #convertSelect(SqlSelect, boolean)}; derived
     * class may override.
     */
    protected void convertSelectImpl(final Blackboard bb, SqlSelect select) {
        this.hintBlackboard.beginSelect(select);

        bb.registerQbName(this.hintBlackboard.currentGroups());

        try {
            convertFrom(bb, select.getFrom());
        } finally {
            this.hintBlackboard.endFrom();
        }

        convertWhere(bb, select.getWhere(), select.getOrderList() != null || select.getFetch() != null);

        final List<SqlNode> orderExprList = new ArrayList<>();
        final List<RelFieldCollation> collationList = new ArrayList<>();
        gatherOrderExprs(bb, select, select.getOrderList(), orderExprList, collationList);
        final RelCollation collation = cluster.traitSet().canonize(RelCollations.of(collationList));

        if (validator.isAggregate(select)) {
            convertAgg(bb, select, orderExprList);
        } else {
            convertSelectList(bb, select, orderExprList);
        }
        convertOrder(select, bb, collation, orderExprList, select.getOffset(), select.getFetch());

        convertOutFile(bb, select.getOutFileParams());

        bb.setRoot(bb.root, true);

        bb.cleanQbName();
    }

    private String stringValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) sqlNode).getNlsString().getValue();

        }
        return sqlNode.toString();
    }

    /**
     * Having translated 'SELECT ... FROM ... [GROUP BY ...] [HAVING ...]', adds
     * a relational expression to make the results unique.
     * <p>
     * If the SELECT clause contains duplicate expressions, adds
     * {@link org.apache.calcite.rel.logical.LogicalProject}s so that we are
     * grouping on the minimal set of keys. The performance gain isn't huge, but
     * it is difficult to detect these duplicate expressions later.
     *
     * @param bb Blackboard
     * @param checkForDupExprs Check for duplicate expressions
     */
    private void distinctify(Blackboard bb, List<SqlNode> orderExprList, List<String> orderNameList,
                             boolean checkForDupExprs) {
        // Look for duplicate expressions in the project.
        // Say we have 'select x, y, x, z'.
        // Then dups will be {[2, 0]}
        // and oldToNew will be {[0, 0], [1, 1], [2, 0], [3, 2]}
        RelNode rel = bb.root;
        if (checkForDupExprs && (rel instanceof LogicalProject)) {
            LogicalProject project = (LogicalProject) rel;
            final List<RexNode> projectExprs = project.getProjects();
            final List<Integer> origins = new ArrayList<>();
            int dupCount = 0;
            for (int i = 0; i < projectExprs.size(); i++) {
                int x = findExpr(projectExprs.get(i), projectExprs, i);
                if (x >= 0) {
                    origins.add(x);
                    ++dupCount;
                } else {
                    origins.add(i);
                }
            }
            if (dupCount == 0) {
                distinctify(bb, orderExprList, orderNameList, false);
                return;
            }

            final Map<Integer, Integer> squished = Maps.newHashMap();
            final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
            final List<Pair<RexNode, String>> newProjects = Lists.newArrayList();
            for (int i = 0; i < fields.size(); i++) {
                if (origins.get(i) == i) {
                    squished.put(i, newProjects.size());
                    newProjects.add(RexInputRef.of2(i, fields));
                }
            }
            rel = LogicalProject.create(rel, Pair.left(newProjects), Pair.right(newProjects));
            bb.root = rel;
            distinctify(bb, orderExprList, orderNameList, false);
            rel = bb.root;

            // Create the expressions to reverse the mapping.
            // Project($0, $1, $0, $2).
            final List<Pair<RexNode, String>> undoProjects = Lists.newArrayList();
            for (int i = 0; i < fields.size(); i++) {
                final int origin = origins.get(i);
                RelDataTypeField field = fields.get(i);
                undoProjects
                    .add(Pair.of((RexNode) new RexInputRef(squished.get(origin), field.getType()), field.getName()));
            }

            rel = LogicalProject.create(rel, Pair.left(undoProjects), Pair.right(undoProjects));
            bb.setRoot(rel, false);

            return;
        }

        // Usual case: all of the expressions in the SELECT clause are
        // different.
        final ImmutableBitSet groupSet = ImmutableBitSet
            .range(rel.getRowType().getFieldCount() - (orderExprList == null ? 0 : orderExprList.size()));

        List<AggregateCall> list = new ArrayList<>();
        if (orderExprList != null) {
            for (int i = 0; i < orderExprList.size(); ++i) {
                final SqlNode order = orderExprList.get(i);
                final String name = orderNameList.get(i);

                SqlCall aggCall = SqlStdOperatorTable.__FIRST_VALUE.createCall(SqlParserPos.ZERO, order);
                final List<Integer> refList = new ArrayList<>();
                final List<RelDataType> operandTypeList = new ArrayList<>();

                String aggName = aggCall.toString();
                final SqlAggFunction aggFuncOp = (SqlAggFunction) aggCall.getOperator();
                if (aggCall.getKind() == SqlKind.AS) {
                    List<SqlNode> operandList = aggCall.getOperandList();
                    aggName = operandList.get(1).toString();
                }

                final List<RelDataTypeField> inputFields = rel.getRowType().getFieldList();
                for (int index = 0; index < inputFields.size(); index++) {
                    if (TStringUtil.equalsIgnoreCase(inputFields.get(index).getName(), name)) {
                        refList.add(index);
                        operandTypeList.add(inputFields.get(index).getType());
                        break;
                    }
                }

                final RelDataType type = aggFuncOp.inferReturnType(relBuilder.getTypeFactory(), operandTypeList);
                final AggregateCall aggregateCall =
                    AggregateCall.create(aggFuncOp, false, false, refList, -1, type, aggName);
                list.add(aggregateCall);
            }
        }
        rel = createAggregate(bb, groupSet, ImmutableList.of(groupSet), ImmutableList.<AggregateCall>copyOf(list));

        bb.setRoot(rel, false);

    }

    private int findExpr(RexNode seek, List<RexNode> exprs, int count) {
        for (int i = 0; i < count; i++) {
            RexNode expr = exprs.get(i);
            if (expr.toString().equals(seek.toString())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Converts a query's ORDER BY clause, if any.
     *
     * @param select Query
     * @param bb Blackboard
     * @param collation Collation list
     * @param orderExprList Method populates this list with orderBy expressions
     * not present in selectList
     * @param offset Expression for number of rows to discard before returning
     * first row
     * @param fetch Expression for number of rows to fetch
     */
    protected void convertOrder(SqlSelect select, Blackboard bb, RelCollation collation, List<SqlNode> orderExprList,
                                SqlNode offset, SqlNode fetch) {
        if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
            assert collation.getFieldCollations().isEmpty();
            if ((offset == null
                || (offset instanceof SqlLiteral && ((SqlLiteral) offset).bigDecimalValue().equals(BigDecimal.ZERO)))
                && fetch == null) {
                return;
            }
        }

        // Create a sorter using the previously constructed collations.
        bb.setRoot(LogicalSort.create(bb.root,
            collation,
            offset == null ? null : convertExpression(offset),
            fetch == null ? null : convertExpression(fetch)), false);

        // If extra expressions were added to the project list for sorting,
        // add another project to remove them. But make the collation empty,
        // because
        // we can't represent the real collation.
        //
        // If it is the top node, use the real collation, but don't trim fields.
        if (orderExprList.size() > 0) {
            final List<RexNode> exprs = new ArrayList<>();
            final RelDataType rowType = bb.root.getRowType();
            final int fieldCount = rowType.getFieldCount() - orderExprList.size();
            for (int i = 0; i < fieldCount; i++) {
                exprs.add(rexBuilder.makeInputRef(bb.root, i));
            }
            bb.setRoot(LogicalProject.create(bb.root, exprs, rowType.getFieldNames().subList(0, fieldCount)), false);
        }
    }

    /**
     * Returns whether a given node contains a {@link SqlInOperator}.
     *
     * @param node a RexNode tree
     */
    private static boolean containsInOperator(SqlNode node) {
        try {
            SqlVisitor<Void> visitor = new SqlBasicVisitor<Void>() {

                public Void visit(SqlCall call) {
                    if (call.getOperator() instanceof SqlInOperator) {
                        throw new Util.FoundOne(call);
                    }
                    return super.visit(call);
                }
            };
            node.accept(visitor);
            return false;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return true;
        }
    }

    /**
     * Push down all the NOT logical operators into any IN/NOT IN operators.
     *
     * @param scope Scope where {@code sqlNode} occurs
     * @param sqlNode the root node from which to look for NOT operators
     * @return the transformed SqlNode representation with NOT pushed down.
     */
    private static SqlNode pushDownNotForIn(SqlValidatorScope scope, SqlNode sqlNode) {
        if ((sqlNode instanceof SqlCall) && containsInOperator(sqlNode)) {
            SqlCall sqlCall = (SqlCall) sqlNode;
            if ((sqlCall.getOperator() == SqlStdOperatorTable.AND)
                || (sqlCall.getOperator() == SqlStdOperatorTable.OR)) {
                SqlNode[] sqlOperands = ((SqlBasicCall) sqlCall).operands;
                for (int i = 0; i < sqlOperands.length; i++) {
                    sqlOperands[i] = pushDownNotForIn(scope, sqlOperands[i]);
                }
                return reg(scope, sqlNode);
            } else if (sqlCall.getOperator() == SqlStdOperatorTable.NOT) {
                SqlNode childNode = sqlCall.operand(0);
                assert childNode instanceof SqlCall;
                SqlBasicCall childSqlCall = (SqlBasicCall) childNode;
                if (childSqlCall.getOperator() == SqlStdOperatorTable.AND) {
                    SqlNode[] andOperands = childSqlCall.getOperands();
                    SqlNode[] orOperands = new SqlNode[andOperands.length];
                    for (int i = 0; i < orOperands.length; i++) {
                        orOperands[i] = reg(scope,
                            SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, andOperands[i]));
                    }
                    for (int i = 0; i < orOperands.length; i++) {
                        orOperands[i] = pushDownNotForIn(scope, orOperands[i]);
                    }
                    return reg(scope,
                        SqlStdOperatorTable.OR.createCall(SqlParserPos.ZERO, orOperands[0], orOperands[1]));
                } else if (childSqlCall.getOperator() == SqlStdOperatorTable.OR) {
                    SqlNode[] orOperands = childSqlCall.getOperands();
                    SqlNode[] andOperands = new SqlNode[orOperands.length];
                    for (int i = 0; i < andOperands.length; i++) {
                        andOperands[i] = reg(scope,
                            SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, orOperands[i]));
                    }
                    for (int i = 0; i < andOperands.length; i++) {
                        andOperands[i] = pushDownNotForIn(scope, andOperands[i]);
                    }
                    return reg(scope,
                        SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, andOperands[0], andOperands[1]));
                } else if (childSqlCall.getOperator() == SqlStdOperatorTable.NOT) {
                    SqlNode[] notOperands = childSqlCall.getOperands();
                    assert notOperands.length == 1;
                    return pushDownNotForIn(scope, notOperands[0]);
                } else if (childSqlCall.getOperator() instanceof SqlInOperator) {
                    SqlNode[] inOperands = childSqlCall.getOperands();
                    SqlInOperator inOp = (SqlInOperator) childSqlCall.getOperator();
                    if (inOp.kind == SqlKind.NOT_IN) {
                        return reg(scope,
                            IN.createCall(SqlParserPos.ZERO, inOperands[0], inOperands[1]));
                    } else {
                        return reg(scope,
                            NOT_IN.createCall(SqlParserPos.ZERO, inOperands[0], inOperands[1]));
                    }
                } else {
                    // childSqlCall is "leaf" node in a logical expression tree
                    // (only considering AND, OR, NOT)
                    return sqlNode;
                }
            } else {
                // sqlNode is "leaf" node in a logical expression tree
                // (only considering AND, OR, NOT)
                return sqlNode;
            }
        } else {
            // tree rooted at sqlNode does not contain inOperator
            return sqlNode;
        }
    }

    /**
     * Registers with the validator a {@link SqlNode} that has been created
     * during the Sql-to-Rel process.
     */
    private static SqlNode reg(SqlValidatorScope scope, SqlNode e) {
        scope.getValidator().deriveType(scope, e);
        return e;
    }

    /**
     * Converts a WHERE clause.
     *
     * @param bb Blackboard
     * @param where WHERE clause, may be null
     */
    private void convertWhere(final Blackboard bb, final SqlNode where, boolean containOrder) {
        if (where == null) {
            return;
        }
        SqlNode newWhere = pushDownNotForIn(bb.scope, where);

        findSubQueries(bb, newWhere, RelOptUtil.Logic.UNKNOWN_AS_FALSE, false, false);
        boolean alreadyConvertSemiJoin = false;
        //保险起见这里对不含order做semiJoin优化，包含order转bkaJoin效率不好
        if (!containOrder && bb.isSupportInvalueSemiJoin()) {
            for (SubQuery node : bb.subQueryList) {
                boolean ret = substituteJoinSubQuery(bb, node);
                alreadyConvertSemiJoin = ret || alreadyConvertSemiJoin;
            }
        }

        boolean oldValue = bb.isIgnoreConvertExtendedExpression();
        bb.setIgnoreConvertExtendedExpression(alreadyConvertSemiJoin);
        final RexNode convertedWhere = bb.convertExpression(newWhere);
        bb.setIgnoreConvertExtendedExpression(oldValue);

        final RexNode convertedWhere2 =
            RexUtil.removeNullabilityCast(typeFactory, convertedWhere);
        /**
         * For CoronaDB, DO NOT optimize it.
         */
        // only allocate filter if the condition is not TRUE
        if (convertedWhere2.isAlwaysTrue()) {
            return;
        }

        final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
        final RelNode filter = factory.createFilter(bb.root, convertedWhere);
        final RelNode r;
        CorrelationUse p = null;
        try {
            p = getCorrelationUse(bb, filter, null, null, null);
        } catch (ValidationFirstValueException e) {
            throw new AssertionError(e.getMessage());
        }
        if (p != null) {
            assert p.r instanceof Filter;
            Filter f = (Filter) p.r;
            r = LogicalFilter.create(f.getInput(), f.getCondition(), ImmutableSet.of(p.id));
        } else {
            r = filter;
        }

        bb.setRoot(r, false);
    }

    private void replaceSubQueries(final Blackboard bb, final SqlNode expr, RelOptUtil.Logic logic) {
        findSubQueries(bb, expr, logic, false, false);
//        for (SubQuery node : bb.subQueryList) {
//            substituteSubQuery(bb, node);
//        }
    }

    private boolean substituteJoinSubQuery(Blackboard bb, SubQuery subQuery) {
        final RexNode expr = subQuery.expr;
        if (expr != null) {
            // Already done.
            return false;
        }

        final SqlBasicCall call;
        final RelNode rel;
        final SqlNode query;
        final RelOptUtil.Exists converted;
        //只是将In values 裁剪转成SemiJoin处理
        switch (subQuery.node.getKind()) {
        case IN:
//      case NOT_IN:
//        case SOME:
//        case ALL:
            call = (SqlBasicCall) subQuery.node;
            query = call.operand(1);
            if (!config.isExpand() && !(query instanceof SqlNodeList)) {
                return false;
            }
            final SqlNode leftKeyNode = call.operand(0);

            final List<RexNode> leftKeys;
            final List<String> columnNames = Lists.newArrayList();
            switch (leftKeyNode.getKind()) {
            case ROW:
                leftKeys = Lists.newArrayList();
                for (SqlNode sqlExpr : ((SqlBasicCall) leftKeyNode).getOperandList()) {
                    leftKeys.add(bb.convertExpression(sqlExpr));

                    if (sqlExpr instanceof SqlIdentifier) {
                        columnNames.add(((SqlIdentifier) sqlExpr).getLastName().toLowerCase());
                    }
                }
                break;
            default:
                leftKeys = ImmutableList.of(bb.convertExpression(leftKeyNode));
                if (leftKeyNode instanceof SqlIdentifier) {
                    columnNames.add(((SqlIdentifier) leftKeyNode).getLastName().toLowerCase());
                }
            }

            if (query instanceof SqlNodeList) {
                if (supportInValuesConvertJoin(columnNames, (SqlNodeList) query, bb)) {
                    final RelDataType targetRowType = SqlTypeUtil
                        .promoteToRowType(typeFactory, validator.getValidatedNodeType(leftKeyNode), null);
                    final boolean notIn = call.getOperator().kind == SqlKind.NOT_IN;
                    converted = convertExists(query, RelOptUtil.SubQueryType.IN, subQuery.logic, notIn, targetRowType);
                    if (converted.indicator) {
                        // Generate
                        // emp CROSS JOIN (SELECT COUNT(*) AS c,
                        // COUNT(deptno) AS ck FROM dept)
                        final RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);
                        final RelNode seek = converted.r.getInput(0); // fragile
                        final int keyCount = leftKeys.size();
                        final List<Integer> args = ImmutableIntList.range(0, keyCount);
                        LogicalAggregate aggregate = LogicalAggregate.create(seek,
                            ImmutableBitSet.of(),
                            null,
                            ImmutableList.of(
                                AggregateCall.create(SqlStdOperatorTable.COUNT,
                                    false,
                                    false,
                                    ImmutableList.<Integer>of(),
                                    -1,
                                    longType,
                                    null),
                                AggregateCall
                                    .create(SqlStdOperatorTable.COUNT, false, false, args, -1, longType, null)));
                        LogicalJoin join = LogicalJoin.create(bb.root,
                            aggregate,
                            rexBuilder.makeLiteral(true),
                            ImmutableSet.<CorrelationId>of(),
                            JoinRelType.INNER);
                        bb.setRoot(join, false);
                    }
                    final RexNode rex = bb
                        .register(converted.r, converted.outerJoin ? JoinRelType.LEFT : JoinRelType.INNER, leftKeys);

                    RelOptUtil.Logic logic = subQuery.logic;
                    switch (logic) {
                    case TRUE_FALSE_UNKNOWN:
                    case UNKNOWN_AS_TRUE:
                        if (!converted.indicator) {
                            logic = RelOptUtil.Logic.TRUE_FALSE;
                        }
                    }
                    subQuery.expr = translateIn(logic, bb.root, rex);
                    if (notIn) {
                        subQuery.expr = rexBuilder.makeCall(SqlStdOperatorTable.NOT, subQuery.expr);
                    }
                    return true;
                }
            }
            return false;
        default:
            return false;
        }
    }

    private RexNode translateIn(RelOptUtil.Logic logic, RelNode root, final RexNode rex) {
        switch (logic) {
        case TRUE:
            return rexBuilder.makeLiteral(true);

        case TRUE_FALSE:
        case UNKNOWN_AS_FALSE:
            assert rex instanceof RexRangeRef;
            final int fieldCount = rex.getType().getFieldCount();
            RexNode rexNode = rexBuilder.makeFieldAccess(rex, fieldCount - 1);
            rexNode = rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, rexNode);

            // Then append the IS NOT NULL(leftKeysForIn).
            //
            // RexRangeRef contains the following fields:
            // leftKeysForIn,
            // rightKeysForIn (the original sub-query select list),
            // nullIndicator
            //
            // The first two lists contain the same number of fields.
            final int k = (fieldCount - 1) / 2;
            for (int i = 0; i < k; i++) {
                rexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                    rexNode,
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeFieldAccess(rex, i)));
            }
            return rexNode;

        case TRUE_FALSE_UNKNOWN:
        case UNKNOWN_AS_TRUE:
            // select e.deptno,
            // case
            // when ct.c = 0 then false
            // when dt.i is not null then true
            // when e.deptno is null then null
            // when ct.ck < ct.c then null
            // else false
            // end
            // from e
            // cross join (select count(*) as c, count(deptno) as ck from v)
            // as ct
            // left join (select distinct deptno, true as i from v) as dt
            // on e.deptno = dt.deptno
            final Join join = (Join) root;
            final Project left = (Project) join.getLeft();
            final RelNode leftLeft = ((Join) left.getInput()).getLeft();
            final int leftLeftCount = leftLeft.getRowType().getFieldCount();
            final RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);
            final RexNode cRef = rexBuilder.makeInputRef(root, leftLeftCount);
            final RexNode ckRef = rexBuilder.makeInputRef(root, leftLeftCount + 1);
            final RexNode iRef = rexBuilder.makeInputRef(root, root.getRowType().getFieldCount() - 1);

            final RexLiteral zero = rexBuilder.makeIntLiteral(0);
            final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
            final RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
            final RexNode unknownLiteral = rexBuilder.makeNullLiteral(trueLiteral.getType());

            final ImmutableList.Builder<RexNode> args = ImmutableList.builder();
            args.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, cRef, zero),
                falseLiteral,
                rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, iRef),
                trueLiteral);
            final JoinInfo joinInfo = join.analyzeCondition();
            for (int leftKey : joinInfo.leftKeys) {
                final RexNode kRef = rexBuilder.makeInputRef(root, leftKey);
                args.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, kRef), unknownLiteral);
            }
            args.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ckRef, cRef), unknownLiteral, falseLiteral);

            return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args.build());

        default:
            throw new AssertionError(logic);
        }
    }

    protected static boolean containsNullLiteral(SqlNodeList valueList) {
        for (SqlNode node : valueList.getList()) {
            if (node instanceof SqlLiteral) {
                SqlLiteral lit = (SqlLiteral) node;
                if (lit.getValue() == null) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determines if a sub-query is non-correlated and if so, converts it to a
     * constant.
     *
     * @param subQuery the call that references the sub-query
     * @param bb blackboard used to convert the sub-query
     * @param converted RelNode tree corresponding to the sub-query
     * @param isExists true if the sub-query is part of an EXISTS expression
     * @return Whether the sub-query can be converted to a constant
     */
    private boolean convertNonCorrelatedSubQuery(SubQuery subQuery, Blackboard bb, RelNode converted,
                                                 boolean isExists) {
        SqlCall call = (SqlBasicCall) subQuery.node;
        if (subQueryConverter.canConvertSubQuery() && isSubQueryNonCorrelated(converted, bb)) {
            // First check if the sub-query has already been converted
            // because it's a nested sub-query. If so, don't re-evaluate
            // it again.
            RexNode constExpr = mapConvertedNonCorrSubqs.get(call);
            if (constExpr == null) {
                constExpr = subQueryConverter.convertSubQuery(call, this, isExists, config.isExplain());
            }
            if (constExpr != null) {
                subQuery.expr = constExpr;
                mapConvertedNonCorrSubqs.put(call, constExpr);
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the RelNode tree for a select statement to a select that
     * produces a single value.
     *
     * @param query the query
     * @param plan the original RelNode tree corresponding to the statement
     * @return the converted RelNode tree
     */
    public RelNode convertToSingleValueSubq(SqlNode query, RelNode plan) {
        // Check whether query is guaranteed to produce a single value.
        if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            SqlNodeList selectList = select.getSelectList();
            SqlNodeList groupList = select.getGroup();

            if ((selectList.size() == 1) && ((groupList == null) || (groupList.size() == 0))) {
                SqlNode selectExpr = selectList.get(0);
                if (selectExpr instanceof SqlCall) {
                    SqlCall selectExprCall = (SqlCall) selectExpr;
                    if (Util.isSingleValue(selectExprCall)) {
                        return plan;
                    }
                }

                // If there is a limit with 0 or 1,
                // it is ensured to produce a single value
                if (select.getFetch() != null && select.getFetch() instanceof SqlNumericLiteral) {
                    SqlNumericLiteral limitNum = (SqlNumericLiteral) select.getFetch();
                    if (((BigDecimal) limitNum.getValue()).intValue() < 2) {
                        return plan;
                    }
                }
            }
        } else if (query instanceof SqlCall) {
            // If the query is (values ...),
            // it is necessary to look into the operands to determine
            // whether SingleValueAgg is necessary
            SqlCall exprCall = (SqlCall) query;
            if (exprCall.getOperator() instanceof SqlValuesOperator && Util.isSingleValue(exprCall)) {
                return plan;
            }
        }

        // If not, project SingleValueAgg
        return RelOptUtil.createSingleValueAggRel(cluster, plan);
    }

    /**
     * Converts "x IN (1, 2, ...)" to "x=1 OR x=2 OR ...".
     *
     * @param leftKeys LHS
     * @param valuesList RHS
     * @param op The operator (IN, NOT IN, &gt; SOME, ...)
     * @return converted expression
     */
    private RexNode convertInToOr(final Blackboard bb, final List<RexNode> leftKeys, SqlNodeList valuesList,
                                  SqlInOperator op) {
        final List<RexNode> comparisons = new ArrayList<>();
        for (SqlNode rightVals : valuesList) {
            RexNode rexComparison;
            final SqlOperator comparisonOp;
            if (op instanceof SqlQuantifyOperator) {
                comparisonOp = RelOptUtil.op(((SqlQuantifyOperator) op).comparisonKind, SqlStdOperatorTable.EQUALS);
            } else {
                comparisonOp = SqlStdOperatorTable.EQUALS;
            }
            if (leftKeys.size() == 1) {
                rexComparison = rexBuilder.makeCall(comparisonOp,
                    leftKeys.get(0),
                    ensureSqlType(leftKeys.get(0).getType(), bb.convertExpression(rightVals)));
            } else {
                assert rightVals instanceof SqlCall;
                final SqlBasicCall call = (SqlBasicCall) rightVals;
                assert (call.getOperator() instanceof SqlRowOperator) && call.operandCount() == leftKeys.size();
                rexComparison = RexUtil.composeConjunction(rexBuilder,
                    Iterables.transform(Pair.zip(leftKeys, call.getOperandList()),
                        new Function<Pair<RexNode, SqlNode>, RexNode>() {

                            public RexNode apply(Pair<RexNode, SqlNode> pair) {
                                return rexBuilder.makeCall(comparisonOp,
                                    pair.left,
                                    ensureSqlType(pair.left.getType(), bb.convertExpression(pair.right)));
                            }
                        }),
                    false);
            }
            comparisons.add(rexComparison);
        }

        switch (op.kind) {
        case ALL:
            return RexUtil.composeConjunction(rexBuilder, comparisons, true);
        case NOT_IN:
            return rexBuilder.makeCall(SqlStdOperatorTable.NOT,
                RexUtil.composeDisjunction(rexBuilder, comparisons, true));
        case IN:
        case SOME:
            return RexUtil.composeDisjunction(rexBuilder, comparisons, true);
        default:
            throw new AssertionError();
        }
    }

    /**
     * Converts "x IN (1, 2, ...)" to "x=1 OR x=2 OR ...".
     *
     * @param leftKeys LHS
     * @param valuesList RHS
     * @param op The operator (IN, NOT IN, &gt; SOME, ...)
     * @return converted expression
     */
    private RexNode convertIn(final Blackboard bb, final List<RexNode> leftKeys, SqlNodeList valuesList,
                              SqlInOperator op) {
        final List<RexNode> comparisons = new ArrayList<>();
        for (SqlNode rightVals : valuesList) {
            RexNode rexComparison;
            final SqlOperator comparisonOp;
            if (op instanceof SqlQuantifyOperator) {
                comparisonOp = RelOptUtil.op(((SqlQuantifyOperator) op).comparisonKind, SqlStdOperatorTable.EQUALS);
            } else {
                comparisonOp = SqlStdOperatorTable.EQUALS;
            }
            if (leftKeys.size() == 1) {
                rexComparison = rexBuilder.makeCall(comparisonOp,
                    leftKeys.get(0),
                    ensureSqlType(leftKeys.get(0).getType(), bb.convertExpression(rightVals)));
            } else {
                assert rightVals instanceof SqlCall;
                final SqlBasicCall call = (SqlBasicCall) rightVals;
                assert (call.getOperator() instanceof SqlRowOperator) && call.operandCount() == leftKeys.size();
                rexComparison = RexUtil.composeConjunction(rexBuilder,
                    Iterables.transform(Pair.zip(leftKeys, call.getOperandList()),
                        new Function<Pair<RexNode, SqlNode>, RexNode>() {

                            public RexNode apply(Pair<RexNode, SqlNode> pair) {
                                return rexBuilder.makeCall(comparisonOp,
                                    pair.left,
                                    ensureSqlType(pair.left.getType(), bb.convertExpression(pair.right)));
                            }
                        }),
                    false);
            }
            comparisons.add(rexComparison);
        }

        switch (op.kind) {
        case ALL:
            return RexUtil.composeConjunction(rexBuilder, comparisons, true);
        case NOT_IN:
            return rexBuilder.makeCall(SqlStdOperatorTable.NOT,
                RexUtil.composeDisjunction(rexBuilder, comparisons, true));
        case IN:
        case SOME:
            return RexUtil.composeDisjunction(rexBuilder, comparisons, true);
        default:
            throw new AssertionError();
        }
    }

    /**
     * Ensures that an expression has a given {@link SqlTypeName}, applying a
     * cast if necessary. If the expression already has the right type family,
     * returns the expression unchanged.
     */
    private RexNode ensureSqlType(RelDataType type, RexNode node) {
        if (type.getSqlTypeName() == node.getType().getSqlTypeName()
            || (type.getSqlTypeName() == SqlTypeName.VARCHAR && node.getType().getSqlTypeName() == SqlTypeName.CHAR)) {
            return node;
        }
        return rexBuilder.ensureType(type, node, true);
    }

    /**
     * Gets the list size threshold under which {@link #convertInToOr} is used.
     * Lists of this size or greater will instead be converted to use a join
     * against an inline table
     * ({@link org.apache.calcite.rel.logical.LogicalValues}) rather than a
     * predicate. A threshold of 0 forces usage of an inline table in all cases;
     * a threshold of Integer.MAX_VALUE forces usage of OR in all cases
     *
     * @return threshold, default {@link #DEFAULT_IN_SUB_QUERY_THRESHOLD}
     */
    @Deprecated // to be removed before 2.0
    protected int getInSubqueryThreshold() {
        return config.getInSubQueryThreshold();
    }

    /**
     * Converts an EXISTS or IN predicate into a join. For EXISTS, the sub-query
     * produces an indicator variable, and the result is a relational expression
     * which outer joins that indicator to the original query. After performing
     * the outer join, the condition will be TRUE if the EXISTS condition holds,
     * NULL otherwise.
     *
     * @param seek A query, for example 'select * from emp' or 'values (1,2,3)'
     * or '('Foo', 34)'.
     * @param subQueryType Whether sub-query is IN, EXISTS or scalar
     * @param logic Whether the answer needs to be in full 3-valued logic (TRUE,
     * FALSE, UNKNOWN) will be required, or whether we can accept an
     * approximation (say representing UNKNOWN as FALSE)
     * @param notIn Whether the operation is NOT IN
     * @return join expression
     */
    private RelOptUtil.Exists convertExists(SqlNode seek, RelOptUtil.SubQueryType subQueryType, RelOptUtil.Logic logic,
                                            boolean notIn, RelDataType targetDataType) {
        final SqlValidatorScope seekScope = (seek instanceof SqlSelect) ? validator
            .getSelectScope((SqlSelect) seek) : null;
        final Blackboard seekBb = createBlackboard(seekScope, null, false);
        RelNode seekRel = convertQueryOrInList(seekBb, seek, targetDataType);

        return RelOptUtil.createExistsPlan(seekRel, subQueryType, logic, notIn);
    }

    private RelNode convertQueryOrInList(Blackboard bb, SqlNode seek, RelDataType targetRowType) {
        // NOTE: Once we start accepting single-row queries as row constructors,
        // there will be an ambiguity here for a case like X IN ((SELECT Y FROM
        // Z)). The SQL standard resolves the ambiguity by saying that a lone
        // select should be interpreted as a table expression, not a row
        // expression. The semantic difference is that a table expression can
        // return multiple rows.
        if (seek instanceof SqlNodeList) {
            boolean allIsDynamic = ((SqlNodeList) seek).getList().stream().anyMatch(t -> t instanceof SqlDynamicParam);
            boolean bMatchAllLiteral = ((SqlNodeList) seek).getList().stream().allMatch(node ->
                (node instanceof SqlLiteral) && ((SqlLiteral) node).getValue() != null
            );
            if (allIsDynamic || bMatchAllLiteral) {
                return convertDynamicRowValues(bb, seek, ((SqlNodeList) seek).getList(), false, targetRowType);
            } else {
                return convertRowValues(bb, seek, ((SqlNodeList) seek).getList(), false, targetRowType);
            }
        } else {
            return convertQueryRecursive(seek, false, null).project();
        }
    }

    private RelNode convertRowValues(Blackboard bb, SqlNode rowList, Collection<SqlNode> rows,
                                     boolean allowLiteralsOnly, RelDataType targetRowType) {
        // NOTE jvs 30-Apr-2006: We combine all rows consisting entirely of
        // literals into a single LogicalValues; this gives the optimizer a
        // smaller
        // input tree. For everything else (computed expressions, row
        // sub-queries), we union each row in as a projection on top of a
        // LogicalOneRow.

        final ImmutableList.Builder<ImmutableList<RexLiteral>> tupleList = ImmutableList.builder();
        final RelDataType rowType;
        if (targetRowType != null) {
            rowType = targetRowType;
        } else {
            rowType = SqlTypeUtil.promoteToRowType(typeFactory, validator.getValidatedNodeType(rowList), null);
        }

        final List<RelNode> unionInputs = new ArrayList<>();
        for (SqlNode node : rows) {
            SqlBasicCall call;
            if (isRowConstructor(node)) {
                call = (SqlBasicCall) node;
                ImmutableList.Builder<RexLiteral> tuple = ImmutableList.builder();
                for (Ord<SqlNode> operand : Ord.zip(call.operands)) {
                    RexLiteral rexLiteral = convertLiteralInValuesList(operand.e, bb, rowType, operand.i);
                    if ((rexLiteral == null) && allowLiteralsOnly) {
                        return null;
                    }
                    if ((rexLiteral == null) || !config.isCreateValuesRel()) {
                        // fallback to convertRowConstructor
                        tuple = null;
                        break;
                    }
                    tuple.add(rexLiteral);
                }
                if (tuple != null) {
                    tupleList.add(tuple.build());
                    continue;
                }
            } else {
                RexLiteral rexLiteral = convertLiteralInValuesList(node, bb, rowType, 0);
                if ((rexLiteral != null) && config.isCreateValuesRel()) {
                    tupleList.add(ImmutableList.of(rexLiteral));
                    continue;
                } else {
                    if ((rexLiteral == null) && allowLiteralsOnly) {
                        return null;
                    }
                }

                // convert "1" to "row(1)"
                call = (SqlBasicCall) SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, node);
            }
            unionInputs.add(convertRowConstructor(bb, call));
        }
        LogicalValues values = LogicalValues.create(cluster, rowType, tupleList.build());
        RelNode resultRel;
        if (unionInputs.isEmpty()) {
            resultRel = values;
        } else {
            if (!values.getTuples().isEmpty()) {
                unionInputs.add(values);
            }
            resultRel = LogicalUnion.create(unionInputs, true);
        }
        leaves.add(resultRel);
        return resultRel;
    }

    private RexLiteral convertLiteralInValuesList(SqlNode sqlNode, Blackboard bb, RelDataType rowType, int iField) {
        if (!(sqlNode instanceof SqlLiteral)) {
            return null;
        }
        RelDataTypeField field = rowType.getFieldList().get(iField);
        RelDataType type = field.getType();
        if (type.isStruct()) {
            // null literals for weird stuff like UDT's need
            // special handling during type flattening, so
            // don't use LogicalValues for those
            return null;
        }

        RexNode literalExpr = exprConverter.convertLiteral(bb, (SqlLiteral) sqlNode);

        if (!(literalExpr instanceof RexLiteral)) {
            assert literalExpr.isA(SqlKind.CAST);
            RexNode child = ((RexCall) literalExpr).getOperands().get(0);
            assert RexLiteral.isNullLiteral(child);

            // NOTE jvs 22-Nov-2006: we preserve type info
            // in LogicalValues digest, so it's OK to lose it here
            return (RexLiteral) child;
        }

        RexLiteral literal = (RexLiteral) literalExpr;

        if (literal.getTypeName() == SqlTypeName.NULL) {
            return literal;
        }

        // Comparable value = literal.getValue();
        //
        // if (SqlTypeUtil.isExactNumeric(type) && SqlTypeUtil.hasScale(type)) {
        // BigDecimal roundedValue =
        // NumberUtil.rescaleBigDecimal(
        // (BigDecimal) value,
        // type.getScale());
        // return rexBuilder.makeExactLiteral(
        // roundedValue,
        // type);
        // }
        //
        // if ((value instanceof NlsString)
        // && (type.getSqlTypeName() == SqlTypeName.CHAR)) {
        // // pad fixed character type
        // NlsString unpadded = (NlsString) value;
        // return rexBuilder.makeCharLiteral(
        // new NlsString(
        // Spaces.padRight(unpadded.getValue(), type.getPrecision()),
        // unpadded.getCharsetName(),
        // unpadded.getCollation()));
        // }
        return literal;
    }

    private RelNode convertDynamicRowValues(Blackboard bb, SqlNode rowList, Collection<SqlNode> rows,
                                            boolean allowLiteralsOnly, RelDataType targetRowType) {
        final ImmutableList.Builder<ImmutableList<RexNode>> tupleList = ImmutableList.builder();
        final RelDataType rowType;
        if (targetRowType != null) {
            rowType = targetRowType;
        } else {
            rowType = SqlTypeUtil.promoteToRowType(typeFactory, validator.getValidatedNodeType(rowList), null);
        }

        final List<RelNode> unionInputs = new ArrayList<>();
        for (SqlNode node : rows) {
            SqlBasicCall call;
            if (isRowConstructor(node)) {
                call = (SqlBasicCall) node;
                ImmutableList.Builder<RexNode> tuple = ImmutableList.builder();
                for (Ord<SqlNode> operand : Ord.zip(call.operands)) {
                    RexDynamicParam rexDynamicParam = null;
                    if (operand.e instanceof SqlDynamicParam) {
                        rexDynamicParam = convertDynamicParam((SqlDynamicParam) operand.e);
                    }
                    if ((rexDynamicParam == null) && allowLiteralsOnly) {
                        return null;
                    }
                    if ((rexDynamicParam == null) || !config.isCreateValuesRel()) {
                        // fallback to convertRowConstructor
                        tuple = null;
                        break;
                    }
                    tuple.add(rexDynamicParam);
                }
                if (tuple != null) {
                    tupleList.add(tuple.build());
                    continue;
                }
            } else {
                RexDynamicParam rexDynamicParam = null;
                if (node instanceof SqlDynamicParam) {
                    rexDynamicParam = convertDynamicParam((SqlDynamicParam) node);
                }

                if ((rexDynamicParam != null) && config.isCreateValuesRel()) {
                    tupleList.add(ImmutableList.of(rexDynamicParam));
                    continue;
                } else {
                    if ((rexDynamicParam == null) && allowLiteralsOnly) {
                        return null;
                    }
                }

                // convert "1" to "row(1)"
                call = (SqlBasicCall) SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, node);
            }
            unionInputs.add(convertRowConstructor(bb, call));
        }
        DynamicValues values = DynamicValues.create(cluster, rowType, tupleList.build());
        RelNode resultRel;
        if (unionInputs.isEmpty()) {
            resultRel = values;
        } else {
            if (!values.getTuples().isEmpty()) {
                unionInputs.add(values);
            }
            resultRel = LogicalUnion.create(unionInputs, true);
        }
        leaves.add(resultRel);
        return resultRel;
    }

    private boolean isRowConstructor(SqlNode node) {
        if (!(node.getKind() == SqlKind.ROW)) {
            return false;
        }
        SqlCall call = (SqlCall) node;
        return call.getOperator().getName().equalsIgnoreCase("row");
    }

    /**
     * Builds a list of all <code>IN</code> or <code>EXISTS</code> operators
     * inside SQL parse tree. Does not traverse inside queries.
     *
     * @param bb blackboard
     * @param node the SQL parse tree
     * @param logic Whether the answer needs to be in full 3-valued logic (TRUE,
     * FALSE, UNKNOWN) will be required, or whether we can accept an
     * approximation (say representing UNKNOWN as FALSE)
     * @param registerOnlyScalarSubQueries if set to true and the parse tree
     * corresponds to a variation of a select node, only register it if it's a
     * scalar sub-query
     */
    private void findSubQueries(Blackboard bb, SqlNode node, RelOptUtil.Logic logic,
                                boolean registerOnlyScalarSubQueries, boolean containNotSupportInKind) {
        final SqlKind kind = node.getKind();
        switch (kind) {
        case EXISTS:
        case SELECT:
        case MULTISET_QUERY_CONSTRUCTOR:
        case MULTISET_VALUE_CONSTRUCTOR:
        case ARRAY_QUERY_CONSTRUCTOR:
        case CURSOR:
        case SCALAR_QUERY:
            if (!registerOnlyScalarSubQueries || (kind == SqlKind.SCALAR_QUERY)) {
                bb.registerSubQuery(node, RelOptUtil.Logic.TRUE_FALSE);
            }
            return;
        case IN:
            break;
        case NOT_IN:
        case NOT:
            logic = logic.negate();
            break;
        }
        boolean bContainNotSupportInKind = containNotSupportInKind;
        if (node instanceof SqlCall) {
            switch (kind) {
            // Do no change logic for AND, IN and NOT IN expressions;
            // but do change logic for OR, NOT and others;
            // EXISTS was handled already.
            case AND:
            case IN:
            case NOT_IN:
                if (bContainNotSupportInKind) {
                    bb.setSupportInvalueSemiJoin(false);
                }
                break;
            case IS_FALSE:
            case IS_NOT_FALSE:
            case IS_TRUE:
            case IS_NOT_TRUE:
            case IS_NOT_NULL:
            case IS_NULL:
                //(pk IN (1,2,3)) IS FALSE' don't support convert semi-join
                bContainNotSupportInKind = true;
                break;
            case OR:
                //只要存在关系代数OR,就禁用
                bContainNotSupportInKind = true;
                break;
            default:
//                logic = RelOptUtil.Logic.TRUE_FALSE_UNKNOWN;
                break;
            }
            for (SqlNode operand : ((SqlCall) node).getOperandList()) {
                if (operand != null) {
                    // In the case of an IN expression, locate scalar
                    // sub-queries so we can convert them to constants
                    findSubQueries(bb,
                        operand,
                        logic,
                        kind == SqlKind.IN || kind == SqlKind.NOT_IN || kind == SqlKind.SOME || kind == SqlKind.ALL
                            || registerOnlyScalarSubQueries, bContainNotSupportInKind);
                }
            }
        } else if (node instanceof SqlNodeList) {
            for (SqlNode child : (SqlNodeList) node) {
                findSubQueries(bb,
                    child,
                    logic,
                    kind == SqlKind.IN || kind == SqlKind.NOT_IN || kind == SqlKind.SOME || kind == SqlKind.ALL
                        || registerOnlyScalarSubQueries, bContainNotSupportInKind);
            }
        }

        // Now that we've located any scalar sub-queries inside the IN
        // expression, register the IN expression itself. We need to
        // register the scalar sub-queries first so they can be converted
        // before the IN expression is converted.
        switch (kind) {
        case IN:
        case NOT_IN:
        case SOME:
        case ALL:
            switch (logic) {
            case TRUE_FALSE_UNKNOWN:
                RelDataType type = validator.getValidatedNodeTypeIfKnown(node);
                if (type == null) {
                    // The node might not be validated if we still don't know type of the node.
                    // Therefore return directly.
                    return;
                } else {
                    break;
                }
                // fall through
            case UNKNOWN_AS_FALSE:
                logic = RelOptUtil.Logic.TRUE;
            }
            bb.registerSubQuery(node, logic);
            break;
        }
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param node Expression to translate
     * @return Converted expression
     */
    public RexNode convertExpression(SqlNode node) {
        Map<String, RelDataType> nameToTypeMap = Collections.emptyMap();
        final ParameterScope scope = new ParameterScope((SqlValidatorImpl) validator, nameToTypeMap);
        final Blackboard bb = createBlackboard(scope, null, false);
        return bb.convertExpression(node);
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format,
     * mapping identifier references to predefined expressions.
     *
     * @param node Expression to translate
     * @param nameToNodeMap map from String to {@link RexNode}; when an
     * {@link SqlIdentifier} is encountered, it is used as a key and translated
     * to the corresponding value from this map
     * @return Converted expression
     */
    public RexNode convertExpression(SqlNode node, Map<String, RexNode> nameToNodeMap) {
        final Map<String, RelDataType> nameToTypeMap = new HashMap<>();
        for (Map.Entry<String, RexNode> entry : nameToNodeMap.entrySet()) {
            nameToTypeMap.put(entry.getKey(), entry.getValue().getType());
        }
        final ParameterScope scope = new ParameterScope((SqlValidatorImpl) validator, nameToTypeMap);
        final Blackboard bb = createBlackboard(scope, nameToNodeMap, false);
        return bb.convertExpression(node);
    }

    /**
     * Converts a non-standard expression.
     * <p>
     * This method is an extension-point that derived classes can override. If
     * this method returns a null result, the normal expression translation
     * process will proceed. The default implementation always returns null.
     *
     * @param node Expression
     * @param bb Blackboard
     * @return null to proceed with the usual expression translation process
     */
    protected RexNode convertExtendedExpression(SqlNode node, Blackboard bb) {
        if (node instanceof SqlBasicCall && (((SqlBasicCall) node).getOperator() == IN
            || ((SqlBasicCall) node).getOperator() == NOT_IN)) {
            if (((SqlBasicCall) node).getOperandList().size() == 2 && ((SqlBasicCall) node).getOperandList()
                .get(1) instanceof SqlNodeList) {
                List<RexNode> rexNodeList = Lists.newArrayList();
                for (SqlNode s : ((SqlNodeList) ((SqlBasicCall) node).getOperandList().get(1)).getList()) {
                    rexNodeList.add(bb.convertExpression(s));
                }
                RexNode r = rexBuilder.makeCall(SqlStdOperatorTable.ROW, rexNodeList);
                return rexBuilder.makeCall(((SqlBasicCall) node).getOperator(),
                    bb.convertExpression(((SqlBasicCall) node).getOperandList().get(0)), r);
            }
        }
        return null;
    }

    private RexNode convertOver(Blackboard bb, SqlNode node) {
        SqlCall call = (SqlCall) node;
        SqlCall aggCall = call.operand(0);
        SqlNode windowOrRef = call.operand(1);
        final SqlWindow window = validator.resolveWindow(windowOrRef, bb.scope, true);

        // ROW_NUMBER() expects specific kind of framing.
        if (aggCall.getKind() == SqlKind.ROW_NUMBER) {
            window.setLowerBound(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO));
            window.setUpperBound(SqlWindow.createCurrentRow(SqlParserPos.ZERO));
            window.setRows(SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
        }
        final SqlNodeList partitionList = window.getPartitionList();
        final ImmutableList.Builder<RexNode> partitionKeys = ImmutableList.builder();
        for (SqlNode partition : partitionList) {
            partitionKeys.add(bb.convertExpression(partition));
        }
        RexNode lowerBound = bb.convertExpression(window.getLowerBound());
        RexNode upperBound = bb.convertExpression(window.getUpperBound());
        SqlNodeList orderList = window.getOrderList();
        if ((orderList.size() == 0) && !window.isRows()) {
            // A logical range requires an ORDER BY clause. Use the implicit
            // ordering of this relation. There must be one, otherwise it would
            // have failed validation.
            orderList = bb.scope.getOrderList();
            if (orderList == null) {
                throw new AssertionError("Relation should have sort key for implicit ORDER BY");
            }
        }
        final ImmutableList.Builder<RexFieldCollation> orderKeys = ImmutableList.builder();
        final Set<SqlKind> flags = EnumSet.noneOf(SqlKind.class);
        for (SqlNode order : orderList) {
            flags.clear();
            RexNode e = bb.convertWindowSortExpression(order, flags);
            orderKeys.add(new RexFieldCollation(e, flags));
        }
        try {
            Preconditions.checkArgument(bb.window == null, "already in window agg mode");
            bb.window = window;
            RexNode rexAgg = exprConverter.convertCall(bb, aggCall);
            rexAgg = rexBuilder.ensureNullability(validator.getValidatedNodeType(call), rexAgg, false);

            // Walk over the tree and apply 'over' to all agg functions. This is
            // necessary because the returned expression is not necessarily a
            // call
            // to an agg function. For example, AVG(x) becomes SUM(x) /
            // COUNT(x).

            final SqlLiteral q = aggCall.getFunctionQuantifier();
            final boolean isDistinct = q != null && q.getValue() == SqlSelectKeyword.DISTINCT;

            final RexShuttle visitor = new HistogramShuttle(partitionKeys.build(),
                orderKeys.build(),
                RexWindowBound.create(window.getLowerBound(), lowerBound),
                RexWindowBound.create(window.getUpperBound(), upperBound),
                window,
                isDistinct);
            RexNode overNode = rexAgg.accept(visitor);

            return overNode;
        } finally {
            bb.window = null;
        }
    }

    private void convertOutFile(Blackboard bb, OutFileParams outFileParams) {
        if (outFileParams == null) {
            return;
        }
        LogicalOutFile logicalOutFile = new LogicalOutFile(cluster, bb.root.getTraitSet(), bb.root, outFileParams);
        bb.setRoot(logicalOutFile, true);
    }

    /**
     * Converts a FROM clause into a relational expression.
     *
     * @param bb Scope within which to resolve identifiers
     * @param from FROM clause of a query. Examples include:
     * <ul>
     * <li>a single table ("SALES.EMP"),
     * <li>an aliased table ("EMP AS E"),
     * <li>a list of tables ("EMP, DEPT"),
     * <li>an ANSI Join expression ("EMP JOIN DEPT ON EMP.DEPTNO =
     * DEPT.DEPTNO"),
     * <li>a VALUES clause ("VALUES ('Fred', 20)"),
     * <li>a query ("(SELECT * FROM EMP WHERE GENDER = 'F')"),
     * <li>or any combination of the above.
     * </ul>
     */
    protected void convertFrom(Blackboard bb, SqlNode from) {
        if (from == null) {
            bb.setRoot(LogicalValues.createOneRow(cluster), false);
            return;
        }

        final SqlCall call;
        final SqlNode[] operands;
        switch (from.getKind()) {
        case MATCH_RECOGNIZE:
            convertMatchRecognize(bb, (SqlCall) from);
            return;
        case AS_OF:
            call = (SqlCall) from;
            SqlNode tableName = call.operand(0);
            SqlNode timeStamp = call.operand(1);
            final RexNode timestampExpr = bb.convertExpression(timeStamp);
            this.hintBlackboard.beginAsOf(timestampExpr);
            SqlIdentifier identifier = (SqlIdentifier) tableName;
            convertIdentifier(bb, identifier, null, identifier.indexNode, identifier.partitions);
            this.hintBlackboard.endAsOf();
            return;

        case AS:
            call = (SqlCall) from;
            SqlNode operand0 = call.operand(0);
            SqlNode operand1 = call.operand(1);
            this.hintBlackboard.beginAlias(operand0, operand1);
            try {
                SqlNode indexNode = null;
                SqlNode partitons = null;
                if (SqlIdentifier.class.isInstance(operand0) && SqlIdentifier.class.isInstance(operand1)) {
                    indexNode = ((SqlIdentifier) operand1).indexNode;
                    partitons = ((SqlIdentifier) operand1).partitions;
                    convertIdentifier(bb, (SqlIdentifier) operand0, null, indexNode, partitons);
                } else {
                    convertFrom(bb, call.operand(0));
                }
            } finally {
                this.hintBlackboard.endAlias();
            }
            if (call.operandCount() > 2 && bb.root instanceof Values) {
                final List<String> fieldNames = new ArrayList<>();
                for (SqlNode node : Util.skip(call.getOperandList(), 2)) {
                    fieldNames.add(((SqlIdentifier) node).getSimple());
                }
                bb.setRoot(relBuilder.push(bb.root).rename(fieldNames).build(), true);
            }
            return;

        case WITH_ITEM:
            convertFrom(bb, ((SqlWithItem) from).query);
            return;

        case WITH:
            convertFrom(bb, ((SqlWith) from).body);
            return;

        case TABLESAMPLE:
            operands = ((SqlBasicCall) from).getOperands();
            SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands[1]);
            if (sampleSpec instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
                String sampleName = ((SqlSampleSpec.SqlSubstitutionSampleSpec) sampleSpec).getName();
                datasetStack.push(sampleName);
                convertFrom(bb, operands[0]);
                datasetStack.pop();
            } else if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
                SqlSampleSpec.SqlTableSampleSpec tableSampleSpec = (SqlSampleSpec.SqlTableSampleSpec) sampleSpec;
                convertFrom(bb, operands[0]);
                RelOptSamplingParameters params = new RelOptSamplingParameters(tableSampleSpec.isBernoulli(),
                    tableSampleSpec.getSamplePercentage(),
                    tableSampleSpec.isRepeatable(),
                    tableSampleSpec.getRepeatableSeed());
                bb.setRoot(new Sample(cluster, bb.root, params), false);
            } else {
                throw new AssertionError("unknown TABLESAMPLE type: " + sampleSpec);
            }
            return;

        case IDENTIFIER:
            convertIdentifier(bb, (SqlIdentifier) from, null, ((SqlIdentifier) from).indexNode,
                ((SqlIdentifier) from).partitions);
            return;
        case EXTEND:
            call = (SqlCall) from;
            SqlIdentifier id = (SqlIdentifier) call.getOperandList().get(0);
            SqlNodeList extendedColumns = (SqlNodeList) call.getOperandList().get(1);
            convertIdentifier(bb, id, extendedColumns);
            return;

        case JOIN:
            final SqlJoin join = (SqlJoin) from;
            final SqlValidatorScope scope = validator.getJoinScope(from);
            final Blackboard fromBlackboard = createBlackboard(scope, null, false, bb);
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            final boolean isNatural = join.isNatural();
            final JoinType joinType = join.getJoinType();
            final SqlValidatorScope leftScope = Util.first(validator.getJoinScope(left),
                ((DelegatingScope) bb.scope).getParent());
            final Blackboard leftBlackboard = createBlackboard(leftScope, null, false, bb);
            final SqlValidatorScope rightScope = Util.first(validator.getJoinScope(right),
                ((DelegatingScope) bb.scope).getParent());
            final Blackboard rightBlackboard = createBlackboard(rightScope, null, false);
            final SqlNodeList hints = this.hintBlackboard.joinHints(left, bb);
            convertFrom(leftBlackboard, left);
            RelNode leftRel = leftBlackboard.root;
            convertFrom(rightBlackboard, right);
            RelNode rightRel = rightBlackboard.root;
            JoinRelType convertedJoinType = convertJoinType(joinType);
            RexNode conditionExp;
            final SqlValidatorNamespace leftNamespace = validator.getNamespace(left);
            final SqlValidatorNamespace rightNamespace = validator.getNamespace(right);
            if (isNatural) {
                final RelDataType leftRowType = leftNamespace.getRowType();
                final RelDataType rightRowType = rightNamespace.getRowType();
                final List<String> columnList = SqlValidatorUtil.deriveNaturalJoinColumnList(leftRowType,
                    rightRowType);
                conditionExp = convertUsing(leftNamespace, rightNamespace, columnList);
            } else {
                conditionExp = convertJoinCondition(fromBlackboard,
                    leftNamespace,
                    rightNamespace,
                    join.getCondition(),
                    join.getConditionType(),
                    leftRel,
                    rightRel);
            }

            final RelNode joinRel = createJoin(fromBlackboard,
                leftRel,
                rightRel,
                conditionExp,
                convertedJoinType,
                hints);
            bb.setRoot(joinRel, false);
            return;

        case SELECT:
        case INTERSECT:
        case EXCEPT:
        case UNION:
            final RelNode rel = convertQueryRecursive(from, false, null).project();
            bb.setRoot(rel, true);
            return;

        case VALUES:
            convertValuesImpl(bb, (SqlCall) from, null);
            return;

        case UNNEST:
            call = (SqlCall) from;
            final List<SqlNode> nodes = call.getOperandList();
            final SqlUnnestOperator operator = (SqlUnnestOperator) call.getOperator();
            for (SqlNode node : nodes) {
                replaceSubQueries(bb, node, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);
            }
            final List<RexNode> exprs = new ArrayList<>();
            final List<String> fieldNames = new ArrayList<>();
            for (Ord<SqlNode> node : Ord.zip(nodes)) {
                exprs.add(bb.convertExpression(node.e));
                fieldNames.add(validator.deriveAlias(node.e, node.i));
            }
            final RelNode input = RelOptUtil.createProject(
                (null != bb.root) ? bb.root : LogicalValues.createOneRow(cluster),
                exprs,
                fieldNames,
                true);

            Uncollect uncollect = new Uncollect(cluster,
                cluster.traitSetOf(Convention.NONE),
                input,
                operator.withOrdinality);
            bb.setRoot(uncollect, true);
            return;

        case COLLECTION_TABLE:
            call = (SqlCall) from;

            // Dig out real call; TABLE() wrapper is just syntactic.
            assert call.getOperandList().size() == 1;
            final SqlCall call2 = call.operand(0);
            convertCollectionTable(bb, call2);
            return;
        case N_ROW:
            call = (SqlCall) from;
            assert call.getOperandList().size() == 1;
            if (call.operand(0) instanceof SqlNumericLiteral) {
                int count = ((SqlNumericLiteral) call.operand(0)).intValue(true);
                bb.setRoot(LogicalValues.createNRow(cluster, count), false);
            } else if (call.operand(0) instanceof SqlDynamicParam) {
                bb.setRoot(LogicalValues.createNRow(cluster, call.operand(0)), false);
            } else {
                throw new AssertionError("not a N_ROW suitable args :" + call.operand(0));
            }
            return;
        case SHOW:
            final SqlShow show = (SqlShow) from;
            final RelDataType rowType = validator.getValidatedNodeTypeIfKnown(show);
            bb.setRoot(Show.create(show, rowType, this.cluster), true);
            return;
        default:
            throw new AssertionError("not a join operator " + from);
        }
    }

    protected void convertMatchRecognize(Blackboard bb, SqlCall call) {
        final SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
        final SqlValidatorNamespace ns = validator.getNamespace(matchRecognize);
        final SqlValidatorScope scope = validator.getMatchRecognizeScope(matchRecognize);

        final Blackboard matchBb = createBlackboard(scope, null, false, bb);
        final RelDataType rowType = ns.getRowType();
        // convert inner query, could be a table name or a derived table
        SqlNode expr = matchRecognize.getTableRef();
        convertFrom(matchBb, expr);
        final RelNode input = matchBb.root;

        // PARTITION BY
        final SqlNodeList partitionList = matchRecognize.getPartitionList();
        final List<RexNode> partitionKeys = new ArrayList<>();
        for (SqlNode partition : partitionList) {
            RexNode e = matchBb.convertExpression(partition);
            partitionKeys.add(e);
        }

        // ORDER BY
        final SqlNodeList orderList = matchRecognize.getOrderList();
        final List<RelFieldCollation> orderKeys = new ArrayList<>();
        for (SqlNode order : orderList) {
            final RelFieldCollation.Direction direction;
            switch (order.getKind()) {
            case DESCENDING:
                direction = RelFieldCollation.Direction.DESCENDING;
                order = ((SqlCall) order).operand(0);
                break;
            case NULLS_FIRST:
            case NULLS_LAST:
                throw new AssertionError();
            default:
                direction = RelFieldCollation.Direction.ASCENDING;
                break;
            }
            final RelFieldCollation.NullDirection nullDirection = validator.getDefaultNullCollation()
                .last(desc(direction)) ? RelFieldCollation.NullDirection.LAST : RelFieldCollation.NullDirection.FIRST;
            RexNode e = matchBb.convertExpression(order);
            orderKeys.add(new RelFieldCollation(((RexInputRef) e).getIndex(), direction, nullDirection));
        }
        final RelCollation orders = cluster.traitSet().canonize(RelCollations.of(orderKeys));

        // convert pattern
        final Set<String> patternVarsSet = new HashSet<>();
        SqlNode pattern = matchRecognize.getPattern();
        final SqlBasicVisitor<RexNode> patternVarVisitor = new SqlBasicVisitor<RexNode>() {

            @Override
            public RexNode visit(SqlCall call) {
                List<SqlNode> operands = call.getOperandList();
                List<RexNode> newOperands = Lists.newArrayList();
                for (SqlNode node : operands) {
                    newOperands.add(node.accept(this));
                }
                return rexBuilder.makeCall(validator.getUnknownType(), call.getOperator(), newOperands);
            }

            @Override
            public RexNode visit(SqlIdentifier id) {
                assert id.isSimple();
                patternVarsSet.add(id.getSimple());
                return rexBuilder.makeLiteral(id.getSimple());
            }

            @Override
            public RexNode visit(SqlLiteral literal) {
                if (literal instanceof SqlNumericLiteral) {
                    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(literal.intValue(true)));
                } else {
                    return rexBuilder.makeLiteral(literal.booleanValue());
                }
            }
        };
        final RexNode patternNode = pattern.accept(patternVarVisitor);

        SqlLiteral interval = matchRecognize.getInterval();
        RexNode intervalNode = null;
        if (interval != null) {
            intervalNode = matchBb.convertLiteral(interval);
        }

        // convert subset
        final SqlNodeList subsets = matchRecognize.getSubsetList();
        final Map<String, TreeSet<String>> subsetMap = Maps.newHashMap();
        for (SqlNode node : subsets) {
            List<SqlNode> operands = ((SqlCall) node).getOperandList();
            SqlIdentifier left = (SqlIdentifier) operands.get(0);
            patternVarsSet.add(left.getSimple());
            SqlNodeList rights = (SqlNodeList) operands.get(1);
            final TreeSet<String> list = new TreeSet<String>();
            for (SqlNode right : rights) {
                assert right instanceof SqlIdentifier;
                list.add(((SqlIdentifier) right).getSimple());
            }
            subsetMap.put(left.getSimple(), list);
        }

        SqlNode afterMatch = matchRecognize.getAfter();
        if (afterMatch == null) {
            afterMatch = SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW.symbol(SqlParserPos.ZERO);
        }

        final RexNode after;
        if (afterMatch instanceof SqlCall) {
            List<SqlNode> operands = ((SqlCall) afterMatch).getOperandList();
            SqlOperator operator = ((SqlCall) afterMatch).getOperator();
            assert operands.size() == 1;
            SqlIdentifier id = (SqlIdentifier) operands.get(0);
            assert patternVarsSet.contains(id.getSimple()) : id.getSimple() + " not defined in pattern";
            RexNode rex = rexBuilder.makeLiteral(id.getSimple());
            after = rexBuilder.makeCall(validator.getUnknownType(), operator, ImmutableList.of(rex));
        } else {
            after = matchBb.convertExpression(afterMatch);
        }

        matchBb.setPatternVarRef(true);

        // convert measures
        final ImmutableMap.Builder<String, RexNode> measureNodes = ImmutableMap.builder();
        for (SqlNode measure : matchRecognize.getMeasureList()) {
            List<SqlNode> operands = ((SqlCall) measure).getOperandList();
            String alias = ((SqlIdentifier) operands.get(1)).getSimple();
            RexNode rex = matchBb.convertExpression(operands.get(0));
            measureNodes.put(alias, rex);
        }

        // convert definitions
        final ImmutableMap.Builder<String, RexNode> definitionNodes = ImmutableMap.builder();
        for (SqlNode def : matchRecognize.getPatternDefList()) {
            List<SqlNode> operands = ((SqlCall) def).getOperandList();
            String alias = ((SqlIdentifier) operands.get(1)).getSimple();
            RexNode rex = matchBb.convertExpression(operands.get(0));
            definitionNodes.put(alias, rex);
        }

        final SqlLiteral rowsPerMatch = matchRecognize.getRowsPerMatch();
        final boolean allRows = rowsPerMatch != null
            && rowsPerMatch.getValue() == SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS;

        matchBb.setPatternVarRef(false);

        final RelFactories.MatchFactory factory = RelFactories.DEFAULT_MATCH_FACTORY;
        final RelNode rel = factory.createMatch(input,
            patternNode,
            rowType,
            matchRecognize.getStrictStart().booleanValue(),
            matchRecognize.getStrictEnd().booleanValue(),
            definitionNodes.build(),
            measureNodes.build(),
            after,
            subsetMap,
            allRows,
            partitionKeys,
            orders,
            intervalNode);
        bb.setRoot(rel, false);
    }

    private void convertIdentifier(Blackboard bb, SqlIdentifier id, SqlNodeList extendedColumns) {
        convertIdentifier(bb, id, extendedColumns, id.indexNode, id.partitions);
    }

    private void convertIdentifier(Blackboard bb, SqlIdentifier id, SqlNodeList extendedColumns, SqlNode indexNode,
                                   SqlNode partitions) {
        final SqlValidatorNamespace fromNamespace = validator.getNamespace(id).resolve();
        if (fromNamespace.getNode() != null) {
            convertFrom(bb, fromNamespace.getNode());
            return;
        }
        final String datasetName = datasetStack.isEmpty() ? null : datasetStack.peek();
        final boolean[] usedDataset = {false};
        RelOptTable table = SqlValidatorUtil.getRelOptTable(fromNamespace, catalogReader, datasetName, usedDataset);
        if (extendedColumns != null && extendedColumns.size() > 0) {
            assert table != null;
            final SqlValidatorTable validatorTable = table.unwrap(SqlValidatorTable.class);
            final List<RelDataTypeField> extendedFields = SqlValidatorUtil
                .getExtendedColumns(validator.getTypeFactory(), validatorTable, extendedColumns);
            table = table.extend(extendedFields);
        }
        final RelNode tableRel;
        boolean isViewTable = false;
        if (table instanceof RelOptTableImpl && ((RelOptTableImpl) table).getImplTable() instanceof ViewTable) {
            isViewTable = true;
        }
        if (config.isConvertTableAccess() || isViewTable) {
            tableRel = toRel(table);
        } else {
            SqlNodeList hint = this.hintBlackboard.currentHints(Util.last(table.getQualifiedName()));
            if (this.hintBlackboard.hasAsOf()) {
                tableRel = LogicalTableScan
                    .create(cluster, table, hint, indexNode, this.hintBlackboard.peekAsOf(), partitions);
            } else {
                tableRel = LogicalTableScan.create(cluster, table, hint, indexNode, null, partitions);

            }
        }
        bb.setRoot(tableRel, true);
        if (usedDataset[0]) {
            bb.setDataset(datasetName);
        }
    }

    protected void convertCollectionTable(Blackboard bb, SqlCall call) {
        final SqlOperator operator = call.getOperator();
        if (operator == SqlStdOperatorTable.TABLESAMPLE) {
            final String sampleName = SqlLiteral.unchain(call.operand(0)).getValueAs(String.class);
            datasetStack.push(sampleName);
            SqlCall cursorCall = call.operand(1);
            SqlNode query = cursorCall.operand(0);
            RelNode converted = convertQuery(query, false, false).rel;
            bb.setRoot(converted, false);
            datasetStack.pop();
            return;
        }
        replaceSubQueries(bb, call, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // Expand table macro if possible. It's more efficient than
        // LogicalTableFunctionScan.
        final SqlCallBinding callBinding = new SqlCallBinding(bb.scope.getValidator(), bb.scope, call);
        if (operator instanceof SqlUserDefinedTableMacro) {
            final SqlUserDefinedTableMacro udf = (SqlUserDefinedTableMacro) operator;
            final TranslatableTable table = udf.getTable(typeFactory, callBinding.operands());
            final RelDataType rowType = table.getRowType(typeFactory);
            RelOptTable relOptTable = RelOptTableImpl.create(null, rowType, table, udf.getNameAsId().names);
            RelNode converted = toRel(relOptTable);
            bb.setRoot(converted, true);
            return;
        }

        Type elementType;
        if (operator instanceof SqlUserDefinedTableFunction) {
            SqlUserDefinedTableFunction udtf = (SqlUserDefinedTableFunction) operator;
            elementType = udtf.getElementType(typeFactory, callBinding.operands());
        } else {
            elementType = null;
        }

        RexNode rexCall = bb.convertExpression(call);
        final List<RelNode> inputs = bb.retrieveCursors();
        Set<RelColumnMapping> columnMappings = getColumnMappings(operator);
        LogicalTableFunctionScan callRel = LogicalTableFunctionScan
            .create(cluster, inputs, rexCall, elementType, validator.getValidatedNodeType(call), columnMappings);
        bb.setRoot(callRel, true);
        afterTableFunction(bb, call, callRel);
    }

    protected void afterTableFunction(SqlToRelConverter.Blackboard bb, SqlCall call, LogicalTableFunctionScan callRel) {
    }

    private Set<RelColumnMapping> getColumnMappings(SqlOperator op) {
        SqlReturnTypeInference rti = op.getReturnTypeInference();
        if (rti == null) {
            return null;
        }
        if (rti instanceof TableFunctionReturnTypeInference) {
            TableFunctionReturnTypeInference tfrti = (TableFunctionReturnTypeInference) rti;
            return tfrti.getColumnMappings();
        } else {
            return null;
        }
    }

    protected RelNode createJoin(Blackboard bb, RelNode leftRel, RelNode rightRel, RexNode joinCond,
                                 JoinRelType joinType, SqlNodeList hints) {
        assert joinCond != null;

        CorrelationUse p = null;
        try {
            p = getCorrelationUse(bb, rightRel, null, null, null);
        } catch (ValidationFirstValueException e) {
            throw new AssertionError(e.getMessage());
        }
        if (p != null) {
            LogicalCorrelate corr = LogicalCorrelate
                .create(leftRel, p.r, p.id, p.requiredColumns, null, null, SemiJoinType.of(joinType));
            if (!joinCond.isAlwaysTrue()) {
                final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
                return factory.createFilter(corr, joinCond);
            }
            return corr;
        }

        final Join originalJoin = (Join) RelFactories.DEFAULT_JOIN_FACTORY
            .createJoin(leftRel, rightRel, joinCond, ImmutableSet.<CorrelationId>of(), joinType, false, hints);

        // return RelOptUtil.pushDownJoinConditions(originalJoin);
        return originalJoin;
    }

    private CorrelationUse getCorrelationUse(Blackboard bb, final RelNode r0, Map<Integer, Integer> exprIndex,
                                             List<SqlIdentifier> firstValues,
                                             Map<SqlIdentifier, Integer> firstValueTarget)
        throws ValidationFirstValueException {
        final Set<CorrelationId> correlatedVariables = RelOptUtil.getVariablesUsed(r0);
        if (correlatedVariables.isEmpty()) {
            return null;
        }
        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
        final List<CorrelationId> correlNames = Lists.newArrayList();

        // All correlations must refer the same namespace since correlation
        // produces exactly one correlation source.
        // The same source might be referenced by different variables since
        // DeferredLookups are not de-duplicated at create time.
        SqlValidatorNamespace prevNs = null;
        SqlValidatorScope current = null;
        for (CorrelationId correlName : correlatedVariables) {
            DeferredLookup lookup = mapCorrelToDeferred.get(correlName);
            RexFieldAccess fieldAccess = lookup.getFieldAccess(correlName);
            String originalRelName = lookup.getOriginalRelName();
            String originalFieldName = fieldAccess.getField().getName();

            for (; ; ) {
                if (current == null) {
                    current = lookup.bb.scope;
                }
                final SqlNameMatcher nameMatcher = lookup.bb.scope.getValidator().getCatalogReader().nameMatcher();
                final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
                current.resolve(ImmutableList.of(originalRelName), nameMatcher, false, resolved);
                assert resolved.count() == 1;
                final SqlValidatorScope.Resolve resolve = resolved.only();
                final SqlValidatorNamespace foundNs = resolve.namespace;
                final RelDataType rowType = resolve.rowType();
                final int childNamespaceIndex = resolve.path.steps().get(0).i;
                final SqlValidatorScope ancestorScope = resolve.scope;
                boolean correlInCurrentScope = ancestorScope == bb.scope;
                if (!correlInCurrentScope) {
                    if (isChlild(bb.scope, ancestorScope)) {
                        current = getSqlValidatorScope(current, originalRelName, nameMatcher, resolved);
                        continue;
                    }
                    break;
                }

                if (prevNs == null) {
                    prevNs = foundNs;
                } else {
                    assert prevNs == foundNs : "All correlation variables should resolve" + " to the same namespace."
                        + " Prev ns=" + prevNs + ", new ns=" + foundNs;
                }

                int namespaceOffset = 0;
                if (childNamespaceIndex > 0) {
                    // If not the first child, need to figure out the width
                    // of output types from all the preceding namespaces
                    assert ancestorScope instanceof ListScope;
                    List<SqlValidatorNamespace> children = ((ListScope) ancestorScope).getChildren();

                    for (int i = 0; i < childNamespaceIndex; i++) {
                        SqlValidatorNamespace child = children.get(i);
                        namespaceOffset += child.getRowType().getFieldCount();
                    }
                }

                RexFieldAccess topLevelFieldAccess = fieldAccess;
                while (topLevelFieldAccess.getReferenceExpr() instanceof RexFieldAccess) {
                    topLevelFieldAccess = (RexFieldAccess) topLevelFieldAccess.getReferenceExpr();
                }
                final List<RelDataTypeField> fieldList = rowType.getFieldList();
                final int index = topLevelFieldAccess.getField().getIndex() - namespaceOffset;
                if (fieldList.size() <= index || index < 0) {
                    current = getSqlValidatorScope(current, originalRelName, nameMatcher, resolved);
                    continue;
                }
                final RelDataTypeField field = fieldList.get(index);
                if (field == null) {
                    current = getSqlValidatorScope(current, originalRelName, nameMatcher, resolved);
                    continue;
                }
                int pos = namespaceOffset + field.getIndex();

                assert field.getType() == topLevelFieldAccess.getField().getType();

                assert pos != -1;

                if (bb.mapRootRelToFieldProjection.containsKey(bb.root)) {
                    // bb.root is an aggregate and only projects group by
                    // keys.
                    Map<Integer, Integer> exprProjection = bb.mapRootRelToFieldProjection.get(bb.root);

                    if (exprIndex != null && exprProjection != null) {
                        exprIndex.putAll(exprProjection);
                    }
                    Optional<SqlIdentifier> optionalSqlIdentifier = null;
                    if (firstValues != null) {
                        optionalSqlIdentifier = firstValues.stream()
                            .filter(i -> i.names.containsAll(ImmutableList.of(originalRelName, originalFieldName)))
                            .findFirst();
                    }
                    // sub-query can reference group by keys projected from
                    // the root of the outer relation.
                    if (exprProjection.containsKey(pos)) {
                        pos = exprProjection.get(pos);
                    } else if (optionalSqlIdentifier != null && optionalSqlIdentifier.isPresent()) {
                        int newpos = firstValueTarget.get(optionalSqlIdentifier.get());
                        exprIndex.put(pos, newpos);
                        pos = newpos;
                    } else {
                        // correl not grouped
                        SqlIdentifier firstValue =
                            new SqlIdentifier(ImmutableList.of(originalRelName, originalFieldName), null,
                                SqlParserPos.ZERO, null);
                        throw new ValidationFirstValueException(
                            "Identifier '" + originalRelName + "." + originalFieldName + "' is not a group expr",
                            firstValue);
                    }
                }

                requiredColumns.set(pos);
                correlNames.add(correlName);
                break;
            }

        }

        if (correlNames.isEmpty()) {
            // None of the correlating variables originated in this scope.
            return null;
        }

        RelNode r = r0;
        if (correlNames.size() > 1) {
            // The same table was referenced more than once.
            // So we deduplicate.
            r = DeduplicateCorrelateVariables.go(rexBuilder, correlNames.get(0), Util.skip(correlNames), r0);
            // Add new node to leaves.
            leaves.add(r);
        }
        return new CorrelationUse(correlNames.get(0), requiredColumns.build(), r);
    }

    private boolean isChlild(SqlValidatorScope parent, SqlValidatorScope child) {
        if (child == null) {
            return false;
        }
        if (child instanceof ListScope) {
            final SqlValidatorScope currParent = ((ListScope) child).getParent();
            if (currParent == parent) {
                return true;
            }
            if (currParent != null) {
                return isChlild(parent, currParent);
            }
        }
        return false;
    }

    private SqlValidatorScope getSqlValidatorScope(SqlValidatorScope current, String originalRelName,
                                                   SqlNameMatcher nameMatcher,
                                                   SqlValidatorScope.ResolvedImpl resolved) {
        if (current instanceof ListScope) {
            resolved.clear();
            current = ((ListScope) current).getParent();
            if (current == null) {
                throw new NotSupportException("Filed not in any table");
            }
            return current;
        } else {
            throw new NotSupportException("Filed not in any table");
        }
    }

    /**
     * Determines whether a sub-query is non-correlated. Note that a
     * non-correlated sub-query can contain correlated references, provided
     * those references do not reference select statements that are parents of
     * the sub-query.
     *
     * @param subq the sub-query
     * @param bb blackboard used while converting the sub-query, i.e., the
     * blackboard of the parent query of this sub-query
     * @return true if the sub-query is non-correlated
     */
    private boolean isSubQueryNonCorrelated(RelNode subq, Blackboard bb) {
        Set<CorrelationId> correlatedVariables = RelOptUtil.getVariablesUsed(subq);
        for (CorrelationId correlName : correlatedVariables) {
            DeferredLookup lookup = mapCorrelToDeferred.get(correlName);
            String originalRelName = lookup.getOriginalRelName();

            final SqlNameMatcher nameMatcher = lookup.bb.scope.getValidator().getCatalogReader().nameMatcher();
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            lookup.bb.scope.resolve(ImmutableList.of(originalRelName), nameMatcher, false, resolved);

            SqlValidatorScope ancestorScope = resolved.only().scope;

            // If the correlated reference is in a scope that's "above" the
            // sub-query, then this is a correlated sub-query.
            SqlValidatorScope parentScope = bb.scope;
            do {
                if (ancestorScope == parentScope) {
                    return false;
                }
                if (parentScope instanceof DelegatingScope) {
                    parentScope = ((DelegatingScope) parentScope).getParent();
                } else {
                    break;
                }
            } while (parentScope != null);
        }
        return true;
    }

    /**
     * Returns a list of fields to be prefixed to each relational expression.
     *
     * @return List of system fields
     */
    protected List<RelDataTypeField> getSystemFields() {
        return Collections.emptyList();
    }

    private RexNode convertJoinCondition(Blackboard bb, SqlValidatorNamespace leftNamespace,
                                         SqlValidatorNamespace rightNamespace, SqlNode condition,
                                         JoinConditionType conditionType, RelNode leftRel, RelNode rightRel) {
        if (condition == null) {
            return rexBuilder.makeLiteral(true);
        }
        bb.setRoot(ImmutableList.of(leftRel, rightRel));
        replaceSubQueries(bb, condition, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
        switch (conditionType) {
        case ON:
            bb.setRoot(ImmutableList.of(leftRel, rightRel));
            return bb.convertExpression(condition);
        case USING:
            final SqlNodeList list = (SqlNodeList) condition;
            final List<String> nameList = new ArrayList<>();
            for (SqlNode columnName : list) {
                final SqlIdentifier id = (SqlIdentifier) columnName;
                String name = id.getSimple();
                nameList.add(name);
            }
            return convertUsing(leftNamespace, rightNamespace, nameList);
        default:
            throw Util.unexpected(conditionType);
        }
    }

    /**
     * Returns an expression for matching columns of a USING clause or inferred
     * from NATURAL JOIN. "a JOIN b USING (x, y)" becomes "a.x = b.x AND a.y =
     * b.y". Returns null if the column list is empty.
     *
     * @param leftNamespace Namespace of left input to join
     * @param rightNamespace Namespace of right input to join
     * @param nameList List of column names to join on
     * @return Expression to match columns from name list, or true if name list
     * is empty
     */
    private RexNode convertUsing(SqlValidatorNamespace leftNamespace, SqlValidatorNamespace rightNamespace,
                                 List<String> nameList) {
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        final List<RexNode> list = Lists.newArrayList();
        for (String name : nameList) {
            List<RexNode> operands = new ArrayList<>();
            int offset = 0;
            for (SqlValidatorNamespace n : ImmutableList.of(leftNamespace, rightNamespace)) {
                final RelDataType rowType = n.getRowType();
                final RelDataTypeField field = nameMatcher.field(rowType, name);
                operands.add(rexBuilder.makeInputRef(field.getType(), offset + field.getIndex()));
                offset += rowType.getFieldList().size();
            }
            list.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, operands));
        }
        return conditionsToAndConjunction(list);
    }

    /**
     * Conjunct a list of expressions with AND operator.
     * e.g. [a.x = b.x, a.y = b.y, a.z = b.z] coverts to (a.x = b.x AND a.y = b.y AND a.z = b.z)
     */
    private RexNode conditionsToAndConjunction(List<RexNode> list) {
        RexNode composedCondition;
        if (list.size() > 2) {
            List<RexNode> composedList = Lists.newArrayListWithCapacity(2);
            composedList.add(0, list.get(0));
            composedList.add(1, conditionsToAndConjunction(list.subList(1, list.size())));
            composedCondition = RexUtil.composeConjunction(rexBuilder, composedList, false);
        } else {
            composedCondition = RexUtil.composeConjunction(rexBuilder, list, false);
        }
        return composedCondition;
    }

    private static JoinRelType convertJoinType(JoinType joinType) {
        switch (joinType) {
        case COMMA:
        case INNER:
        case CROSS:
            return JoinRelType.INNER;
        case FULL:
            return JoinRelType.FULL;
        case LEFT:
            return JoinRelType.LEFT;
        case RIGHT:
            return JoinRelType.RIGHT;
        default:
            throw Util.unexpected(joinType);
        }
    }

    /**
     * Converts the SELECT, GROUP BY and HAVING clauses of an aggregate query.
     * <p>
     * This method extracts SELECT, GROUP BY and HAVING clauses, and creates an
     * {@link AggConverter}, then delegates to {@link #createAggImpl}. Derived
     * class may override this method to change any of those clauses or specify
     * a different {@link AggConverter}.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Query
     * @param orderExprList Additional expressions needed to implement ORDER BY
     */
    protected void convertAgg(Blackboard bb, SqlSelect select, List<SqlNode> orderExprList) {
        final boolean distinct = select.isDistinct();
        assert bb.root != null : "precondition: child != null";
        SqlNodeList groupList = select.getGroup();
        SqlNodeList selectList = select.getSelectList();
        SqlNode having = select.getHaving();

        final AggConverter aggConverter = new AggConverter(bb, select);
        if (this.validator instanceof SqlValidatorImpl) {
            SqlValidatorImpl sqlValidator = (SqlValidatorImpl) this.validator;
            having = sqlValidator.validatedExpandingHaving(select);
        }
        List<SqlIdentifier> firstValueForSubquery = Lists.newArrayList();
        RelNode input = bb.root;
        int count = 0;
        while (true) {
            try {
                bb.setRoot(input, false);
                createAggImpl(bb, aggConverter, selectList, groupList, having, orderExprList, firstValueForSubquery);
                break;
            } catch (ValidationFirstValueException e) {
                if (count++ > CORRELATE_LOOP_UPPER_BAND) {
                    throw new AssertionError(
                        e.getMessage() + ", DEAD LOOP DETECTED:" + firstValueForSubquery.subList(0, 10).toString());
                }
                firstValueForSubquery.add(e.getSqlIdentifier());
            }
        }

        if (distinct) {
            distinctify(bb, null, null, true);
        }
    }

    protected final void createAggImpl(Blackboard bb, final AggConverter aggConverter, SqlNodeList selectList,
                                       SqlNodeList groupList, SqlNode having, List<SqlNode> orderExprList,
                                       List<SqlIdentifier> firstValueForSubquery)
        throws ValidationFirstValueException {
        // Find aggregate functions in SELECT and HAVING clause
        final AggregateFinder aggregateFinder = new AggregateFinder();

        SqlNodeList newOrderByList = new SqlNodeList(SqlParserPos.ZERO);
        List<SqlNode> listInOrder = Lists.newArrayList();
        for (SqlNode order : orderExprList) {
            boolean found = false;
            if (groupList != null) {
                for (SqlNode g : groupList.getList()) {
                    if (order.toString().equals(g.toString())) {
                        // add original group by node,to benefit the compareration afterward.
                        listInOrder.add(g);
                        found = true;
                    }
                }
            }

            if (!found) {
                SqlNode virtualAgg = SqlStdOperatorTable.__FIRST_VALUE.createCall(SqlParserPos.ZERO, order);
                newOrderByList.add(virtualAgg);
            }
        }

        SqlNodeList firstValueList = new SqlNodeList(SqlParserPos.ZERO);
        Map<SqlIdentifier, Integer> firstValueTarget = Maps.newHashMap();
        for (SqlIdentifier firstValue : firstValueForSubquery) {
            SqlNode virtualAgg = SqlStdOperatorTable.__FIRST_VALUE.createCall(SqlParserPos.ZERO, firstValue);
            firstValueList.add(virtualAgg);
        }

        selectList.accept(aggregateFinder);
        if (having != null) {
            having.accept(aggregateFinder);
        }

        // first replace the sub-queries inside the aggregates
        // because they will provide input rows to the aggregates.
        replaceSubQueries(bb, aggregateFinder.list, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // also replace sub-queries inside filters in the aggregates
        replaceSubQueries(bb, aggregateFinder.filterList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // If group-by clause is missing, pretend that it has zero elements.
        if (groupList == null) {
            groupList = SqlNodeList.EMPTY;
        }

        replaceSubQueries(bb, groupList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // register the group exprs

        // build a map to remember the projections from the top scope to the
        // output of the current root.
        //
        // Calcite allows expressions, not just column references in
        // group by list. This is not SQL 2003 compliant, but hey.

        final AggregatingSelectScope scope = aggConverter.aggregatingSelectScope;
        final AggregatingSelectScope.Resolved r = scope.resolved.get();
        for (SqlNode groupExpr : r.groupExprList) {
            aggConverter.addGroupExpr(groupExpr);
        }

        final RexNode havingExpr;
        final List<Pair<RexNode, String>> projects = Lists.newArrayList();

        final Map<SqlIdentifier, SqlNode> firstValueMap = Maps.newHashMap();
        try {
            Preconditions.checkArgument(bb.agg == null, "already in agg mode");
            bb.agg = aggConverter;

            // convert the select and having expressions, so that the
            // agg converter knows which aggregations are required

            selectList.accept(aggConverter);
            // Assert we don't have dangling items left in the stack
            assert !aggConverter.inOver;
            for (SqlNode expr : newOrderByList) {
                expr.accept(aggConverter);
                assert !aggConverter.inOver;
            }
            for (SqlIdentifier firstValue : firstValueForSubquery) {
                SqlNode virtualAgg = SqlStdOperatorTable.__FIRST_VALUE.createCall(SqlParserPos.ZERO, firstValue);
                virtualAgg.accept(aggConverter);
                firstValueMap.put(firstValue, virtualAgg);
            }
            if (having != null) {
                having.accept(aggConverter);
                assert !aggConverter.inOver;
            }

            // compute inputs to the aggregator
            List<Pair<RexNode, String>> preExprs = aggConverter.getPreExprs();

            if (preExprs.size() == 0) {
                // Special case for COUNT(*), where we can end up with no inputs
                // at all. The rest of the system doesn't like 0-tuples, so we
                // select a dummy constant here.
                final RexNode zero = rexBuilder.makeIntLiteral(0);
                preExprs = ImmutableList.of(Pair.of(zero, (String) null));
            }

            final RelNode inputRel = bb.root;

            // Project the expressions required by agg and having.
            bb.setRoot(RelOptUtil.createProject(inputRel, preExprs, true).setHints(inputRel.getHints()), false);
            bb.mapRootRelToFieldProjection.put(bb.root, r.groupExprProjection);

            // REVIEW jvs 31-Oct-2007: doesn't the declaration of
            // monotonicity here assume sort-based aggregation at
            // the physical level?

            // Tell bb which of group columns are sorted.
            bb.columnMonotonicities.clear();
            for (SqlNode groupItem : groupList) {
                bb.columnMonotonicities.add(bb.scope.getMonotonicity(groupItem));
            }

            // Does this statement only contain HAVING? in this case it is not an Aggregation actually
            boolean isHavingOnly = r.groupSet.isEmpty() && aggConverter.getAggCalls().stream()
                .allMatch(c -> c.getAggregation() == SqlStdOperatorTable.__FIRST_VALUE);

            if (!isHavingOnly) {
                // Add the aggregator
                bb.setRoot(createAggregate(bb, r.groupSet, r.groupSets, aggConverter.getAggCalls()), false);
            }

            bb.mapRootRelToFieldProjection.put(bb.root, r.groupExprProjection);

            // Replace sub-queries in having here and modify having to use
            // the replaced expressions
            if (having != null) {
                SqlNode newHaving = pushDownNotForIn(bb.scope, having);
                replaceSubQueries(bb, newHaving, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
                havingExpr = bb.convertExpression(newHaving);
            } else {
                havingExpr = relBuilder.literal(true);
            }

            // Now convert the other sub-queries in the select list.
            // This needs to be done separately from the sub-query inside
            // any aggregate in the select list, and after the aggregate rel
            // is allocated.
            replaceSubQueries(bb, selectList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

            // Now sub-queries in the entire select list have been converted.
            // Convert the select expressions to get the final list to be
            // projected.
            int k = 0;

            // For select expressions, use the field names previously assigned
            // by the validator. If we derive afresh, we might generate names
            // like "EXPR$2" that don't match the names generated by the
            // validator. This is especially the case when there are system
            // fields; system fields appear in the relnode's rowtype but do not
            // (yet) appear in the validator type.
            final SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope(bb.scope);
            assert selectScope != null;
            final SqlValidatorNamespace selectNamespace = validator.getNamespace(selectScope.getNode());
            final List<String> names = selectNamespace.getRowType().getFieldNames();
            int sysFieldCount = selectList.size() - names.size();
            for (SqlNode expr : selectList) {
                projects.add(Pair.of(bb.convertExpression(expr),
                    k < sysFieldCount ? validator.deriveAlias(expr, k++) : names.get(k++ - sysFieldCount)));
            }

            for (SqlNode expr : newOrderByList) {
                projects.add(Pair.of(bb.convertExpression(expr), validator.deriveAlias(expr, k++)));
            }
            for (SqlNode expr : listInOrder) {
                projects.add(Pair.of(bb.convertExpression(expr), validator.deriveAlias(expr, k++)));
            }

            for (SqlIdentifier firstValue : firstValueForSubquery) {
                SqlNode virtualAgg = firstValueMap.get(firstValue);
                firstValueTarget.put(firstValue, ((RexInputRef) (bb.convertExpression(virtualAgg))).getIndex());
            }
        } finally {
            bb.agg = null;
        }

        // implement HAVING (we have already checked that it is non-trivial)
        relBuilder.push(bb.root);
        if (havingExpr != null) {
            relBuilder.filter(havingExpr);
        }

        // implement the SELECT list
        relBuilder.project(Pair.left(projects), Pair.right(projects)).rename(Pair.right(projects));
        RelNode rel = relBuilder.build();
        CorrelationUse correlationUse = null;
        if (rel instanceof Project) {
            Map<Integer, Integer> exprIndex = Maps.newHashMap();
            correlationUse = getCorrelationUse(bb, rel, exprIndex, firstValueForSubquery, firstValueTarget);
            if (exprIndex.size() > 0) {
                // update correlation id rowtype
                RelDataType newType = ((Project) rel).getInput().getRowType();
                RelNode newrel = RexUtil.replaceRexFieldAccess(correlationUse.r, exprIndex, correlationUse.id, newType);
                correlationUse = new CorrelationUse(correlationUse.id, correlationUse.requiredColumns, newrel);
            }
        }

        if (correlationUse == null) {
            bb.setRoot(rel, false);
        } else {
            LogicalProject logicalProject = (LogicalProject) correlationUse.r;
            bb.setRoot(LogicalProject.create(logicalProject.getInput(),
                logicalProject.getProjects(),
                rel.getRowType(),
                ImmutableSet.<CorrelationId>of(correlationUse.id)), false);
        }

        // Tell bb which of group columns are sorted.
        bb.columnMonotonicities.clear();
        for (SqlNode selectItem : selectList) {
            bb.columnMonotonicities.add(bb.scope.getMonotonicity(selectItem));
        }
    }

    /**
     * Creates an Aggregate.
     * <p>
     * In case the aggregate rel changes the order in which it projects fields,
     * the <code>groupExprProjection</code> parameter is provided, and the
     * implementation of this method may modify it.
     * <p>
     * The <code>sortedCount</code> parameter is the number of expressions known
     * to be monotonic. These expressions must be on the leading edge of the
     * grouping keys. The default implementation of this method ignores this
     * parameter.
     *
     * @param bb Blackboard
     * @param groupSet Bit set of ordinals of grouping columns
     * @param groupSets Grouping sets
     * @param aggCalls Array of calls to aggregate functions
     * @return LogicalAggregate
     */
    protected RelNode createAggregate(Blackboard bb, ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
                                      List<AggregateCall> aggCalls) {
        return LogicalAggregate.create(bb.root, groupSet, groupSets, aggCalls);
    }

    public RexDynamicParam convertDynamicParam(final SqlDynamicParam dynamicParam) {
        // REVIEW jvs 8-Jan-2005: dynamic params may be encountered out of
        // order. Should probably cross-check with the count from the parser
        // at the end and make sure they all got filled in. Why doesn't List
        // have a resize() method?!? Make this a utility.
        while (dynamicParam.getIndex() >= dynamicParamSqlNodes.size()) {
            dynamicParamSqlNodes.add(null);
        }

        dynamicParamSqlNodes.set(dynamicParam.getIndex(), dynamicParam);
        RelDataType type = typeFactory.createSqlType(dynamicParam.getTypeName());
        if (SqlTypeUtil.isCharacter(type) && dynamicParam.getCharset() != null) {
            type = typeFactory
                .createTypeWithCharsetAndCollation(type, dynamicParam.getCharset(), dynamicParam.getCollation());
        }
        return rexBuilder.makeDynamicParam(type, dynamicParam.getIndex());
    }

    /**
     * Creates a list of collations required to implement the ORDER BY clause,
     * if there is one. Populates <code>extraOrderExprs</code> with any sort
     * expressions which are not in the select clause.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Select clause. Never null, because we invent a dummy SELECT
     * if ORDER BY is applied to a set operation (UNION etc.)
     * @param orderList Order by clause, may be null
     * @param extraOrderExprs Sort expressions which are not in the select
     * clause (output)
     * @param collationList List of collations (output)
     */
    protected void gatherOrderExprs(Blackboard bb, SqlSelect select, SqlNodeList orderList,
                                    List<SqlNode> extraOrderExprs, List<RelFieldCollation> collationList) {
        // TODO: add validation rules to SqlValidator also
        assert bb.root != null : "precondition: child != null";
        assert select != null;
        if (orderList == null) {
            return;
        }
        for (SqlNode orderItem : orderList) {
            collationList.add(convertOrderItem(select,
                orderItem,
                extraOrderExprs,
                RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.UNSPECIFIED));
        }
    }

    protected RelFieldCollation convertOrderItem(SqlSelect select, SqlNode orderItem, List<SqlNode> extraExprs,
                                                 RelFieldCollation.Direction direction,
                                                 RelFieldCollation.NullDirection nullDirection) {
        assert select != null;
        // Handle DESC keyword, e.g. 'select a, b from t order by a desc'.
        switch (orderItem.getKind()) {
        case DESCENDING:
            return convertOrderItem(select,
                ((SqlCall) orderItem).operand(0),
                extraExprs,
                RelFieldCollation.Direction.DESCENDING,
                nullDirection);
        case NULLS_FIRST:
            return convertOrderItem(select,
                ((SqlCall) orderItem).operand(0),
                extraExprs,
                direction,
                RelFieldCollation.NullDirection.FIRST);
        case NULLS_LAST:
            return convertOrderItem(select,
                ((SqlCall) orderItem).operand(0),
                extraExprs,
                direction,
                RelFieldCollation.NullDirection.LAST);
        }

        SqlNode converted = validator.expandOrderExpr(select, orderItem);

        switch (nullDirection) {
        case UNSPECIFIED:
            nullDirection = validator.getDefaultNullCollation().last(
                desc(direction)) ? RelFieldCollation.NullDirection.LAST : RelFieldCollation.NullDirection.FIRST;
        }

        // Scan the select list and order exprs for an identical expression.
        final SelectScope selectScope = validator.getRawSelectScope(select);
        int ordinal = -1;
        for (SqlNode selectItem : selectScope.getExpandedSelectList()) {
            ++ordinal;
            if (converted.equalsDeep(stripAs(selectItem), Litmus.IGNORE)) {
                return new RelFieldCollation(ordinal, direction, nullDirection);
            }
        }

        for (SqlNode extraExpr : extraExprs) {
            ++ordinal;
            if (converted.equalsDeep(extraExpr, Litmus.IGNORE)) {
                return new RelFieldCollation(ordinal, direction, nullDirection);
            }
        }

        // TODO: handle collation sequence
        // TODO: flag expressions as non-standard

        extraExprs.add(converted);
        return new RelFieldCollation(ordinal + 1, direction, nullDirection);
    }

    private static boolean desc(RelFieldCollation.Direction direction) {
        switch (direction) {
        case DESCENDING:
        case STRICTLY_DESCENDING:
            return true;
        default:
            return false;
        }
    }

    @Deprecated // to be removed before 2.0
    protected boolean enableDecorrelation() {
        // disable sub-query decorrelation when needed.
        // e.g. if outer joins are not supported.
        return config.isDecorrelationEnabled();
    }

    protected RelNode decorrelateQuery(RelNode rootRel) {
        return RelDecorrelator.decorrelateQuery(rootRel);
    }

    /**
     * Returns whether to trim unused fields as part of the conversion process.
     *
     * @return Whether to trim unused fields
     */
    @Deprecated // to be removed before 2.0
    public boolean isTrimUnusedFields() {
        return config.isTrimUnusedFields();
    }

    /**
     * Recursively converts a query to a relational expression.
     *
     * @param query Query
     * @param top Whether this query is the top-level query of the statement
     * @param targetRowType Target row type, or null
     * @return Relational expression
     */
    protected RelRoot convertQueryRecursive(SqlNode query, boolean top, RelDataType targetRowType) {
        final SqlKind kind = query.getKind();
        switch (kind) {
        case SELECT:
            return RelRoot.of(convertSelect((SqlSelect) query, top), kind);
        case INSERT:
        case REPLACE:
            return RelRoot.of(convertInsert((SqlInsert) query), kind);
        case DELETE:
            return RelRoot.of(convertDelete((SqlDelete) query), kind);
        case UPDATE:
            return RelRoot.of(convertUpdate((SqlUpdate) query), kind);
        case MERGE:
            return RelRoot.of(convertMerge((SqlMerge) query), kind);
        case UNION:
        case INTERSECT:
        case EXCEPT:
            return RelRoot.of(convertSetOp((SqlCall) query), kind);
        case WITH:
            return convertWith((SqlWith) query, top);
        case VALUES:
            return RelRoot.of(convertValues((SqlCall) query, targetRowType), kind);
        case CREATE_TABLE:
            return RelRoot.of(convertCreateTable((SqlCreateTable) query), kind);
        case ALTER_TABLE:
            if (query instanceof SqlAlterTablePartitionKey) {
                return RelRoot.of(convertAlterTable((SqlAlterTable) query), kind);
            } else if (query instanceof SqlAlterTableRepartition) {
                return RelRoot.of(convertAlterTable((SqlAlterTable) query), kind);
            }
            return RelRoot.of(convertAlterTable((SqlAlterTable) query), kind);
        case RENAME_TABLE:
            return RelRoot.of(convertRenameTable((SqlRenameTable) query), kind);
        case TRUNCATE_TABLE:
            return RelRoot.of(convertTruncateTable((SqlTruncateTable) query), kind);
        case DROP_TABLE:
            return RelRoot.of(convertDropTable((SqlDropTable) query), kind);
        case CREATE_INDEX:
            return RelRoot.of(convertCreateIndex((SqlCreateIndex) query), kind);
        case DROP_INDEX:
            return RelRoot.of(convertDropIndex((SqlDropIndex) query), kind);
        case CREATE_SEQUENCE:
        case DROP_SEQUENCE:
        case ALTER_SEQUENCE:
        case RENAME_SEQUENCE:
            return RelRoot.of(convertSequenceDdl((SqlSequence) query), kind);
        case ALTER_RULE:
            return RelRoot.of(convertAlterRule((SqlAlterRule) query), kind);
        case CREATE_VIEW:
        case DROP_VIEW:
        case CHECK_GLOBAL_INDEX:
        case REBALANCE:
            return RelRoot.of(convertGenericDdl((SqlDdl) query), kind);
        case MOVE_DATABASE:
            return RelRoot.of(convertMoveDatabase((SqlMoveDatabase) query), kind);
        case FLASHBACK_TABLE:
        case PURGE:
            return RelRoot.of(new LogicalRecyclebin(cluster, query), kind);
        case SHOW:
            SqlShow show = (SqlShow) query;
            return RelRoot.of(convertShow(show), show.getShowKind());
        case CREATE_DATABASE:
            return RelRoot.of(convertCreateDatabase((SqlCreateDatabase) query), kind);
        case DROP_DATABASE:
            return RelRoot.of(convertDropDatabase((SqlDropDatabase) query), kind);
        case ALTER_TABLEGROUP:
            return RelRoot.of(convertAlterTableGroup((SqlAlterTableGroup) query), kind);
        case CREATE_TABLEGROUP:
            return RelRoot.of(convertCreateTableGroup((SqlCreateTableGroup) query), kind);
        case DROP_TABLEGROUP:
            return RelRoot.of(convertDropTableGroup((SqlDropTableGroup) query), kind);
        case ALTER_TABLE_SET_TABLEGROUP:
            return RelRoot.of(convertAlterTableSetTableGroup((SqlAlterTableSetTableGroup) query), kind);
        case CHANGE_CONSENSUS_ROLE:
            return RelRoot.of(convertChangeConsensusRole((SqlChangeConsensusRole) query), kind);
        case ALTER_SYSTEM_SET_CONFIG:
            return RelRoot.of(convertAlterSystemSetConfig((SqlAlterSystemSetConfig) query), kind);
        case REFRESH_TOPOLOGY:
            return RelRoot.of(convertRefreshTopology((SqlRefreshTopology) query), kind);
        case ALTER_FILESTORAGE:
            return RelRoot.of(convertAlterFileStorage((SqlAlterFileStorage) query), kind);
        case UNARCHIVE:
            return RelRoot.of(convertUnArchive((SqlUnArchive) query), kind);
        case DROP_FILESTORAGE:
            return RelRoot.of(convertDropFileStorage((SqlDropFileStorage) query), kind);
        default:
            if (kind.belongsTo(SqlKind.DAL)) {
                return RelRoot.of(convertDal((SqlDal) query), kind);
            }
            throw new AssertionError("not a query: " + query);
        }
    }

    private RelNode convertAlterSystemSetConfig(SqlAlterSystemSetConfig query) {
        RelDataType targetRowType = validator.getValidatedNodeType(query);
        return AlterSystemSetConfig.create(query, targetRowType, getCluster());
    }

    private RelNode convertChangeConsensusRole(SqlChangeConsensusRole query) {
        RelDataType targetRowType = validator.getValidatedNodeType(query);
        return ChangeConsensusRole.create(query, targetRowType, getCluster());
    }

    private RelNode convertCreateIndex(SqlCreateIndex query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        query = checkAndRewriteGsiName(query);
        checkGsiColumnLen(query);
        Map<SqlNode, RexNode> rexNodesForPartition = getRexInfoFromPartition(query.getPartitioning());

        return CreateIndex.create(getCluster(), query, query.getOperandList().get(0), rexNodesForPartition);
    }

    private RelNode convertAlterTable(SqlAlterTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        query = checkAndRewriteGsiName(query);
        if (query.getAlters().size() == 1 && query.getAlters().get(0) instanceof SqlAddIndex) {
            checkGsiColumnLen(query);
        }
        List<SqlAlterSpecification> alterItems = query.getAlters();
        if (1 == alterItems.size()) {
            SqlAlterSpecification alterSpecItem = alterItems.get(0);
            if (alterSpecItem instanceof SqlAddIndex) {
                // All add index goes here for name check.
                return convertAddIndex(query);
            } else if (alterSpecItem instanceof SqlAlterTableModifyPartitionValues) {
                throw new NotSupportException(
                    "alter table add/drop partition, please use alter tablegroup add/drop partition");
            } else if (alterSpecItem instanceof SqlAlterTableSplitPartitionByHotValue) {
                return convertAlterTableSplitHotValue(query);
            }
        } else {
            Optional<SqlAlterSpecification> splitPartitionByHotValue =
                alterItems.stream().filter(o -> o instanceof SqlAlterTableSplitPartitionByHotValue).findFirst();
            if (splitPartitionByHotValue.isPresent()) {
                throw new RuntimeException("alter table split hot value is only allow to be executed separately");
            }
        }
        if (query instanceof SqlAlterTableRepartition) {
            SqlNode partitioning = ((SqlAlterTableRepartition) query).getSqlPartition();
            Map<SqlNode, RexNode> rexNodesForPartition = getRexInfoFromPartition(partitioning);
            return AlterTableRepartition.create(getCluster(), query, query.getOperandList().get(0),
                rexNodesForPartition);
        }

        return AlterTable.create(getCluster(), query, query.getOperandList().get(0), new HashMap<>());
    }

    private RelNode convertRenameTable(SqlRenameTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return RenameTable.create(getCluster(), query, query.getOperandList().get(0), query.getOriginNewTableName());
    }

    private RelNode convertTruncateTable(SqlTruncateTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return TruncateTable.create(getCluster(), query, query.getOperandList().get(0));
    }

    private RelNode convertDropTable(SqlDropTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return DropTable.create(getCluster(), query, query.getOperandList().get(0));
    }

    private RelNode convertAlterFileStorage(SqlAlterFileStorage query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;
        if (query.isAsOf()) {
            return AlterFileStorageAsOfTimestamp.create(getCluster(), getCluster().traitSetOf(Convention.NONE), query,
                targetRowType, query.getFileStorageName().toString(),
                query.getTimestamp().getNlsString().getValue().toString());
        } else if (query.isPurgeBefore()) {
            return AlterFileStoragePurgeBeforeTimestamp.create(getCluster(), getCluster().traitSetOf(Convention.NONE), query,
                targetRowType, query.getFileStorageName().toString(),
                query.getTimestamp().getNlsString().getValue().toString());
        } else if (query.isBackup()) {
            return AlterFileStorageBackup.create(getCluster(), getCluster().traitSetOf(Convention.NONE), query,
                targetRowType, query.getFileStorageName().toString());
        } else {
            throw new AssertionError("unknown alter file storage sql");
        }
    }

    private RelNode convertDropFileStorage(SqlDropFileStorage query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;
        return DropFileStorage.create(getCluster(), getCluster().traitSetOf(Convention.NONE), query,
            targetRowType, query.getName().toString());
    }

    private RelNode convertAlterTableGroup(SqlAlterTableGroup query) {;
        Map<SqlNode, RexNode> partRexInfoCtx = getRexInfoFromSqlAlterSpec(query.getAlters());
        query.setPartRexInfoCtx(partRexInfoCtx);
        assert query.getAlters().size() == 1;
        SqlAlterSpecification item = query.getAlters().get(0);
        String tableGroupName = query.getTableGroupName().toString();
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;
        if (item instanceof SqlAlterTableGroupSplitPartition) {
            return AlterTableGroupSplitPartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType,
                    partRexInfoCtx, tableGroupName);
        } else if (item instanceof SqlAlterTableGroupMergePartition) {
            return AlterTableGroupMergePartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType,
                    partRexInfoCtx, tableGroupName);
        } else if (item instanceof SqlAlterTableGroupMovePartition) {
            Map<String, Set<String>> targetPartitions = new HashMap<>();
            String instId =
                Util.last(((SqlIdentifier) (((SqlAlterTableGroupMovePartition) (item)).getTargetStorageId())).names);
            Set<String> partitionsToBeMoved = ((SqlAlterTableGroupMovePartition) (item)).getOldPartitions().stream()
                .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                    Collectors.toSet());
            targetPartitions.put(instId, partitionsToBeMoved);
            return AlterTableGroupMovePartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType,
                    tableGroupName, targetPartitions);
        } else if (item instanceof SqlAlterTableGroupExtractPartition) {
            return AlterTableGroupExtractPartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType,
                    partRexInfoCtx, tableGroupName);
        } else if (item instanceof SqlAlterTableGroupRenamePartition) {
            return AlterTableGroupRenamePartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType,
                    tableGroupName);
        } else if (item instanceof SqlAlterTableAddPartition) {
            return AlterTableGroupAddPartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType,
                    partRexInfoCtx, tableGroupName);
        } else if (item instanceof SqlAlterTableDropPartition) {
            return AlterTableGroupDropPartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType, tableGroupName);
        } else if (item instanceof SqlAlterTableModifyPartitionValues) {
            return AlterTableGroupModifyPartition
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType, tableGroupName);
        } else if (item instanceof SqlAlterTableTruncatePartition) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "please use alter table truncate partition instead");
        } else if (item instanceof SqlAlterTableGroupSplitPartitionByHotValue) {
            return AlterTableGroupSplitPartitionByHotValue
                .create(getCluster(), getCluster().traitSetOf(Convention.NONE), query, targetRowType, partRexInfoCtx,
                    tableGroupName);
        } else {
            assert false;
            return null;
        }
    }

    private RelNode convertAlterTableSetTableGroup(SqlAlterTableSetTableGroup sqlAlterTableSetTableGroup) {
        final RelDataType targetRowType = validator.getValidatedNodeType(sqlAlterTableSetTableGroup);
        String tableGroupName = sqlAlterTableSetTableGroup.getTargetTableGroup();
        SqlNode tableName = sqlAlterTableSetTableGroup.getTargetTable();
        assert targetRowType != null;
        return AlterTableSetTableGroup
            .create(getCluster(), getCluster().traitSetOf(Convention.NONE), sqlAlterTableSetTableGroup, targetRowType,
                tableName, tableGroupName);
    }

    private RelNode convertRefreshTopology(SqlRefreshTopology sqlRefreshTopology) {
        final RelDataType targetRowType = validator.getValidatedNodeType(sqlRefreshTopology);
        assert targetRowType != null;
        return RefreshTopology
            .create(getCluster(), getCluster().traitSetOf(Convention.NONE), sqlRefreshTopology, targetRowType);
    }

    private RelNode convertCreateTableGroup(SqlCreateTableGroup query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        return CreateTableGroup.create(query, targetRowType, getCluster());
    }

    private RelNode convertDropTableGroup(SqlDropTableGroup query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        return DropTableGroup.create(query, targetRowType, getCluster());
    }

    private RelNode convertAddIndex(SqlAlterTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        query = checkAndRewriteGsiName(query);
        checkGsiColumnLen(query);

        List<SqlAlterSpecification> alters = query.getAlters();
        assert alters.size() == 1;
        SqlAddIndex sqlAddIndex = (SqlAddIndex) alters.get(0);
        SqlNode partitioning = sqlAddIndex.getIndexDef().getPartitioning();
        if (partitioning != null) {
            Map<SqlNode, RexNode> rexNodesForPartition = getRexInfoFromPartition(partitioning);
            return AlterTable.create(getCluster(), query, query.getOperandList().get(0), rexNodesForPartition);
        } else {
            return AlterTable.create(getCluster(), query, query.getOperandList().get(0), new HashMap<>());
        }
    }

    private RelNode convertAlterTableSplitHotValue(SqlAlterTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        List<SqlAlterSpecification> alters = query.getAlters();
        assert alters.size() == 1;
        if (alters.size() > 1) {
            throw new RuntimeException("alter table split hot value is only allow to be executed separately");
        }
        Map<SqlNode, RexNode> rexNodesForPartition = getRexInfoFromSqlAlterSpec(query.getAlters());
        return AlterTable.create(getCluster(), query, query.getOperandList().get(0), rexNodesForPartition);
    }

    //
//    private RelNode convertDropIndex(SqlDropIndex query) {
//        final RelDataType targetRowType = validator.getValidatedNodeType(query);
//        assert targetRowType != null;
//        final LogicalCreateTable logicalCreateTable = LogicalCreateTable.create(getCluster(), null , query, (SqlIdentifier) query.getOperandList().get(0),null);
//        return logicalCreateTable;
//    }

    private RelNode convertDropIndex(SqlDropIndex query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        query = checkAndRewriteGsiName(query);

        return DropIndex.create(getCluster(), query, query.getOperandList().get(0));
    }

    private RelNode convertSequenceDdl(SqlSequence query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return SequenceDdl.create(getCluster(), query, query.getOperandList().get(0));
    }

    private RelNode convertAlterRule(SqlAlterRule query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return AlterRule.create(getCluster(), query, query.getOperandList().get(0));
    }

    private RelNode convertGenericDdl(SqlDdl query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return GenericDdl.create(getCluster(), query, query.getOperandList().get(0));
    }

    private RelNode convertMoveDatabase(SqlDdl query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        return MoveDatabase.create(getCluster(), query, targetRowType);
    }

    private RelNode convertAlterTableModifyListPartitionValues(SqlAlterTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        query = checkAndRewriteGsiName(query);
        checkGsiColumnLen(query);

        List<SqlAlterSpecification> alters = query.getAlters();
        assert alters.size() == 1;
        SqlAlterTableModifyPartitionValues modifyListValues = (SqlAlterTableModifyPartitionValues) alters.get(0);
        SqlPartition sqlPartition = modifyListValues.getPartition();
        SqlPartitionValue sqlPartitionValue = sqlPartition.getValues();
        List<SqlNode> items = sqlPartitionValue.getItems().stream().map(o -> o.getValue()).collect(Collectors.toList());
        Map<SqlNode, RexNode> allRexExprInfo = new HashMap<>();
        //SqlValidatorScope scope = validator.getNamespace()

        for (int i = 0; i < items.size(); i++) {
            SqlNode item = items.get(i);
            validator.validate(item);
            RexNode rexExpr = convertExpression(item);
            allRexExprInfo.put(item, rexExpr);
        }

        return AlterTable.create(getCluster(), query, query.getOperandList().get(0), allRexExprInfo);
    }

    protected RelNode convertDal(SqlDal dal) {
        RelDataType rowType = validator.getValidatedNodeTypeIfKnown(dal);

        return Dal.create(dal, rowType, this.cluster);
    }

    protected RelNode convertShow(SqlShow show) {
        final SqlShow original = show;
        show = checkAndRewriteShow(original);

        if (show.canConvertToSelect()) {
            SqlSelect select = show.convertToSelect();
            validator.validate(select);
            return convertSelect(select, true);
        }

        if (isShowColumns(show.getOperandList())) {
            List<SqlNode> operands = Lists.newArrayList();
            operands.add(new SqlIdentifier("field", SqlParserPos.ZERO));
            operands.add(SqlCharStringLiteral.createCharString(IMPLICIT_COL_NAME, SqlParserPos.ZERO));
            SqlNode columnFilter = new SqlBasicCall(NOT_EQUALS, operands.toArray(new SqlNode[0]), SqlParserPos.ZERO);

            if (show.where != null) {
                List<SqlNode> operandsFilters = Lists.newArrayList();
                operandsFilters.add(show.where);
                operandsFilters.add(columnFilter);
                columnFilter = new SqlBasicCall(SqlStdOperatorTable.AND,
                    operandsFilters.toArray(new SqlNode[0]),
                    SqlParserPos.ZERO);
            }
            show = (SqlShow) show.clone(SqlParserPos.ZERO, columnFilter);
            validator.validate(show);
        }

        RelDataType rowType = validator.getValidatedNodeTypeIfKnown(original); // Use original sql node to get type.

        RelNode result = Show.create(show, rowType, this.cluster);

        if (null != show.where || null != show.orderBy || null != show.limit) {

            SqlValidatorScope scope = validator.getOverScope(show);
            final Blackboard bb = createBlackboard(scope, null, true);
            bb.setRoot(result, true);

            if (appendWhereNode(show) && null != show.where) {
                RexNode rexNode = bb.convertExpression(show.where);

                result = LogicalFilter.create(result, rexNode);

                bb.setRoot(result, true);
            }

            if (appendSortNode(show)) {
                if (null != show.orderBy || null != show.limit) {
                    final List<SqlNode> orderExprList = new ArrayList<>();
                    final List<RelFieldCollation> collationList = new ArrayList<>();
                    SqlSelect select = show.fakeSelect;
                    gatherOrderExprs(bb, select, (SqlNodeList) show.orderBy, orderExprList, collationList);
                    final RelCollation collation = cluster.traitSet().canonize(RelCollations.of(collationList));
                    convertOrder(select, bb, collation, orderExprList, select.getOffset(), select.getFetch());
                    bb.setRoot(bb.root, true);

                    result = bb.root;
                } // end of if
            } // end of if
        } // end of if

        return result;
    }

    private boolean isShowColumns(List<SqlNode> operandList) {
        if (operandList.size() > 0 && operandList.get(0) instanceof SqlLiteral) {
            if (operandList.get(0).toString().equalsIgnoreCase("COLUMNS")) {
                return true;
            }
            if (operandList.get(0).toString().equalsIgnoreCase("FULL")) {
                if (operandList.get(1) instanceof SqlLiteral && operandList.get(1)
                    .toString()
                    .equalsIgnoreCase("COLUMNS")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean appendWhereNode(SqlShow show) {
        final EnumSet skip = EnumSet.of(SqlKind.SHOW_SLOW, SqlKind.SHOW_SEQUENCES);
        return SqlKind.LOGICAL_SHOW_QUERY.contains(show.getShowKind()) && !skip.contains(show.getShowKind());
    }

    private boolean appendSortNode(SqlShow show) {
        final EnumSet skip = EnumSet.of(SqlKind.SHOW_SEQUENCES);
        return SqlKind.LOGICAL_SHOW_QUERY.contains(show.getShowKind()) && !skip.contains(show.getShowKind());
    }

    protected SqlCreateTable checkAndRewriteGsiName(SqlCreateTable query) {
        // implemented in TddlSqlToRelConverter
        return query;
    }

    protected SqlCreateIndex checkAndRewriteGsiName(SqlCreateIndex query) {
        // implemented in TddlSqlToRelConverter
        return query;
    }

    protected SqlDropIndex checkAndRewriteGsiName(SqlDropIndex query) {
        // implemented in TddlSqlToRelConverter
        return query;
    }

    protected SqlAlterTable checkAndRewriteGsiName(SqlAlterTable query) {
        // implemented in TddlSqlToRelConverter
        return query;
    }

    protected SqlShow checkAndRewriteShow(SqlShow show) {
        // implemented in TddlSqlToRelConverter
        return show;
    }

    protected void checkGsiColumnLen(SqlCreateTable create) {
        // implemented in TddlSqlToRelConverter
    }

    protected void checkGsiColumnLen(SqlCreateIndex create) {
        // implemented in TddlSqlToRelConverter
    }

    protected void checkGsiColumnLen(SqlAlterTable query) {
        // implemented in TddlSqlToRelConverter
    }

    private RelNode convertCreateTable(SqlCreateTable query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;

        query = checkAndRewriteGsiName(query);
        checkGsiColumnLen(query);
        Map<SqlNode, RexNode> rexNodesForPartition = convertPartition(query.getSqlPartition());
        if (GeneralUtil.isNotEmpty(query.getGlobalKeys())) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> pair : query.getGlobalKeys()) {
                Map<SqlNode, RexNode> rexNodesForIndexPartition = convertPartition(pair.getValue().getPartitioning());
                rexNodesForPartition.putAll(rexNodesForIndexPartition);
            }
        }
        if (GeneralUtil.isNotEmpty(query.getGlobalUniqueKeys())) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> pair : query.getGlobalUniqueKeys()) {
                Map<SqlNode, RexNode> rexNodesForIndexPartition = convertPartition(pair.getValue().getPartitioning());
                rexNodesForPartition.putAll(rexNodesForIndexPartition);
            }
        }
        if (GeneralUtil.isNotEmpty(query.getClusteredKeys())) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> pair : query.getClusteredKeys()) {
                Map<SqlNode, RexNode> rexNodesForIndexPartition = convertPartition(pair.getValue().getPartitioning());
                rexNodesForPartition.putAll(rexNodesForIndexPartition);
            }
        }
        if (GeneralUtil.isNotEmpty(query.getClusteredUniqueKeys())) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> pair : query.getClusteredUniqueKeys()) {
                Map<SqlNode, RexNode> rexNodesForIndexPartition = convertPartition(pair.getValue().getPartitioning());
                rexNodesForPartition.putAll(rexNodesForIndexPartition);
            }
        }
        return CreateTable
            .create(getCluster(), query, query.getOperandList().get(0), query.getLikeTableName(), rexNodesForPartition);
    }

    private Map<SqlNode, RexNode> getRexInfoFromPartition(SqlNode partitioning) {
        SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) partitioning;
        Map<SqlNode, RexNode> partRexInfoCtx = new HashMap<>();
        if (sqlPartitionBy == null) {
            return partRexInfoCtx;
        }

        for (SqlNode sqlNode : sqlPartitionBy.getPartitions()) {
            SqlPartition sqlPartition = (SqlPartition) sqlNode;
            SqlPartitionValue sqlPartitionValue = sqlPartition.getValues();
            for (SqlPartitionValueItem item : ((SqlPartition) sqlPartition).getValues().getItems()) {
                SqlNode itemValue = item.getValue();
                if (!item.isMaxValue()) {
                    validator.validate(itemValue);
                    RexNode rexExpr = convertExpression(itemValue);
                    partRexInfoCtx.put(itemValue, rexExpr);
                } else {
                    // add null literal as placeholder, do not need to derive actual value and value type for maxvalue
                    SqlLiteral sqlLiteral =
                        SqlLiteral.createLiteralForIntTypes("0", SqlParserPos.ZERO, SqlTypeName.INTEGER);
                    partRexInfoCtx.put(itemValue, convertExpression(sqlLiteral));
                }
            }
        }
        return partRexInfoCtx;
    }

    private Map<SqlNode, RexNode> convertPartition(SqlNode sqlPartitionBy) {
        Map<SqlNode, RexNode> partRexInfoCtx = new HashMap<>();
        if (sqlPartitionBy != null) {
            final SqlValidatorScope partitionScope = validator.getPartitionByScope(sqlPartitionBy);
            final Blackboard bb = createBlackboard(partitionScope, null, false);
            for (SqlNode sqlNode : ((SqlPartitionBy) sqlPartitionBy).getColumns()) {
//                if (sqlNode instanceof SqlCall) {
//                    partRexInfoCtx.put(sqlNode, bb.convertExpression(sqlNode));
//                }
            }
            for (SqlNode sqlPartition : ((SqlPartitionBy) sqlPartitionBy).getPartitions()) {
                for (SqlPartitionValueItem item : ((SqlPartition) sqlPartition).getValues().getItems()) {
                    if (!item.isMaxValue()) {
                        partRexInfoCtx.put(item.getValue(), bb.convertExpression(item.getValue()));
                    } else {
                        // add null literal as placeholder
                        SqlLiteral sqlLiteral =
                            SqlLiteral.createLiteralForIntTypes("0", SqlParserPos.ZERO, SqlTypeName.INTEGER);
                        partRexInfoCtx.put(item.getValue(), bb.convertExpression(sqlLiteral));
                    }
//                    if (sqlNode instanceof SqlCall) {
//                        partRexInfoCtx.put(sqlNode, bb.convertExpression(sqlNode));
//                    } else if (sqlNode instanceof SqlLiteral) {
//                        partRexInfoCtx.put(sqlNode, bb.convertLiteral((SqlLiteral) sqlNode));
//                    }
                }
            }
        }
        return partRexInfoCtx;
    }

    private Map<SqlNode, RexNode> getRexInfoFromSqlAlterSpec(List<SqlAlterSpecification> sqlAlterSpecifications) {
        //final SqlValidatorScope partitionScope = validator
        Map<SqlNode, RexNode> partRexInfoCtx = new HashMap<>();
        if (GeneralUtil.isNotEmpty(sqlAlterSpecifications)) {
            assert sqlAlterSpecifications.size() == 1;
            final Blackboard bb = createBlackboard(null, null, false);
            if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableGroupSplitPartition) {
                SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
                    (SqlAlterTableGroupSplitPartition) sqlAlterSpecifications.get(0);
                SqlNode atVal = sqlAlterTableGroupSplitPartition.getAtValue();
                if (atVal != null) {
                    partRexInfoCtx.put(atVal, bb.convertExpression(atVal));
                } else if (GeneralUtil.isNotEmpty(sqlAlterTableGroupSplitPartition.getNewPartitions())) {
                    List<SqlPartition> sqlPartitions = sqlAlterTableGroupSplitPartition.getNewPartitions();
                    for (SqlPartition sqlPartition : sqlPartitions) {
                        SqlPartitionValue partVal = sqlPartition.getValues();
                        for (SqlPartitionValueItem valItem : partVal.getItems()) {
                            if (valItem.isMaxValue() || valItem.isMinValue()) {
                                continue;
                            }
                            RexNode rexExpr = convertExpression(valItem.getValue());
                            partRexInfoCtx.put(valItem.getValue(), bb.convertExpression(valItem.getValue()));
                        }
                    }
                }
            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableGroupMovePartition) {

            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableGroupMergePartition) {

            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableGroupExtractPartition) {
                SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
                    (SqlAlterTableGroupExtractPartition) sqlAlterSpecifications.get(0);
                for (SqlNode hotKey : sqlAlterTableGroupExtractPartition.getHotKeys()) {
                    partRexInfoCtx.put(hotKey, bb.convertExpression(hotKey));
                    if (hotKey != null) {
                        partRexInfoCtx.put(hotKey, bb.convertExpression(hotKey));
                    }
                }
            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableAddPartition) {

                SqlAlterTableAddPartition addPartition =
                    (SqlAlterTableAddPartition) sqlAlterSpecifications.get(0);
                for (SqlNode sqlNode : addPartition.getPartitions()) {
                    SqlPartition sqlPartition = (SqlPartition) sqlNode;
                    SqlPartitionValue sqlPartitionValue = sqlPartition.getValues();
                    for (SqlPartitionValueItem valItem : sqlPartitionValue.getItems()) {
                        if (valItem.isMaxValue() || valItem.isMinValue()) {
                            continue;
                        }
                        SqlNode exprAst = valItem.getValue();
                        validator.validate(exprAst);
                        RexNode rexExpr = convertExpression(exprAst);
                        partRexInfoCtx.put(exprAst, rexExpr);
                    }
                }

            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableDropPartition) {

            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableModifyPartitionValues) {

                SqlAlterTableModifyPartitionValues modifyPartition =
                    (SqlAlterTableModifyPartitionValues) sqlAlterSpecifications.get(0);
                SqlPartition sqlPartition = modifyPartition.getPartition();
                SqlPartitionValue sqlPartitionValue = sqlPartition.getValues();
                List<SqlNode> items =
                    sqlPartitionValue.getItems().stream().map(o -> o.getValue()).collect(Collectors.toList());
                for (int i = 0; i < items.size(); i++) {
                    SqlNode item = items.get(i);
                    validator.validate(item);
                    RexNode rexExpr = convertExpression(item);
                    partRexInfoCtx.put(item, rexExpr);
                }

            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableTruncatePartition) {
                //return RelRoot.of(convertAlterTableTruncatePartition((SqlAlterTable) query), kind);
            } else if (sqlAlterSpecifications.get(0) instanceof SqlAlterTableSplitPartitionByHotValue) {
                SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue =
                    (SqlAlterTableSplitPartitionByHotValue) sqlAlterSpecifications.get(0);
                SqlNode partitions = sqlAlterTableSplitPartitionByHotValue.getPartitions();
                if (partitions != null) {
                    partRexInfoCtx.put(partitions, bb.convertExpression(partitions));
                }
                for (SqlNode hotKey : sqlAlterTableSplitPartitionByHotValue.getHotKeys()) {
                    partRexInfoCtx.put(hotKey, bb.convertExpression(hotKey));
                }
            }

        }
        return partRexInfoCtx;
    }

    private RelNode convertCreateDatabase(SqlCreateDatabase query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        return CreateDatabase.create(query, targetRowType, getCluster());
    }

    private RelNode convertDropDatabase(SqlDropDatabase query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        return DropDatabase.create(query, targetRowType, getCluster());
    }

    /**
     * Converts a set operation (UNION, INTERSECT, MINUS) into relational
     * expressions.
     *
     * @param call Call to set operator
     * @return Relational expression
     */
    protected RelNode convertSetOp(SqlCall call) {
        final RelNode left = convertQueryRecursive(call.operand(0), false, null).project();
        final RelNode right = convertQueryRecursive(call.operand(1), false, null).project();
        switch (call.getKind()) {
        case UNION:
            return LogicalUnion.create(ImmutableList.of(left, right), all(call));

        case INTERSECT:
            return LogicalIntersect.create(ImmutableList.of(left, right), all(call));

        case EXCEPT:
            return LogicalMinus.create(ImmutableList.of(left, right), all(call));

        default:
            throw Util.unexpected(call.getKind());
        }
    }

    private boolean all(SqlCall call) {
        return ((SqlSetOperator) call.getOperator()).isAll();
    }

    /*
     **create table test1(id bigint(20) unsigned NOT NULL AUTO_INCREMENT, b int, primary key(id));
     **insert into test1 values(default,1);
     * change the datatype of default from ANY to the target type
     */
    private void resolveDefaultForInsert(SqlInsert call, RelDataType targetRowType) {
        final SqlNode source = call.getSource();
        boolean needResolve = false;
        if (source instanceof SqlBasicCall) {
            SqlOperator sqlOperator = ((SqlBasicCall) source).getOperator();
            if (sqlOperator instanceof SqlValuesOperator) {
                SqlNode[] rowValSqlNodes = ((SqlBasicCall) (source)).getOperands();
                for (int row = 0; row < rowValSqlNodes.length; row++) {
                    if (rowValSqlNodes[row] instanceof SqlBasicCall) {
                        SqlOperator rowOperator = ((SqlBasicCall) rowValSqlNodes[row]).getOperator();
                        if (rowOperator instanceof SqlRowOperator) {
                            SqlNode[] valSqlNodes = ((SqlBasicCall) rowValSqlNodes[row]).getOperands();
                            for (int index = 0; index < valSqlNodes.length; index++) {
                                if (valSqlNodes[index] instanceof SqlBasicCall &&
                                    ((SqlBasicCall) valSqlNodes[index]).getOperator() instanceof SqlDefaultOperator) {
                                    SqlBasicCall defaultFunctionCall = new SqlBasicCall(
                                        new SqlDefaultOperator(
                                            targetRowType.getFieldList().get(index).getType().getSqlTypeName()),
                                        ((SqlBasicCall) valSqlNodes[index]).getOperands(),
                                        valSqlNodes[index].getParserPosition());
                                    valSqlNodes[index] = defaultFunctionCall;
                                    needResolve = true;
                                }
                            }
                            if (needResolve) {
                                needResolve = false;
                                ((SqlBasicCall) source).setOperand(row, rowValSqlNodes[row]);
                            }
                        }

                    }
                }
            }
        }
    }

    protected RelNode convertInsert(SqlInsert call) {
        RelOptTable targetTable = getTargetTable(call);

        final RelDataType targetRowType = validator.getValidatedNodeType(call);
        assert targetRowType != null;
        RelDataType rowtypeForSourceRel = null;
        if (call.getTargetColumnList() == null || call.getTargetColumnList().size() != targetRowType.getFieldCount()) {
            rowtypeForSourceRel = removeImplictKeyRowType(targetRowType);
        } else {
            rowtypeForSourceRel = targetRowType;
        }
//        resolveDefaultForInsert(call, targetRowType);
        RelNode sourceRel = convertQueryRecursive(call.getSource(), false, rowtypeForSourceRel).project(false);
        if (sourceRel.getRowType().getFieldCount() != targetRowType.getFieldCount()) {
            List<String> originalNames = Lists.newArrayList();
            originalNames.addAll(sourceRel.getRowType().getFieldNames());
            List<RexNode> newPros = Lists.newLinkedList();
            for (int i = 0; i < sourceRel.getRowType().getFieldCount(); i++) {
                newPros.add(i, rexBuilder.makeInputRef(sourceRel, i));
            }
            // handle implicit column value
            for (int i = 0; i < targetRowType.getFieldCount(); i++) {
                if (isImplicitKey(targetRowType.getFieldNames().get(i))) {
                    newPros.add(i, rexBuilder.makeLiteral(0, targetRowType.getFieldList().get(i).getType(), true));
                    originalNames.add(i, targetRowType.getFieldNames().get(i));
                }
            }
            sourceRel = LogicalProject.create(sourceRel, newPros,
                RexUtil.createOriginalStructType(sourceRel.getCluster().getTypeFactory(), newPros, originalNames));
        }

        final Set<Integer> appended = new HashSet<>();
        RelNode massagedRel = convertColumnList(call, sourceRel, appended);

        LogicalTableModify.Operation op = LogicalTableModify.Operation.mapFromSqlKind(call.getKind());
        return createModify(targetTable, massagedRel, op, call.getBatchSize(), appended, call.getHints());
    }

    private RelDataType removeImplictKeyRowType(RelDataType targetRowType) {
        List<RelDataType> relDataTypes = Lists.newLinkedList();
        List<String> fieldsName = Lists.newLinkedList();
        for (RelDataTypeField fieldType : targetRowType.getFieldList()) {
            if (!isImplicitKey(fieldType.getName())) {
                relDataTypes.add(fieldType.getType());
                fieldsName.add(fieldType.getName());
            }
        }
        return typeFactory.createStructType(relDataTypes, fieldsName);
    }

    /**
     * Creates a relational expression to modify a table or modifiable view.
     */
    private RelNode createModify(RelOptTable targetTable, RelNode source,
                                 LogicalTableModify.Operation op, int batchSize, Set<Integer> appendedColumnIndex,
                                 SqlNodeList hints) {
        final ModifiableTable modifiableTable = targetTable.unwrap(ModifiableTable.class);
        if (modifiableTable != null && modifiableTable == targetTable.unwrap(Table.class)) {
            return modifiableTable
                .toModificationRel(cluster, targetTable, catalogReader, source, op, null, null, false);
        }
        final ModifiableView modifiableView = targetTable.unwrap(ModifiableView.class);
        if (modifiableView != null) {
            final Table delegateTable = modifiableView.getTable();
            final RelDataType delegateRowType = delegateTable.getRowType(typeFactory);
            final RelOptTable delegateRelOptTable = RelOptTableImpl
                .create(null, delegateRowType, delegateTable, modifiableView.getTablePath());
            final RelNode newSource = createSource(targetTable, source, modifiableView, delegateRowType);
            return createModify(delegateRelOptTable, newSource, op, batchSize, appendedColumnIndex, hints);
        }
        return LogicalTableModify.create(targetTable,
            catalogReader,
            source,
            op,
            null,
            null,
            false,
            batchSize,
            appendedColumnIndex,
            hints,
            TableInfo.singleSource(targetTable, source));
    }

    /**
     * Wraps a relational expression in the projects and filters implied by a
     * {@link ModifiableView}.
     * <p>
     * The input relational expression is suitable for inserting into the view,
     * and the returned relational expression is suitable for inserting into its
     * delegate table.
     * <p>
     * In principle, the delegate table of a view might be another modifiable
     * view, and if so, the process can be repeated.
     */
    private RelNode createSource(RelOptTable targetTable, RelNode source, ModifiableView modifiableView,
                                 RelDataType delegateRowType) {
        final ImmutableIntList mapping = modifiableView.getColumnMapping();
        assert mapping.size() == targetTable.getRowType().getFieldCount();

        // For columns represented in the mapping, the expression is just a
        // field
        // reference.
        final Map<Integer, RexNode> projectMap = new HashMap<>();
        final List<RexNode> filters = new ArrayList<>();
        for (int i = 0; i < mapping.size(); i++) {
            int target = mapping.get(i);
            if (target >= 0) {
                projectMap.put(target, RexInputRef.of(i, source.getRowType()));
            }
        }

        // For columns that are not in the mapping, and have a constraint of the
        // form "column = value", the expression is the literal "value".
        //
        // If a column has multiple constraints, the extra ones will become a
        // filter.
        final RexNode constraint = modifiableView.getConstraint(rexBuilder, delegateRowType);
        RelOptUtil.inferViewPredicates(projectMap, filters, constraint);
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        for (RelDataTypeField field : delegateRowType.getFieldList()) {
            RexNode node = projectMap.get(field.getIndex());
            if (node == null) {
                node = rexBuilder.makeNullLiteral(field.getType());
            }
            projects.add(Pair.of(rexBuilder.ensureType(field.getType(), node, false), field.getName()));
        }

        source = RelOptUtil.createProject(source, projects, true);
        if (filters.size() > 0) {
            source = RelOptUtil.createFilter(source, filters);
        }
        return source;
    }

    public RelOptTable.ToRelContext createToRelContext() {
        return new RelOptTable.ToRelContext() {

            public RelOptCluster getCluster() {
                return cluster;
            }

            @Override
            public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                                      List<String> viewPath) {
                return viewExpander.expandView(rowType, queryString, schemaPath, viewPath);
            }

        };
    }

    protected boolean supportInValuesConvertJoin(
        List<String> columnNames, SqlNodeList valueList, Blackboard bb) {
        return false;
    }

    public RelNode toRel(final RelOptTable table) {
        final RelNode scan = table.toRel(createToRelContext());

        final InitializerExpressionFactory ief = Util.first(table.unwrap(InitializerExpressionFactory.class),
            NullInitializerExpressionFactory.INSTANCE);

        // Lazily create a blackboard that contains all non-generated columns.
        final Supplier<Blackboard> bb = new Supplier<Blackboard>() {

            public Blackboard get() {
                RexNode sourceRef = rexBuilder.makeRangeReference(scan);
                return createInsertBlackboard(table, sourceRef, table.getRowType().getFieldNames());
            }
        };

        int virtualCount = 0;
        final List<RexNode> list = new ArrayList<>();
        for (RelDataTypeField f : table.getRowType().getFieldList()) {
            final ColumnStrategy strategy = ief.generationStrategy(table, f.getIndex());
            switch (strategy) {
            case VIRTUAL:
                list.add(ief.newColumnDefaultValue(table, f.getIndex(), bb.get()));
                ++virtualCount;
                break;
            default:
                list.add(rexBuilder.makeInputRef(scan, RelOptTableImpl.realOrdinal(table, f.getIndex())));
            }
        }
        if (virtualCount > 0) {
            relBuilder.push(scan);
            relBuilder.project(list);
            return relBuilder.build();
        }
        return scan;
    }

    public RelOptTable getTargetTable(SqlNode call) {
        final SqlValidatorNamespace targetNs = validator.getNamespace(call);
        if (targetNs.isWrapperFor(SqlValidatorImpl.DmlNamespace.class)) {
            final SqlValidatorImpl.DmlNamespace dmlNamespace = targetNs.unwrap(SqlValidatorImpl.DmlNamespace.class);
            return SqlValidatorUtil.getRelOptTable(dmlNamespace, catalogReader, null, null);
        }
        final SqlValidatorNamespace resolvedNamespace = targetNs.resolve();
        return SqlValidatorUtil.getRelOptTable(resolvedNamespace, catalogReader, null, null);
    }

    protected List<TableInfoNode> getUpdateSrcTables(SqlUpdate sqlUpdate) {
        List<TableInfoNode> result = new ArrayList<>();

        for (SqlNode srcWithAlias : sqlUpdate.getAliases()) {
            SqlNode srcNode = srcWithAlias;

            if (srcWithAlias instanceof SqlBasicCall && srcWithAlias.getKind() == SqlKind.AS) {
                srcNode = ((SqlBasicCall) srcWithAlias).getOperands()[0];
            }

            if (srcNode instanceof SqlIdentifier) {
                // Table name
                result.add(new TableInfoNode(srcNode, srcWithAlias, ImmutableList.of(getTargetTable(srcNode))));
            } else if (srcNode instanceof SqlSelect) {
                // SubQuery
                final List<SqlIdentifier> tableNames = sqlUpdate.subQueryTables(srcNode);
                result.add(new TableInfoNode(srcNode,
                    srcWithAlias,
                    tableNames.stream().map(this::getTargetTable).collect(Collectors.toList())));
            } else {
                Assert.fail("Should not be here");
            }
        }

        return result;
    }

    protected TableInfo getDeleteTargetTables(SqlDelete sqlDelete, Blackboard bb, RelNode sourceRel) {
        final SqlNameMatcher sqlNameMatcher = this.catalogReader.nameMatcher();

        final List<Map<String, Integer>> sourceColumnIndexMap =
            getColumnIndexMap(bb, sourceRel, sqlDelete.getSourceTableColumns());

        final SqlNodeList targetTables = sqlDelete.getTargetTables();

        final Map<String, Integer> aliasNodeMap = sqlNameMatcher
            .isCaseSensitive() ? new HashMap<>() : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < sqlDelete.getAliases().size(); i++) {
            final SqlNode aliasNode = sqlDelete.getAliases().get(i);
            aliasNodeMap.put(SqlValidatorUtil.getAlias(aliasNode, -1), i);
        }

        final List<Integer> targetTableIndexes = new ArrayList<>();
        for (SqlNode targetTable : targetTables) {
            final Integer i = aliasNodeMap.get(SqlValidatorUtil.getAlias(targetTable, -1));
            targetTableIndexes.add(i);
        }

        final Map<SqlNode, List<SqlIdentifier>> subqueryTableMap = new HashMap<>(sqlDelete.getSubQueryTableMap());
        final List<TableModify.TableInfoNode> srcTableInfos = new ArrayList<>();
        for (SqlNode srcWithAlias : sqlDelete.getAliases()) {
            SqlNode srcNode = srcWithAlias;

            if (srcWithAlias instanceof SqlBasicCall && srcWithAlias.getKind() == SqlKind.AS) {
                srcNode = ((SqlBasicCall) srcWithAlias).getOperands()[0];
            }

            if (srcNode instanceof SqlIdentifier) {
                // Table name
                srcTableInfos.add(
                    new TableModify.TableInfoNode(srcNode, srcWithAlias, ImmutableList.of(getTargetTable(srcNode))));
            } else if (srcNode instanceof SqlSelect) {
                // SubQuery
                final List<SqlIdentifier> tableNames = sqlDelete.subQueryTables(srcNode);
                srcTableInfos.add(new TableModify.TableInfoNode(srcNode,
                    srcWithAlias,
                    tableNames.stream().map(this::getTargetTable).collect(Collectors.toList())));

                subqueryTableMap.remove(srcNode);
            } else {
                Assert.fail("Should not be here");
            }
        }

        final List<TableModify.TableInfoNode> refTableInfos = new ArrayList<>();
        subqueryTableMap.forEach((k, v) -> refTableInfos
            .add(new TableModify.TableInfoNode(k, k,
                v.stream().map(this::getTargetTable).collect(Collectors.toList()))));

        return TableInfo.create(sqlDelete, srcTableInfos, targetTableIndexes, sourceColumnIndexMap, refTableInfos);
    }

    protected List<Map<String, Integer>> getColumnIndexMap(Blackboard bb, RelNode sourceRel,
                                                           List<Pair<SqlNode, List<SqlNode>>> targetTableColumns) {
        throw new UnsupportedOperationException("DrdsRelMdColumnOriginNames is required");
    }

    /**
     * Creates a source for an INSERT statement.
     * <p>
     * If the column list is not specified, source expressions match target
     * columns in order.
     * <p>
     * If the column list is specified, Source expressions are mapped to target
     * columns by name via targetColumnList, and may not cover the entire target
     * table. So, we'll make up a full row, using a combination of default
     * values and the source expressions provided.
     *
     * @param call Insert expression
     * @param source Source relational expression
     * @return Converted INSERT statement
     */
    protected RelNode convertColumnList(final SqlInsert call, RelNode source, Set<Integer> appendedColumnIndex) {
        RelDataType sourceRowType = source.getRowType();
        final RexNode sourceRef = rexBuilder.makeRangeReference(sourceRowType, 0, false);
        final List<String> targetColumnNames = new ArrayList<>();
        final List<RexNode> columnExprs = new ArrayList<>();
        collectInsertTargets(call, sourceRef, targetColumnNames, columnExprs);

        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType targetRowType = RelOptTableImpl.realRowType(targetTable);
        final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final List<RexNode> sourceExps = new ArrayList<>(Collections.<RexNode>nCopies(targetFields.size(), null));
        final List<String> fieldNames = new ArrayList<>(Collections.<String>nCopies(targetFields.size(), null));

        final InitializerExpressionFactory initializerFactory = getInitializerFactory(
            validator.getNamespace(call).getTable());

        // Walk the name list and place the associated value in the
        // expression list according to the ordinal value returned from
        // the table construct, leaving nulls in the list for columns
        // that are not referenced.
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        for (Pair<String, RexNode> p : Pair.zip(targetColumnNames, columnExprs)) {
            RelDataTypeField field = nameMatcher.field(targetRowType, p.left);
            assert field != null : "column " + p.left + " not found";
            sourceExps.set(field.getIndex(), p.right);
        }

        // Lazily create a blackboard that contains all non-generated columns.
        final Supplier<Blackboard> bb = new Supplier<Blackboard>() {

            public Blackboard get() {
                return createInsertBlackboard(targetTable, sourceRef, targetColumnNames);
            }
        };

        // Walk the expression list and get default values for any columns
        // that were not supplied in the statement. Get field names too.
        for (int i = 0; i < targetFields.size(); ++i) {
            final RelDataTypeField field = targetFields.get(i);
            final String fieldName = field.getName();
            fieldNames.set(i, fieldName);
            if (sourceExps.get(i) == null || sourceExps.get(i).getKind() == SqlKind.DEFAULT) {
                sourceExps.set(i, initializerFactory.newColumnDefaultValue(targetTable, i, bb.get()));

                // bare nulls are dangerous in the wrong hands
                sourceExps.set(i, castNullLiteralIfNeeded(sourceExps.get(i), field.getType()));

                if (null != appendedColumnIndex && null == sourceExps.get(i)) {
                    appendedColumnIndex.add(i);
                }
            }
        }

        return RelOptUtil.createProject(source, sourceExps, fieldNames, true);
    }

    /**
     * Creates a blackboard for translating the expressions of generated columns
     * in an INSERT statement.
     */
    protected Blackboard createInsertBlackboard(RelOptTable targetTable, RexNode sourceRef,
                                                List<String> targetColumnNames) {
        final Map<String, RexNode> nameToNodeMap = new HashMap<>();
        int j = 0;

        // Assign expressions for non-generated columns.
        final List<ColumnStrategy> strategies = targetTable.getColumnStrategies();
        final List<String> targetFields = targetTable.getRowType().getFieldNames();
        for (String targetColumnName : targetColumnNames) {
            final int i = targetFields.indexOf(targetColumnName);
            switch (strategies.get(i)) {
            case STORED:
            case VIRTUAL:
                break;
            default:
                nameToNodeMap.put(targetColumnName, rexBuilder.makeFieldAccess(sourceRef, j++));
            }
        }
        return createBlackboard(null, nameToNodeMap, false);
    }

    protected InitializerExpressionFactory getInitializerFactory(SqlValidatorTable validatorTable) {
        // We might unwrap a null instead of a InitializerExpressionFactory.
        final Table table = unwrap(validatorTable, Table.class);
        if (table != null) {
            InitializerExpressionFactory f = unwrap(table, InitializerExpressionFactory.class);
            if (f != null) {
                return f;
            }
        }
        return NullInitializerExpressionFactory.INSTANCE;
    }

    private static <T> T unwrap(Object o, Class<T> clazz) {
        if (o instanceof Wrapper) {
            return ((Wrapper) o).unwrap(clazz);
        }
        return null;
    }

    protected RexNode castNullLiteralIfNeeded(RexNode node, RelDataType type) {
        if (!RexLiteral.isNullLiteral(node)) {
            return node;
        }
        return rexBuilder.makeCast(type, node, false);
    }

    /**
     * Given an INSERT statement, collects the list of names to be populated and
     * the expressions to put in them.
     *
     * @param call Insert statement
     * @param sourceRef Expression representing a row from the source relational
     * expression
     * @param targetColumnNames List of target column names, to be populated
     * @param columnExprs List of expressions, to be populated
     */
    protected void collectInsertTargets(SqlInsert call, final RexNode sourceRef, final List<String> targetColumnNames,
                                        List<RexNode> columnExprs) {
        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType tableRowType = targetTable.getRowType();
        SqlNodeList targetColumnList = call.getTargetColumnList();
        if (targetColumnList == null) {
            if (validator.getConformance().isInsertSubsetColumnsAllowed()) {
                final RelDataType targetRowType = typeFactory
                    .createStructType(tableRowType.getFieldList().subList(0, sourceRef.getType().getFieldCount()));
                targetColumnNames.addAll(targetRowType.getFieldNames());
            } else {
                targetColumnNames.addAll(tableRowType.getFieldNames());
            }
        } else {
            for (int i = 0; i < targetColumnList.size(); i++) {
                SqlIdentifier id = (SqlIdentifier) targetColumnList.get(i);
                RelDataTypeField field = SqlValidatorUtil
                    .getTargetField(tableRowType, typeFactory, id, catalogReader, targetTable);
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNames.add(field.getName());
            }
        }

        final Blackboard bb = createInsertBlackboard(targetTable, sourceRef, targetColumnNames);

        // Next, assign expressions for generated columns.
        final List<ColumnStrategy> strategies = targetTable.getColumnStrategies();
        for (String columnName : targetColumnNames) {
            final int i = tableRowType.getFieldNames().indexOf(columnName);
            final RexNode expr;
            switch (strategies.get(i)) {
            case STORED:
                final InitializerExpressionFactory f = Util.first(
                    targetTable.unwrap(InitializerExpressionFactory.class),
                    NullInitializerExpressionFactory.INSTANCE);
                expr = f.newColumnDefaultValue(targetTable, i, bb);
                break;
            case VIRTUAL:
                expr = null;
                break;
            default:
                expr = bb.nameToNodeMap.get(columnName);
            }
            columnExprs.add(expr);
        }

        // Remove virtual columns from the list.
        for (int i = 0; i < targetColumnNames.size(); i++) {
            if (columnExprs.get(i) == null) {
                columnExprs.remove(i);
                targetColumnNames.remove(i);
                --i;
            }
        }
    }

    protected void interceptDMLAllTableSql(SqlNode sqlNode) {
        return;
    }

    private RelNode convertDelete(SqlDelete call) {
        interceptDMLAllTableSql(call);

        final SqlSelect sourceSelect = call.getSourceSelect();

        final SqlValidatorScope selectScope = validator.getWhereScope(sourceSelect);
        final Blackboard bb = createBlackboard(selectScope, null, false);
        convertSelectImpl(bb, sourceSelect);
        RelNode sourceRel = bb.root;

        // DELETE part belongs to the same scope with source select
        // so revert last bb.setRoot(bb.root, true); executed in convertSelectImpl()
        final RelNode last = this.leaves.remove(this.leaves.size() - 1);
        final TableInfo tableInfo = getDeleteTargetTables(call, bb, sourceRel);

        return LogicalTableModify.create(tableInfo.getTargetTables().get(0),
            catalogReader,
            sourceRel,
            LogicalTableModify.Operation.DELETE,
            null,
            null,
            false,
            SqlDmlKeyword.convertFromSqlNodeToString(call.getKeywords()),
            0,
            null,
            call.getHints(),
            tableInfo,
            ImmutableList.of(),
            ImmutableList.of());
    }

    private RelNode convertUpdate(SqlUpdate call) {
        interceptDMLAllTableSql(call);

        // Source table info
        // Map column to table it belongs to
        final Map<String, Integer> aliasTableMap = new HashMap<>();
        final Map<String, List<Integer>> columnTableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final List<TableInfoNode> srcTableInfoNodes = buildUpdateSourceInfo(call, aliasTableMap, columnTableMap);

        // Target table info
        final List<String> targetColumns = new ArrayList<>();
        final List<Integer> targetTableIndexes = new ArrayList<>();
        buildUpdateTargetColumnInfo(call.getTargetColumnList()
            .getList(), srcTableInfoNodes, aliasTableMap, columnTableMap, targetColumns, targetTableIndexes);

        // Source select
        final SqlValidatorScope scope = validator.getWhereScope(call.getSourceSelect());
        Blackboard bb = createBlackboard(scope, null, false);

        final List<Integer> extraTargetTables = new ArrayList<>();
        final List<String> extraTargetColumns = new ArrayList<>();
        final SqlSelect sourceSelect = rewriteUpdateSourceSelect(call,
            targetTableIndexes,
            targetColumns,
            srcTableInfoNodes,
            extraTargetTables,
            extraTargetColumns);
        targetTableIndexes.addAll(extraTargetTables);
        targetColumns.addAll(extraTargetColumns);

        convertSelectImpl(bb, sourceSelect);
        RelNode sourceRel = bb.root;

        // UPDATE part belongs to the same scope with source select
        // so revert last bb.setRoot(bb.root, true); executed in convertSelectImpl()
        final RelNode last = this.leaves.remove(this.leaves.size() - 1);

        final List<Map<String, Integer>> sourceColumnIndexMap =
            getColumnIndexMap(bb, sourceRel, call.getSourceTableColumns());
        final TableInfo tableInfo = TableInfo
            .create(call.getSourceSelect().getFrom(), srcTableInfoNodes, targetTableIndexes, sourceColumnIndexMap);

        final List<String> newTargetColumns = new ArrayList<>();
        final List<Integer> newTargetTables = new ArrayList<>();
        sourceRel = transformUpdateSourceRel(sourceRel,
            tableInfo,
            targetColumns,
            sourceColumnIndexMap,
            newTargetColumns,
            newTargetTables);

        // Target expressions
        final List<RexNode> projects = getProjects(sourceRel);
        final int first = projects.size() - newTargetColumns.size();
        final Builder<RexNode> rexNodeSourceExpressionListBuilder = ImmutableList.builder();
        rexNodeSourceExpressionListBuilder.addAll(projects.subList(first, projects.size()));

        return LogicalTableModify.create(tableInfo.getTargetTables().get(0),
            catalogReader,
            sourceRel,
            LogicalTableModify.Operation.UPDATE,
            newTargetColumns,
            rexNodeSourceExpressionListBuilder.build(),
            false,
            SqlDmlKeyword.convertFromSqlNodeToString(call.getKeywords()),
            0,
            null,
            call.getHints(),
            call.getOptimizerHints(),
            TableInfo.create(tableInfo.getSrcNode(), tableInfo.getSrcInfos(), newTargetTables,
                tableInfo.getSourceColumnIndexMap(), tableInfo.getRefTableInfos()),
            extraTargetTables.stream().map(i -> srcTableInfoNodes.get(i).getRefTable()).collect(Collectors.toList()),
            extraTargetColumns);
    }

    protected RelNode transformUpdateSourceRel(RelNode old, TableInfo tableInfo, List<String> targetColumns,
                                               List<Map<String, Integer>> sourceColumnIndexMap,
                                               List<String> outTargetColumns, List<Integer> outTargetTables) {
        outTargetColumns.addAll(targetColumns);
        outTargetTables.addAll(tableInfo.getTargetTableIndexes());
        return old;
    }

    protected SqlSelect rewriteUpdateSourceSelect(SqlUpdate update, List<Integer> targetTableIndexes,
                                                  List<String> targetColumns, List<TableInfoNode> srcTables,
                                                  List<Integer> outExtraTargetTableIndexes,
                                                  List<String> outExtraTargetColumns) {
        return update.getSourceSelect();
    }

    private List<TableInfoNode> buildUpdateSourceInfo(SqlUpdate call, Map<String, Integer> outAliasTableMap,
                                                      Map<String, List<Integer>> outColumnTableMap) {
        final List<TableInfoNode> srcTableInfoNodes = getUpdateSrcTables(call);
        final SqlNodeList aliases = call.getAliases();
        final SqlNodeList tables = call.getSourceTables();
        for (Ord<SqlNode> o : Ord.zip(aliases)) {
            final SqlNode alias = o.e;
            final SqlNode table = tables.get(o.i);
            final String aliasString = SqlValidatorUtil.getAlias(alias, -1);

            if (!(table instanceof SqlSelect)) {
                // Ignore subquery
                final RelOptTable relOptTable = srcTableInfoNodes.get(o.i).getRefTable();
                outAliasTableMap.put(aliasString, o.i);
                for (RelDataTypeField field : relOptTable.getRowType().getFieldList()) {
                    if (!outColumnTableMap.containsKey(field.getName())) {
                        outColumnTableMap.put(field.getName(), new ArrayList<>());
                    }

                    outColumnTableMap.get(field.getName()).add(o.i);
                }
            }
        }
        return srcTableInfoNodes;
    }

    protected void buildUpdateTargetColumnInfo(List<SqlNode> targetColumns, List<TableInfoNode> tableInfoNodes,
                                               Map<String, Integer> aliasTableIndexMap,
                                               Map<String, List<Integer>> columnTableIndexMap,
                                               List<String> outTargetColumnNames, List<Integer> outTargetTableIndexes) {
        for (SqlNode node : targetColumns) {
            final SqlIdentifier id = (SqlIdentifier) node;
            if (id.names.size() == 2) {
                /*
                 * UPDATE t1 a , t2 b SET a.id = 1
                 */
                final Integer tableIndex = aliasTableIndexMap.get(id.names.get(0));
                if (null == tableIndex) {
                    throw new AssertionError("Unknown table name: " + id.names.get(0));
                }
                final RelOptTable relOptTable = tableInfoNodes.get(tableIndex).getRefTable();

                final RelDataType targetRowType = relOptTable.getRowType();
                final RelDataTypeField field = SqlValidatorUtil
                    .getTargetField(targetRowType, typeFactory, id, catalogReader, relOptTable);
                if (null == field) {
                    throw new AssertionError("Column '" + id.toString() + "' not found");
                }

                outTargetColumnNames.add(field.getName());
                outTargetTableIndexes.add(tableIndex);
            } else {
                /*
                 * UPDATE t1 a , t2 b SET id = 1
                 */
                final String columnName = Util.last(id.names);
                final List<Integer> tableIndexes = columnTableIndexMap.get(columnName);
                if (null == tableIndexes || tableIndexes.size() < 1) {
                    throw new AssertionError("Unknown column '" + columnName + "' in 'field list'");
                } else if (tableIndexes.size() > 1) {
                    throw new AssertionError("Column '" + columnName + "' in field list is ambiguous");
                }

                final Integer tableIndex = tableIndexes.get(0);
                final RelOptTable relOptTable = tableInfoNodes.get(tableIndex).getRefTable();
                final RelDataType targetRowType = relOptTable.getRowType();
                final RelDataTypeField field = SqlValidatorUtil
                    .getTargetField(targetRowType, typeFactory, id, catalogReader, relOptTable);
                if (null == field) {
                    throw new AssertionError("Column '" + id.toString() + "' not found");
                }

                outTargetColumnNames.add(field.getName());
                outTargetTableIndexes.add(tableIndex);
            }
        }
    }

    private List<RexNode> getProjects(RelNode sourceRel) {
        if (sourceRel instanceof Project) {
            return ((Project) sourceRel).getProjects();
        } else if (sourceRel instanceof Sort) {
            return getProjects(((Sort) sourceRel).getInput());
        }

        Assert.fail("Unsupported update source");
        return ImmutableList.of();
    }

    private RelNode convertMerge(SqlMerge call) {
        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final RelDataType targetRowType = targetTable.getRowType();
        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall != null) {
            for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
                SqlIdentifier id = (SqlIdentifier) targetColumn;
                RelDataTypeField field = SqlValidatorUtil
                    .getTargetField(targetRowType, typeFactory, id, catalogReader, targetTable);
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNameList.add(field.getName());
            }
        }

        // replace the projection of the source select with a
        // projection that contains the following:
        // 1) the expressions corresponding to the new insert row (if there is
        // an insert)
        // 2) all columns from the target table (if there is an update)
        // 3) the set expressions in the update call (if there is an update)

        // first, convert the merge's source select to construct the columns
        // from the target table and the set expressions in the update call
        RelNode mergeSourceRel = convertSelect(call.getSourceSelect(), false);

        // then, convert the insert statement so we can get the insert
        // values expressions
        SqlInsert insertCall = call.getInsertCall();
        int nLevel1Exprs = 0;
        List<RexNode> level1InsertExprs = null;
        List<RexNode> level2InsertExprs = null;
        if (insertCall != null) {
            RelNode insertRel = convertInsert(insertCall);

            // if there are 2 level of projections in the insert source, combine
            // them into a single project; level1 refers to the topmost project;
            // the level1 projection contains references to the level2
            // expressions, except in the case where no target expression was
            // provided, in which case, the expression is the default value for
            // the column; or if the expressions directly map to the source
            // table
            level1InsertExprs = ((LogicalProject) insertRel.getInput(0)).getProjects();
            if (insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
                level2InsertExprs = ((LogicalProject) insertRel.getInput(0).getInput(0)).getProjects();
            }
            nLevel1Exprs = level1InsertExprs.size();
        }

        LogicalJoin join = (LogicalJoin) mergeSourceRel.getInput(0);
        int nSourceFields = join.getLeft().getRowType().getFieldCount();
        final List<RexNode> projects = new ArrayList<>();
        for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
            if ((level2InsertExprs != null) && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
                int level2Idx = ((RexInputRef) level1InsertExprs.get(level1Idx)).getIndex();
                projects.add(level2InsertExprs.get(level2Idx));
            } else {
                projects.add(level1InsertExprs.get(level1Idx));
            }
        }
        if (updateCall != null) {
            final LogicalProject project = (LogicalProject) mergeSourceRel;
            projects.addAll(Util.skip(project.getProjects(), nSourceFields));
        }

        RelNode massagedRel = RelOptUtil.createProject(join, projects, null, true);

        return LogicalTableModify.create(targetTable,
            catalogReader,
            massagedRel,
            LogicalTableModify.Operation.MERGE,
            targetColumnNameList,
            null,
            false);
    }

    /**
     * Converts an identifier into an expression in a given scope. For example,
     * the "empno" in "select empno from emp join dept" becomes "emp.empno".
     */
    private RexNode convertIdentifier(Blackboard bb, SqlIdentifier identifier) {
        // first check for reserved identifiers like CURRENT_USER
        final SqlCall call = SqlUtil.makeCall(opTab, identifier);
        if (call != null) {
            if (bb.scope != null) {
                // Some keywords in sql should not be recognized as call, for example:
                // "user" in "show processlist where user is not null"
                final SqlIdentifier fqId = bb.scope.fullyQualify(identifier).identifier;
                if (!SpecialIdentiFiers.set.contains(fqId.toString().toUpperCase())) {
                    return bb.convertExpression(call);
                }
            } else {
                return bb.convertExpression(call);
            }
        }

        String pv = null;
        if (bb.isPatternVarRef && identifier.names.size() > 1) {
            pv = identifier.names.get(0);
        }

        final SqlQualified qualified;
        if (bb.scope != null) {
            qualified = bb.scope.fullyQualify(identifier);
        } else {
            qualified = SqlQualified.create(null, 1, null, identifier);
        }
        Pair<RexNode, Map<String, Integer>> e0 = bb.lookupExp(qualified);
        if (e0 == null) {
            throw new RuntimeException("Unresolved identifier " + identifier);
        }
        RexNode e = e0.left;

        final List<String> suffix = qualified.suffix();
        for (int i = 0; i < suffix.size(); i++) {
            String name = suffix.get(i);
            if (e == e0.left && e0.right != null) {
                final Integer integer = e0.right.get(name);
                int m = -1;
                if (integer == null) {
                    if (bb.scope instanceof ListScope) {
                        final SqlValidatorScope parent = ((ListScope) bb.scope).getParent();
                        e0 = bb.lookupExp(qualified, parent);
                        e = e0.left;
                        --i;
                        continue;
                    } else {
                        throw new NotSupportException("Field not in any table");
                    }
                } else {
                    i = integer;
                }
                e = rexBuilder.makeFieldAccess(e, i);
            } else {
                final boolean caseSensitive = true; // name already
                // fully-qualified
                if (identifier.isStar() && bb.scope instanceof MatchRecognizeScope) {
                    e = rexBuilder.makeFieldAccess(e, 0);
                } else {
                    e = rexBuilder.makeFieldAccess(e, name, caseSensitive);
                }
            }
        }
        if (e instanceof RexInputRef) {
            // adjust the type to account for nulls introduced by outer joins
            e = adjustInputRef(bb, (RexInputRef) e);
            if (pv != null) {
                e = RexPatternFieldRef.of(pv, (RexInputRef) e);
            }
        }

        if (e0.left instanceof RexCorrelVariable) {
            assert e instanceof RexFieldAccess;
            final RexNode prev = bb.mapCorrelateToRex.put(((RexCorrelVariable) e0.left).getId(), (RexFieldAccess) e);
            assert prev == null;
        }
        return e;
    }

    /**
     * Adjusts the type of a reference to an input field to account for nulls
     * introduced by outer joins; and adjusts the offset to match the physical
     * implementation.
     *
     * @param bb Blackboard
     * @param inputRef Input ref
     * @return Adjusted input ref
     */
    protected RexNode adjustInputRef(Blackboard bb, RexInputRef inputRef) {
        RelDataTypeField field = bb.getRootField(inputRef);
        if (field != null) {
            return rexBuilder.makeInputRef(field.getType(), inputRef.getIndex());
        }
        return inputRef;
    }

    /**
     * Converts a row constructor into a relational expression.
     *
     * @param bb Blackboard
     * @param rowConstructor Row constructor expression
     * @return Relational expression which returns a single row.
     */
    private RelNode convertRowConstructor(Blackboard bb, SqlCall rowConstructor) {
        Preconditions.checkArgument(isRowConstructor(rowConstructor));
        final List<SqlNode> operands = rowConstructor.getOperandList();
        return convertMultisets(operands, bb);
    }

    private RelNode convertCursor(Blackboard bb, SubQuery subQuery) {
        final SqlCall cursorCall = (SqlCall) subQuery.node;
        assert cursorCall.operandCount() == 1;
        SqlNode query = cursorCall.operand(0);
        RelNode converted = convertQuery(query, false, false).rel;
        int iCursor = bb.cursors.size();
        bb.cursors.add(converted);
        subQuery.expr = new RexInputRef(iCursor, converted.getRowType());
        return converted;
    }

    private RelNode convertMultisets(final List<SqlNode> operands, Blackboard bb) {
        // NOTE: Wael 2/04/05: this implementation is not the most efficient in
        // terms of planning since it generates XOs that can be reduced.
        final List<Object> joinList = new ArrayList<>();
        List<SqlNode> lastList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            SqlNode operand = operands.get(i);
            if (!(operand instanceof SqlCall)) {
                lastList.add(operand);
                continue;
            }

            final SqlCall call = (SqlCall) operand;
            final RelNode input;
            switch (call.getKind()) {
            case MULTISET_VALUE_CONSTRUCTOR:
            case ARRAY_VALUE_CONSTRUCTOR:
                final SqlNodeList list = new SqlNodeList(call.getOperandList(), call.getParserPosition());
                CollectNamespace nss = (CollectNamespace) validator.getNamespace(call);
                Blackboard usedBb;
                if (null != nss) {
                    usedBb = createBlackboard(nss.getScope(), null, false);
                } else {
                    usedBb = createBlackboard(new ListScope(bb.scope) {

                        public SqlNode getNode() {
                            return call;
                        }
                    }, null, false);
                }
                RelDataType multisetType = validator.getValidatedNodeType(call);
                ((SqlValidatorImpl) validator).setValidatedNodeType(list, multisetType.getComponentType());
                input = convertQueryOrInList(usedBb, list, null);
                break;
            case MULTISET_QUERY_CONSTRUCTOR:
            case ARRAY_QUERY_CONSTRUCTOR:
                final RelRoot root = convertQuery(call.operand(0), false, true);
                input = root.rel;
                break;
            default:
                lastList.add(operand);
                continue;
            }

            if (lastList.size() > 0) {
                joinList.add(lastList);
            }
            lastList = new ArrayList<>();
            Collect collect = new Collect(cluster,
                cluster.traitSetOf(Convention.NONE),
                input,
                validator.deriveAlias(call, i));
            joinList.add(collect);
        }

        if (joinList.size() == 0) {
            joinList.add(lastList);
        }

        for (int i = 0; i < joinList.size(); i++) {
            Object o = joinList.get(i);
            if (o instanceof List) {
                @SuppressWarnings("unchecked")
                List<SqlNode> projectList = (List<SqlNode>) o;
                final List<RexNode> selectList = new ArrayList<>();
                final List<String> fieldNameList = new ArrayList<>();
                for (int j = 0; j < projectList.size(); j++) {
                    SqlNode operand = projectList.get(j);
                    selectList.add(bb.convertExpression(operand));

                    // REVIEW angel 5-June-2005: Use deriveAliasFromOrdinal
                    // instead of deriveAlias to match field names from
                    // SqlRowOperator. Otherwise, get error Type
                    // 'RecordType(INTEGER EMPNO)' has no field 'EXPR$0' when
                    // doing select * from unnest( select multiset[empno]
                    // from sales.emps);

                    fieldNameList.add(SqlUtil.deriveAliasFromOrdinal(j));
                }

                RelNode projRel = RelOptUtil
                    .createProject(LogicalValues.createOneRow(cluster), selectList, fieldNameList);

                joinList.set(i, projRel);
            }
        }

        RelNode ret = (RelNode) joinList.get(0);
        for (int i = 1; i < joinList.size(); i++) {
            RelNode relNode = (RelNode) joinList.get(i);
            ret = RelFactories.DEFAULT_JOIN_FACTORY.createJoin(ret,
                relNode,
                rexBuilder.makeLiteral(true),
                ImmutableSet.<CorrelationId>of(),
                JoinRelType.INNER,
                false);
        }
        return ret;
    }

    private void convertSelectList(Blackboard bb, SqlSelect select, List<SqlNode> orderList) {
        SqlNodeList selectList = select.getSelectList();
        selectList = validator.expandStar(selectList, select, false);

        replaceSubQueries(bb, selectList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        List<String> fieldNames = new ArrayList<>();
        List<String> newOrderByNameList = select.isDistinct() ? new ArrayList<>() : null; // Used in distinctify.
        List<String> originalNames = new ArrayList<>();
        final List<RexNode> exprs = new ArrayList<>();
        final Collection<String> aliases = new TreeSet<>();

        // Project any system fields. (Must be done before regular select items,
        // because offsets may be affected.)
        final List<SqlMonotonicity> columnMonotonicityList = new ArrayList<>();
        extraSelectItems(bb, select, exprs, fieldNames, aliases, columnMonotonicityList);
        // Project select clause.
        int i = -1;
        for (SqlNode expr : selectList) {
            ++i;
            exprs.add(bb.convertExpression(expr));
            fieldNames.add(deriveAlias(expr, aliases, i));
            originalNames.add(deriveOriginalAlias(expr, i));
        }

        SqlNodeList newOrderByList = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode order : orderList) {
            boolean found = false;
            for (SqlNode g : selectList.getList()) {
                if (order.toString().equals(g.toString())) {
                    found = true;
                }
            }

            if (!found) {
                newOrderByList.add(order);
            }
        }

        // Project extra fields for sorting.
        for (SqlNode expr : newOrderByList) {
            ++i;
            SqlNode expr2 = validator.expandOrderExpr(select, expr);
            exprs.add(bb.convertExpression(expr2));
            String fieldName = deriveAlias(expr, aliases, i);
            if (newOrderByNameList != null) {
                newOrderByNameList.add(fieldName);
            }
            fieldNames.add(fieldName);
            originalNames.add(deriveOriginalAlias(expr, i));
        }

        fieldNames = SqlValidatorUtil.uniquify(fieldNames, catalogReader.nameMatcher().isCaseSensitive());

        RelNode project = RelOptUtil.createProject(bb.root, exprs, fieldNames, originalNames);
        CorrelationUse correlationUse = null;
        try {
            correlationUse = getCorrelationUse(bb, project, null, null, null);
        } catch (ValidationFirstValueException e) {
            throw new AssertionError(e.getMessage());
        }

        if (correlationUse == null) {
            bb.setRoot(LogicalProject.create(bb.root, exprs, fieldNames, originalNames), false);
        } else {
            LogicalProject logicalProject = (LogicalProject) correlationUse.r;
            bb.setRoot(LogicalProject
                .create(logicalProject.getInput(), logicalProject.getProjects(), fieldNames, originalNames,
                    ImmutableSet.<CorrelationId>of(correlationUse.id)), false);
        }

        assert bb.columnMonotonicities.isEmpty();
        bb.columnMonotonicities.addAll(columnMonotonicityList);
        for (SqlNode selectItem : selectList) {
            bb.columnMonotonicities.add(selectItem.getMonotonicity(bb.scope));
        }
        if (select.isDistinct()) {
            distinctify(bb, newOrderByList.getList(), newOrderByNameList, true);
        }
    }

    /**
     * Adds extra select items. The default implementation adds nothing; derived
     * classes may add columns to exprList, nameList, aliasList and
     * columnMonotonicityList.
     *
     * @param bb Blackboard
     * @param select Select statement being translated
     * @param exprList List of expressions in select clause
     * @param nameList List of names, one per column
     * @param aliasList Collection of aliases that have been used already
     * @param columnMonotonicityList List of monotonicity, one per column
     */
    protected void extraSelectItems(Blackboard bb, SqlSelect select, List<RexNode> exprList, List<String> nameList,
                                    Collection<String> aliasList, List<SqlMonotonicity> columnMonotonicityList) {
    }

    protected String deriveAlias(final SqlNode node, Collection<String> aliases, final int ordinal) {
        String alias = validator.deriveAlias(node, ordinal);
        if ((alias == null) || aliases.contains(alias)) {
            String aliasBase = (alias == null) ? "EXPR$" : alias;
            for (int j = 0; ; j++) {
                alias = aliasBase + j;
                if (!aliases.contains(alias)) {
                    break;
                }
            }
        }
        aliases.add(alias);
        return alias;
    }

    protected String deriveOriginalAlias(final SqlNode node, final int ordinal) {
        String alias = validator.deriveAlias(node, ordinal);
        if (alias == null) {
            String aliasBase = (alias == null) ? "EXPR$" : alias;
            alias = aliasBase + ordinal;
        }
        return alias;
    }

    /**
     * Converts a WITH sub-query into a relational expression.
     */
    public RelRoot convertWith(SqlWith with, boolean top) {
        return convertQuery(with.body, false, top);
    }

    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public RelNode convertValues(SqlCall values, RelDataType targetRowType) {
        final SqlValidatorScope scope = validator.getOverScope(values);
        assert scope != null;
        final Blackboard bb = createBlackboard(scope, null, false);
        convertValuesImpl(bb, values, targetRowType);
        return bb.root;
    }

    /**
     * Converts a values clause (as in "INSERT INTO T(x,y) VALUES (1,2)") into a
     * relational expression.
     *
     * @param bb Blackboard
     * @param values Call to SQL VALUES operator
     * @param targetRowType Target row type
     */
    private void convertValuesImpl(Blackboard bb, SqlCall values, RelDataType targetRowType) {
        // Attempt direct conversion to LogicalValues; if that fails, deal with
        // fancy stuff like sub-queries below.
        boolean allIsDynamic = values.getOperandList().stream().anyMatch(t -> t instanceof SqlDynamicParam);
        RelNode valuesRel;
        if (allIsDynamic) {
            valuesRel = convertDynamicRowValues(bb, values, values.getOperandList(), true, targetRowType);
        } else {
            valuesRel = convertRowValues(bb, values, values.getOperandList(), true, targetRowType);
        }

        if (valuesRel != null) {
            bb.setRoot(valuesRel, true);
            return;
        }

        final List<RelNode> unionRels = new ArrayList<>();
        for (SqlNode rowConstructor1 : values.getOperandList()) {
            SqlCall rowConstructor = (SqlCall) rowConstructor1;
            Blackboard tmpBb = createBlackboard(bb.scope, null, false, bb);
            replaceSubQueries(tmpBb, rowConstructor, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);
            final List<Pair<RexNode, String>> exps = new ArrayList<>();
            // Field names are in targetRowType. Using operand will lose field
            // names.
            // for (Ord<SqlNode> operand :
            // Ord.zip(rowConstructor.getOperandList())) {
            // exps.add(
            // Pair.of(
            // tmpBb.convertExpression(operand.e),
            // validator.deriveAlias(operand.e, operand.i)));
            // }
            for (int i = 0; i < rowConstructor.getOperandList().size(); i++) {
                SqlNode operand = rowConstructor.getOperandList().get(i);
                List<RelDataTypeField> fieldList = targetRowType.getFieldList();
                exps.add(Pair.of(tmpBb.convertExpression(operand), fieldList.get(i).getName()));
            }
            RelNode in = (null == tmpBb.root) ? LogicalValues.createOneRow(cluster) : tmpBb.root;
            unionRels.add(RelOptUtil.createProject(in, Pair.left(exps), Pair.right(exps), true));
        }

        if (unionRels.size() == 0) {
            throw new AssertionError("empty values clause");
        } else if (unionRels.size() == 1) {
            bb.setRoot(unionRels.get(0), true);
        } else {
            bb.setRoot(LogicalUnion.create(unionRels, true), true);
        }

        // REVIEW jvs 22-Jan-2004: should I add
        // mapScopeToLux.put(validator.getScope(values),bb.root);
        // ?
    }

    // ~ Inner Classes
    // ----------------------------------------------------------

    /**
     * Workspace for translating an individual SELECT statement (or sub-SELECT).
     */
    protected class Blackboard implements SqlRexContext, SqlVisitor<RexNode>, InitializerContext {

        /**
         * Collection of {@link RelNode} objects which correspond to a SELECT
         * statement.
         */
        public final SqlValidatorScope scope;
        private final Map<String, RexNode> nameToNodeMap;
        public RelNode root;
        private List<RelNode> inputs;
        private final Map<CorrelationId, RexFieldAccess> mapCorrelateToRex = new HashMap<>();

        private boolean isPatternVarRef = false;

        final List<RelNode> cursors = new ArrayList<>();

        /**
         * List of <code>IN</code> and <code>EXISTS</code> nodes inside this
         * <code>SELECT</code> statement (but not inside sub-queries).
         */
        private final Set<SubQuery> subQueryList = new LinkedHashSet<>();

        /**
         * Workspace for building aggregates.
         */
        AggConverter agg;

        /**
         * When converting window aggregate, we need to know if the window is
         * guaranteed to be non-empty.
         */
        SqlWindow window;

        /**
         * Project the groupby expressions out of the root of this sub-select.
         * Sub-queries can reference group by expressions projected from the
         * "right" to the sub-query.
         */
        private final Map<RelNode, Map<Integer, Integer>> mapRootRelToFieldProjection = new HashMap<>();

        private final List<SqlMonotonicity> columnMonotonicities = new ArrayList<>();

        private final List<RelDataTypeField> systemFieldList = new ArrayList<>();
        final boolean top;

        private final InitializerExpressionFactory initializerExpressionFactory =
            new NullInitializerExpressionFactory();

        private final Deque<SqlNode> queryBlockName;

        public Map<String, SqlNodeList> getDbjHintName() {
            return dbjHintName;
        }

        //fore dbj hints
        private final Map<String, SqlNodeList> dbjHintName;

        private boolean supportInvalueSemiJoin = true;

        private boolean ignoreConvertExtendedExpression = false;

        /**
         * Creates a Blackboard.
         *
         * @param scope Name-resolution scope for expressions validated within
         * this query. Can be null if this Blackboard is for a leaf node, say
         * @param nameToNodeMap Map which translates the expression to map a
         * given parameter into, if translating expressions; null otherwise
         * @param top Whether this is the root of the query
         */
        protected Blackboard(SqlValidatorScope scope, Map<String, RexNode> nameToNodeMap, boolean top) {
            this.scope = scope;
            this.nameToNodeMap = nameToNodeMap;
            this.top = top;
            this.queryBlockName = new ArrayDeque<>();
            this.dbjHintName = new HashMap<>();
        }

        protected Blackboard(SqlValidatorScope scope, Map<String, RexNode> nameToNodeMap, boolean top,
                             Blackboard current) {
            this.scope = scope;
            this.nameToNodeMap = nameToNodeMap;
            this.top = top;
            this.queryBlockName = current.queryBlockName;
            this.dbjHintName = current.dbjHintName;
        }

        public void setPatternVarRef(boolean isVarRef) {
            this.isPatternVarRef = isVarRef;
        }

        public RexNode register(RelNode rel, JoinRelType joinType) {
            return register(rel, joinType, null);
        }

        /**
         * Registers a relational expression.
         *
         * @param rel Relational expression
         * @param joinType Join type
         * @param leftKeys LHS of IN clause, or null for expressions other than
         * IN
         * @return Expression with which to refer to the row (or partial row)
         * coming from this relational expression's side of the join
         */
        public RexNode register(RelNode rel, JoinRelType joinType, List<RexNode> leftKeys) {
            assert joinType != null;
            if (root == null) {
                assert leftKeys == null;
                setRoot(rel, false);
                return rexBuilder.makeRangeReference(root.getRowType(), 0, false);
            }

            final RexNode joinCond;
            final int origLeftInputCount = root.getRowType().getFieldCount();
            if (leftKeys != null) {
                List<RexNode> newLeftInputExpr = Lists.newArrayList();
                for (int i = 0; i < origLeftInputCount; i++) {
                    newLeftInputExpr.add(rexBuilder.makeInputRef(root, i));
                }

                final List<Integer> leftJoinKeys = Lists.newArrayList();
                for (RexNode leftKey : leftKeys) {
                    int index = newLeftInputExpr.indexOf(leftKey);
                    if (index < 0 || joinType == JoinRelType.LEFT) {
                        index = newLeftInputExpr.size();
                        newLeftInputExpr.add(leftKey);
                    }
                    leftJoinKeys.add(index);
                }

                RelNode newLeftInput = RelOptUtil.createProject(root, newLeftInputExpr, null, true);

                // maintain the group by mapping in the new LogicalProject
                if (mapRootRelToFieldProjection.containsKey(root)) {
                    mapRootRelToFieldProjection.put(newLeftInput, mapRootRelToFieldProjection.get(root));
                }

                setRoot(newLeftInput, false);

                // right fields appear after the LHS fields.
                final int rightOffset = root.getRowType().getFieldCount() - newLeftInput.getRowType().getFieldCount();
                final List<Integer> rightKeys = Util.range(rightOffset, rightOffset + leftKeys.size());

                joinCond = RelOptUtil.createEquiJoinCondition(newLeftInput, leftJoinKeys, rel, rightKeys, rexBuilder);
            } else {
                joinCond = rexBuilder.makeLiteral(true);
            }

            int leftFieldCount = root.getRowType().getFieldCount();
            final RelNode join = createJoin(this, root, rel, joinCond, joinType, new SqlNodeList(SqlParserPos.ZERO));

            setRoot(join, false);

            if (leftKeys != null && joinType == JoinRelType.LEFT) {
                final int leftKeyCount = leftKeys.size();
                int rightFieldLength = rel.getRowType().getFieldCount();
                assert leftKeyCount == rightFieldLength - 1;

                final int rexRangeRefLength = leftKeyCount + rightFieldLength;
                RelDataType returnType = typeFactory
                    .createStructType(new AbstractList<Map.Entry<String, RelDataType>>() {

                        public Map.Entry<String, RelDataType> get(int index) {
                            return join.getRowType().getFieldList().get(origLeftInputCount + index);
                        }

                        public int size() {
                            return rexRangeRefLength;
                        }
                    });

                return rexBuilder.makeRangeReference(returnType, origLeftInputCount, false);
            } else {
                return rexBuilder
                    .makeRangeReference(rel.getRowType(), leftFieldCount, joinType.generatesNullsOnRight());
            }
        }

        /**
         * Sets a new root relational expression, as the translation process
         * backs its way further up the tree.
         *
         * @param root New root relational expression
         * @param leaf Whether the relational expression is a leaf, that is,
         * derived from an atomic relational expression such as a table name in
         * the from clause, or the projection on top of a select-sub-query. In
         * particular, relational expressions derived from JOIN operators are
         * not leaves, but set expressions are.
         */
        public void setRoot(RelNode root, boolean leaf) {
            setRoot(Collections.singletonList(root), root, root instanceof LogicalJoin);
            if (leaf) {
                leaves.add(root);
            }
            this.columnMonotonicities.clear();
        }

        private void setRoot(List<RelNode> inputs, RelNode root, boolean hasSystemFields) {
            updateQbName(root);
            this.inputs = inputs;
            this.root = root;
            this.systemFieldList.clear();
            if (hasSystemFields) {
                this.systemFieldList.addAll(getSystemFields());
            }
        }

        /**
         * Notifies this Blackboard that the root just set using
         * {@link #setRoot(RelNode, boolean)} was derived using dataset
         * substitution.
         * <p>
         * The default implementation is not interested in such notifications,
         * and does nothing.
         *
         * @param datasetName Dataset name
         */
        public void setDataset(String datasetName) {
        }

        void setRoot(List<RelNode> inputs) {
            setRoot(inputs, null, false);
        }

        /**
         * Returns an expression with which to reference a from-list item.
         *
         * @param qualified the alias of the from item
         * @return a {@link RexFieldAccess} or {@link RexRangeRef}, or null if
         * not found
         */
        Pair<RexNode, Map<String, Integer>> lookupExp(SqlQualified qualified) {
            return lookupExp(qualified, null);
        }

        /**
         * Returns an expression with which to reference a from-list item.
         *
         * @param qualified the alias of the from item
         * @return a {@link RexFieldAccess} or {@link RexRangeRef}, or null if
         * not found
         */
        Pair<RexNode, Map<String, Integer>> lookupExp(SqlQualified qualified, SqlValidatorScope lookupScope) {
            if (nameToNodeMap != null && qualified.prefixLength == 1) {
                RexNode node = null;
                String columnName = qualified.identifier.names.get(0);
                for (Map.Entry<String, RexNode> entry : nameToNodeMap.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(columnName)) {
                        node = entry.getValue();
                        break;
                    }
                }
                if (node == null) {
                    throw new AssertionError(
                        "Unknown identifier '" + qualified.identifier + "' encountered while expanding expression");
                }
                return Pair.of(node, null);
            }
            final SqlNameMatcher nameMatcher = scope.getValidator().getCatalogReader().nameMatcher();
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            if (lookupScope != null) {
                lookupScope.resolve(qualified.prefix(), nameMatcher, false, resolved);
            } else {
                scope.resolve(qualified.prefix(), nameMatcher, false, resolved);
            }
            if (!(resolved.count() == 1)) {
                return null;
            }
            final SqlValidatorScope.Resolve resolve = resolved.only();
            final RelDataType rowType = resolve.rowType();

            // Found in current query's from list. Find which from item.
            // We assume that the order of the from clause items has been
            // preserved.
            final SqlValidatorScope ancestorScope = resolve.scope;
            boolean isParent = ancestorScope != scope;
            if ((inputs != null) && !isParent) {
                final LookupContext rels = new LookupContext(this, inputs, systemFieldList.size());
                final RexNode node = lookup(resolve.path.steps().get(0).i, rels);
                if (node == null) {
                    return null;
                } else {
                    final Map<String, Integer> fieldOffsets = new HashMap<>();
                    for (RelDataTypeField f : resolve.rowType().getFieldList()) {
                        if (!fieldOffsets.containsKey(f.getName())) {
                            fieldOffsets.put(f.getName(), f.getIndex());
                        }
                    }
                    final Map<String, Integer> map = ImmutableMap.copyOf(fieldOffsets);
                    return Pair.of(node, map);
                }
            } else {
                // We're referencing a relational expression which has not been
                // converted yet. This occurs when from items are correlated,
                // e.g. "select from emp as emp join emp.getDepts() as dept".
                // Create a temporary expression.
                DeferredLookup lookup = new DeferredLookup(this, qualified.identifier.names.get(0));
                final CorrelationId correlId = cluster.createCorrel();
                mapCorrelToDeferred.put(correlId, lookup);
                if (resolve.path.steps().get(0).i < 0) {
                    return Pair.of(rexBuilder.makeCorrel(rowType, correlId), null);
                } else {
                    final RelDataTypeFactory.Builder builder = typeFactory.builder();
                    final ListScope ancestorScope1 = (ListScope) resolve.scope;
                    final ImmutableMap.Builder<String, Integer> fields = ImmutableMap.builder();
                    int i = 0;
                    int offset = 0;
                    for (SqlValidatorNamespace c : ancestorScope1.getChildren()) {
                        builder.addAll(c.getRowType().getFieldList());
                        if (i == resolve.path.steps().get(0).i) {
                            for (RelDataTypeField field : c.getRowType().getFieldList()) {
                                fields.put(field.getName(), field.getIndex() + offset);
                            }
                        }
                        ++i;
                        offset += c.getRowType().getFieldCount();
                    }
                    final RexNode c = rexBuilder.makeCorrel(builder.uniquify().build(), correlId);
                    return Pair.<RexNode, Map<String, Integer>>of(c, fields.build());
                }
            }
        }

        /**
         * Creates an expression with which to reference the expression whose
         * offset in its from-list is {@code offset}.
         */
        RexNode lookup(int offset, LookupContext lookupContext) {
            Pair<RelNode, Integer> pair = lookupContext.findRel(offset);
            return rexBuilder.makeRangeReference(pair.left.getRowType(), pair.right, false);
        }

        RelDataTypeField getRootField(RexInputRef inputRef) {
            if (inputs == null) {
                return null;
            }
            int fieldOffset = inputRef.getIndex();
            for (RelNode input : inputs) {
                RelDataType rowType = input.getRowType();
                if (rowType == null) {
                    // TODO: remove this once leastRestrictive
                    // is correctly implemented
                    return null;
                }
                if (fieldOffset < rowType.getFieldCount()) {
                    return rowType.getFieldList().get(fieldOffset);
                }
                fieldOffset -= rowType.getFieldCount();
            }
            throw new AssertionError();
        }

        public void flatten(List<RelNode> rels, int systemFieldCount, int[] start,
                            List<Pair<RelNode, Integer>> relOffsetList) {
            for (RelNode rel : rels) {
                if (leaves.contains(rel) || rel instanceof LogicalMatch) {
                    relOffsetList.add(Pair.of(rel, start[0]));
                    start[0] += rel.getRowType().getFieldCount();
                } else {
                    if (rel instanceof LogicalJoin || rel instanceof LogicalAggregate) {
                        start[0] += systemFieldCount;
                    }
                    flatten(rel.getInputs(), systemFieldCount, start, relOffsetList);
                }
            }
        }

        void registerSubQuery(SqlNode node, RelOptUtil.Logic logic) {
            for (SubQuery subQuery : subQueryList) {
                if (node.equalsDeep(subQuery.node, Litmus.IGNORE)) {
                    return;
                }
            }
            subQueryList.add(new SubQuery(node, logic));
        }

        SubQuery getSubQuery(SqlNode expr) {
            for (SubQuery subQuery : subQueryList) {
                if (expr.equalsDeep(subQuery.node, Litmus.IGNORE)) {
                    return subQuery;
                }
            }

            return null;
        }

        ImmutableList<RelNode> retrieveCursors() {
            try {
                return ImmutableList.copyOf(cursors);
            } finally {
                cursors.clear();
            }
        }

        public boolean isSupportInvalueSemiJoin() {
            return supportInvalueSemiJoin;
        }

        public void setSupportInvalueSemiJoin(boolean supportInvalueSemiJoin) {
            this.supportInvalueSemiJoin = supportInvalueSemiJoin;
        }

        public boolean isIgnoreConvertExtendedExpression() {
            return ignoreConvertExtendedExpression;
        }

        public void setIgnoreConvertExtendedExpression(boolean ignoreConvertExtendedExpression) {
            this.ignoreConvertExtendedExpression = ignoreConvertExtendedExpression;
        }

        public RexNode convertExpression(SqlNode expr) {
            // If we're in aggregation mode and this is an expression in the
            // GROUP BY clause, return a reference to the field.
            if (agg != null) {
                final SqlNode expandedGroupExpr = validator.expand(expr, scope);
                final int ref = agg.lookupGroupExpr(expandedGroupExpr);
                if (ref >= 0) {
                    return rexBuilder.makeInputRef(root, ref);
                }
                if (expr instanceof SqlCall) {
                    final RexNode rex = agg.lookupAggregates((SqlCall) expr);
                    if (rex != null) {
                        return rex;
                    }
                }
            }

            // Allow the derived class chance to override the standard
            // behavior for special kinds of expressions.
            RexNode rex = null;
            if (!ignoreConvertExtendedExpression) {
                rex = convertExtendedExpression(expr, this);
                if (rex != null) {
                    return rex;
                }
            }

            // Sub-queries and OVER expressions are not like ordinary
            // expressions.
            final SqlKind kind = expr.getKind();
            final SubQuery subQuery;
            if (!config.isExpand()) {
                final SqlCall call;
                final SqlNode query;
                final RelRoot root;
                switch (kind) {
                case IN:
                case NOT_IN:
                case SOME:
                case ALL:
                    call = (SqlCall) expr;
                    query = call.operand(1);
                    if (!(query instanceof SqlNodeList)) {
                        root = convertQueryRecursive(query, false, null);
                        final SqlNode operand = call.operand(0);
                        List<SqlNode> nodes;
                        switch (operand.getKind()) {
                        case ROW:
                            nodes = ((SqlCall) operand).getOperandList();
                            break;
                        default:
                            nodes = ImmutableList.of(operand);
                        }
                        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                        for (SqlNode node : nodes) {
                            builder.add(convertExpression(node));
                        }
                        final ImmutableList<RexNode> list = builder.build();
                        switch (kind) {
                        case IN:
                            return RexSubQuery.in(root.rel, list);
                        case NOT_IN:
                            return RexSubQuery.not_in(root.rel, list);
                        case SOME:
                            return RexSubQuery.some(root.rel, list, (SqlQuantifyOperator) call.getOperator());
                        case ALL:
                            return
                                // rexBuilder.makeCall(SqlStdOperatorTable.NOT,RexSubQuery.some(root.rel,
                                // list,
                                // negate((SqlQuantifyOperator)
                                // call.getOperator())));
                                RexSubQuery.all(root.rel, list, (SqlQuantifyOperator) call.getOperator());
                        default:
                            throw new AssertionError(kind);
                        }
                    }
                    break;

                case EXISTS:
                    call = (SqlCall) expr;
                    query = Iterables.getOnlyElement(call.getOperandList());
                    root = convertQueryRecursive(query, false, null);
                    RelNode rel = root.rel;
                    while (rel instanceof Project
                        || rel instanceof Sort && ((Sort) rel).fetch == null && ((Sort) rel).offset == null) {
                        rel = ((SingleRel) rel).getInput();
                    }
                    return RexSubQuery.exists(rel);
                case NOT_EXISTS:
                    call = (SqlCall) expr;
                    query = Iterables.getOnlyElement(call.getOperandList());
                    root = convertQueryRecursive(query, false, null);
                    RelNode relNotexist = root.rel;
                    while (relNotexist instanceof Project
                        || relNotexist instanceof Sort && ((Sort) relNotexist).fetch == null
                        && ((Sort) relNotexist).offset == null) {
                        relNotexist = ((SingleRel) relNotexist).getInput();
                    }
                    return RexSubQuery.not_exists(relNotexist);
                case SCALAR_QUERY:
                    call = (SqlCall) expr;
                    query = Iterables.getOnlyElement(call.getOperandList());
                    root = convertQueryRecursive(query, false, null);
                    return RexSubQuery.scalar(root.rel);
                }
            }

            switch (kind) {
            case SOME:
            case ALL:
                if (config.isExpand()) {
                    throw new RuntimeException(kind + " is only supported if expand = false");
                }
                // fall through
            case CURSOR:
            case IN:
            case NOT_IN:
                subQuery = Preconditions.checkNotNull(getSubQuery(expr));
                if (subQuery.expr == null) {
                    rex = convertExtendedExpression(expr, this);
                    if (rex != null) {
                        return rex;
                    }
                }
                rex = Preconditions.checkNotNull(subQuery.expr);
                return rex;

            case SELECT:
            case EXISTS:
            case SCALAR_QUERY:
                subQuery = getSubQuery(expr);
                assert subQuery != null;
                rex = subQuery.expr;
                assert rex != null : "rex != null";

                if (((kind == SqlKind.SCALAR_QUERY) || (kind == SqlKind.EXISTS)) && isConvertedSubq(rex)) {
                    // scalar sub-query or EXISTS has been converted to a
                    // constant
                    return rex;
                }

                // The indicator column is the last field of the sub-query.
                RexNode fieldAccess = rexBuilder.makeFieldAccess(rex, rex.getType().getFieldCount() - 1);

                // The indicator column will be nullable if it comes from
                // the null-generating side of the join. For EXISTS, add an
                // "IS TRUE" check so that the result is "BOOLEAN NOT NULL".
                if (fieldAccess.getType().isNullable() && kind == SqlKind.EXISTS) {
                    fieldAccess = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldAccess);
                }
                return fieldAccess;
            //case ROW:

            case OVER:
                return convertOver(this, expr);
            default:
                // fall through
            }

            // Apply standard conversions.
            rex = expr.accept(this);
            if (rex == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Can't convert expression " + expr);
            }
            return rex;
        }

        /**
         * Converts an item in an ORDER BY clause, extracting DESC, NULLS LAST
         * and NULLS FIRST flags first.
         */
        public RexNode convertSortExpression(SqlNode expr, Set<SqlKind> flags) {
            switch (expr.getKind()) {
            case DESCENDING:
            case NULLS_LAST:
            case NULLS_FIRST:
                flags.add(expr.getKind());
                final SqlNode operand = ((SqlCall) expr).operand(0);
                return convertSortExpression(operand, flags);
            default:
                return convertExpression(expr);
            }
        }

        /**
         * Converts an item in an ORDER BY clause, extracting DESC, NULLS LAST
         * and NULLS FIRST flags first.
         */
        public RexNode convertWindowSortExpression(SqlNode expr, Set<SqlKind> flags) {
            SqlNode operand = null;
            switch (expr.getKind()) {
            case DESCENDING:
                flags.add(expr.getKind());
                flags.add(SqlKind.NULLS_LAST);
                operand = ((SqlCall) expr).operand(0);
                return convertSortExpression(operand, flags);
            case ASCENDING:
                flags.add(expr.getKind());
                flags.add(SqlKind.NULLS_FIRST);
                operand = ((SqlCall) expr).operand(0);
                return convertSortExpression(operand, flags);
            case NULLS_LAST:
            case NULLS_FIRST:
                if (!flags.contains(expr.getKind())) {
                    throw new UnsupportedOperationException("ASCENDING/DESCENDING must be nulls first/last.");
                }
                if (!flags.contains(expr.getKind())) {
                    return convertSortExpression(operand, flags);
                }
                flags.add(expr.getKind());
                operand = ((SqlCall) expr).operand(0);
                return convertSortExpression(operand, flags);
            default:
                if (!flags.contains(SqlKind.DESCENDING)) {
                    flags.add(SqlKind.NULLS_FIRST);
                }
                return convertExpression(expr);
            }
        }

        /**
         * Determines whether a RexNode corresponds to a sub-query that's been
         * converted to a constant.
         *
         * @param rex the expression to be examined
         * @return true if the expression is a dynamic parameter, a literal, or
         * a literal that is being cast
         */
        private boolean isConvertedSubq(RexNode rex) {
            if ((rex instanceof RexLiteral) || (rex instanceof RexDynamicParam)) {
                return true;
            }
            if (rex instanceof RexCall) {
                RexCall call = (RexCall) rex;
                if (call.getOperator() == SqlStdOperatorTable.CAST) {
                    RexNode operand = call.getOperands().get(0);
                    if (operand instanceof RexLiteral) {
                        return true;
                    }
                }
            }
            return false;
        }

        public int getGroupCount() {
            if (agg != null) {
                return agg.groupExprs.size();
            }
            if (window != null) {
                return window.isAlwaysNonEmpty() ? 1 : 0;
            }
            return -1;
        }

        public RexBuilder getRexBuilder() {
            return rexBuilder;
        }

        public RexRangeRef getSubQueryExpr(SqlCall call) {
            final SubQuery subQuery = getSubQuery(call);
            assert subQuery != null;
            return (RexRangeRef) subQuery.expr;
        }

        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }

        public InitializerExpressionFactory getInitializerExpressionFactory() {
            return initializerExpressionFactory;
        }

        public SqlValidator getValidator() {
            return validator;
        }

        public RexNode convertLiteral(SqlLiteral literal) {
            return exprConverter.convertLiteral(this, literal);
        }

        public RexNode convertInterval(SqlIntervalQualifier intervalQualifier) {
            return exprConverter.convertInterval(this, intervalQualifier);
        }

        public RexNode visit(SqlLiteral literal) {
            return exprConverter.convertLiteral(this, literal);
        }

        public RexNode visit(SqlCall call) {
            if (agg != null) {
                final SqlOperator op = call.getOperator();
                if (window == null && (op.isAggregator() || op.getKind() == SqlKind.FILTER)) {
                    return agg.lookupAggregates(call);
                }
            }
            return exprConverter.convertCall(this, new SqlCallBinding(validator, scope, call).permutedCall());
        }

        public RexNode visit(SqlNodeList nodeList) {
            throw new UnsupportedOperationException();
        }

        public RexNode visit(SqlIdentifier id) {
            return convertIdentifier(this, id);
        }

        public RexNode visit(SqlDataTypeSpec type) {
            throw new UnsupportedOperationException();
        }

        public RexNode visit(SqlDynamicParam param) {
            return convertDynamicParam(param);
        }

        public RexNode visit(SqlIntervalQualifier intervalQualifier) {
            return convertInterval(intervalQualifier);
        }

        public List<SqlMonotonicity> getColumnMonotonicities() {
            return columnMonotonicities;
        }

        private class UpdateQbNameBean {
            boolean noQbName = true;
            boolean noPushdown = true;
            SqlBasicCall pushdown = null;
        }

        public void updateQbName(RelNode root) {
            if (null != root && null != currentQbName()) {
                updateQbName(root, currentQbName());
            }
        }

        public void updateQbName(RelNode root, SqlNode qbName) {
            final SqlNodeList currentHints = root.getHints();

            SqlNodeList updatedHints = currentHints;
            if (null == currentHints || currentHints.size() == 0) {
                updatedHints = new SqlNodeList(ImmutableList.of(qbName), SqlParserPos.ZERO);
            } else {
                final UpdateQbNameBean qbNameInfo = new UpdateQbNameBean();

                queryQbName(currentHints, new QbNameHandler() {

                    @Override
                    public Object handleQbName(SqlBasicCall qbNameHint) {
                        qbNameInfo.noQbName = false;
                        return null;
                    }

                    @Override
                    public Object handlePushdown(SqlBasicCall pushdownHint) {
                        qbNameInfo.noPushdown = false;
                        qbNameInfo.pushdown = pushdownHint;
                        return null;
                    }

                    @Override
                    public Object handleQbNameInPushdown(SqlBasicCall pushdownHint, SqlNode key, SqlNode value) {
                        qbNameInfo.noQbName = false;
                        return null;
                    }
                });

                if (qbNameInfo.noQbName && qbNameInfo.noPushdown) {
                    updatedHints.add(qbName);
                } else if (qbNameInfo.noQbName && !qbNameInfo.noPushdown && null != qbNameInfo.pushdown) {
                    // cannot be here, add qb_name to PUSHDOWN HINT by default
                    // see FastSqlToCalciteNodeVisitor.convertTDDLHint()
                }
            }

            root.setHints(updatedHints);
        }

        public void pushQbName(SqlNode qbName) {
            if (null != qbName) {
                queryBlockName.push(qbName);
            }
        }

        public SqlNode popQbName() {
            if (queryBlockName.size() > 0) {
                return queryBlockName.pop();
            }

            return null;
        }

        public SqlNode currentQbName() {
            return queryBlockName.peek();
        }

        public SqlNode cleanQbName() {
            return popQbName();
        }

        public void registerQbName(SqlNodeList hints) {
            if (null == hints) {
                return;
            }

            SqlNode qbName = getQbName(hints);

            if (null != qbName) {
                queryBlockName.push(qbName);
            }
        }

        public void registerQbName(Map<String, SqlNodeList> hintGroup) {
            if (null == hintGroup) {
                return;
            }

            SqlNode qbName = getQbName(hintGroup);

            if (null != qbName) {
                queryBlockName.push(qbName);
            }
        }

        public SqlNode getQbName(Map<String, SqlNodeList> hintGroup) {
            final SqlNodeList hints = hintGroup.get(DEFAULT_HINT_GROUP);
            if (null == hints) {
                return null;
            }

            return getQbName(hints);
        }

        private SqlNode getQbName(SqlNodeList hints) {
            final String[] qbName = {""};

            queryQbName(hints, new QbNameHandler<String>() {
                @Override
                public String handleQbName(SqlBasicCall qbNameHint) {
                    qbName[0] = stringValue(qbNameHint.operands[0]);
                    return qbName[0];
                }

                @Override
                public String handlePushdown(SqlBasicCall pushdownHint) {
                    return null;
                }

                @Override
                public String handleQbNameInPushdown(SqlBasicCall pushdownHint, SqlNode key, SqlNode value) {
                    qbName[0] = stringValue(value);
                    return qbName[0];
                }
            });

            if (TStringUtil.isNotBlank(qbName[0])) {
                if ("dbj".equalsIgnoreCase(qbName[0])) {
                    dbjHintName.put("dbj", hints);
                    return null;
                }
                return generateQbNameHint(qbName[0]);
            } else {
                return null;
            }
        }

        private void queryQbName(SqlNodeList hints, QbNameHandler qbNameHandler) {
            for (SqlNode hint : hints) {
                final SqlBasicCall hintCall = (SqlBasicCall) hint;
                final String name = hintCall.getOperator().getName();

                if ("qb_name".equalsIgnoreCase(name) && hintCall.getOperandList().size() > 0) {
                    qbNameHandler.handleQbName(hintCall);
                    break;
                } else if ("pushdown".equalsIgnoreCase(name) && hintCall.getOperandList().size() > 0) {
                    qbNameHandler.handlePushdown(hintCall);
                    for (int iArg = 0; iArg < hintCall.getOperandList().size(); iArg++) {
                        SqlNode operand = hintCall.getOperandList().get(iArg);
                        if (operand instanceof SqlBasicCall) {
                            // equality
                            final SqlBasicCall call = (SqlBasicCall) operand;

                            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                                final String key = stringValue(call.operands[0]).toUpperCase();

                                if ("qb_name".equalsIgnoreCase(key)) {
                                    qbNameHandler.handleQbNameInPushdown(hintCall, call.operands[0], call.operands[1]);
                                    break;
                                }
                            } else {
                                // ignore operator which is not equals
                            }
                        } else if (iArg == 5) {
                            qbNameHandler.handleQbNameInPushdown(hintCall, null, operand);
                            break;
                        }
                    }
                } else if ("dbj".equalsIgnoreCase(name) && hintCall.getOperandList().size() > 0) {
                    //SqlNode operand = hintCall.getOperandList().get(0);
                    //qbNameHandler.handleQbNameInPushdown(hintCall, null, operand);

                    qbNameHandler.handleQbName(hintCall);
                    break;

                }
            }
        }

        private SqlNode generateQbNameHint(String qbName) {
            SqlNode[] argNodes = new SqlNode[1];
            argNodes[0] = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                ImmutableList.of(new SqlIdentifier("name", SqlParserPos.ZERO),
                    SqlLiteral.createCharString(qbName, SqlParserPos.ZERO)));

            return new SqlBasicCall(new SqlUnresolvedFunction(new SqlIdentifier("QB_NAME", SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION), argNodes, SqlParserPos.ZERO);
        }
    }

    private interface QbNameHandler<T> {
        T handleQbName(SqlBasicCall qbNameHint);

        T handlePushdown(SqlBasicCall pushdownHint);

        T handleQbNameInPushdown(SqlBasicCall pushdownHint, SqlNode key, SqlNode value);
    }

    private SqlQuantifyOperator negate(SqlQuantifyOperator operator) {
        assert operator.kind == SqlKind.ALL;
        return SqlStdOperatorTable.some(operator.comparisonKind.negateNullSafe());
    }

    /**
     * Deferred lookup.
     */
    private static class DeferredLookup {

        Blackboard bb;
        String originalRelName;

        DeferredLookup(Blackboard bb, String originalRelName) {
            this.bb = bb;
            this.originalRelName = originalRelName;
        }

        public RexFieldAccess getFieldAccess(CorrelationId name) {
            return (RexFieldAccess) bb.mapCorrelateToRex.get(name);
        }

        public String getOriginalRelName() {
            return originalRelName;
        }
    }

    /**
     * A default implementation of SubQueryConverter that does no conversion.
     */
    private class NoOpSubQueryConverter implements SubQueryConverter {

        public boolean canConvertSubQuery() {
            return false;
        }

        public RexNode convertSubQuery(SqlCall subQuery, SqlToRelConverter parentConverter, boolean isExists,
                                       boolean isExplain) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Converts expressions to aggregates.
     * <p>
     * Consider the expression <blockquote>
     * {@code SELECT deptno, SUM(2 * sal) FROM emp GROUP BY deptno}
     * </blockquote>
     * <p>
     * Then:
     * <ul>
     * <li>groupExprs = {SqlIdentifier(deptno)}</li>
     * <li>convertedInputExprs = {RexInputRef(deptno), 2 *
     * RefInputRef(sal)}</li>
     * <li>inputRefs = {RefInputRef(#0), RexInputRef(#1)}</li>
     * <li>aggCalls = {AggCall(SUM, {1})}</li>
     * </ul>
     */
    protected class AggConverter implements SqlVisitor<Void> {

        private final Blackboard bb;
        public final AggregatingSelectScope aggregatingSelectScope;

        private final Map<String, String> nameMap = Maps.newHashMap();

        /**
         * The group-by expressions, in {@link SqlNode} format.
         */
        private final SqlNodeList groupExprs = new SqlNodeList(SqlParserPos.ZERO);

        /**
         * The auxiliary group-by expressions.
         */
        private final Map<SqlNode, Ord<AuxiliaryConverter>> auxiliaryGroupExprs = new HashMap<>();

        /**
         * Input expressions for the group columns and aggregates, in
         * {@link RexNode} format. The first elements of the list correspond to
         * the elements in {@link #groupExprs}; the remaining elements are for
         * aggregates. The right field of each pair is the name of the
         * expression, where the expressions are simple mappings to input
         * fields.
         */
        private final List<Pair<RexNode, String>> convertedInputExprs = new ArrayList<>();

        /**
         * Expressions to be evaluated as rows are being placed into the
         * aggregate's hash table. This is when group functions such as TUMBLE
         * cause rows to be expanded.
         */
        private final List<RexNode> midExprs = new ArrayList<>();

        private final List<AggregateCall> aggCalls = new ArrayList<>();
        private final Map<SqlNode, RexNode> aggMapping = new HashMap<>();
        private final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

        /**
         * Are we directly inside a windowed aggregate?
         */
        private boolean inOver = false;

        /**
         * Creates an AggConverter.
         * <p>
         * The <code>select</code> parameter provides enough context to name
         * aggregate calls which are top-level select list items.
         *
         * @param bb Blackboard
         * @param select Query being translated; provides context to give
         */
        public AggConverter(Blackboard bb, SqlSelect select) {
            this.bb = bb;
            this.aggregatingSelectScope = (AggregatingSelectScope) bb.getValidator().getSelectScope(select);

            // Collect all expressions used in the select list so that aggregate
            // calls can be named correctly.
            final SqlNodeList selectList = select.getSelectList();
            for (int i = 0; i < selectList.size(); i++) {
                SqlNode selectItem = selectList.get(i);
                String name = null;
                if (SqlUtil.isCallTo(selectItem, SqlStdOperatorTable.AS)) {
                    final SqlCall call = (SqlCall) selectItem;
                    selectItem = call.operand(0);
                    name = call.operand(1).toString();
                }
                if (name == null) {
                    name = validator.deriveAlias(selectItem, i);
                }
                nameMap.put(validator.expand(selectItem, aggregatingSelectScope).toString(), name);
            }
        }

        public int addGroupExpr(SqlNode expr) {
            int ref = lookupGroupExpr(expr);
            if (ref >= 0) {
                return ref;
            }
            final int index = groupExprs.size();
            groupExprs.add(expr);
            String name = nameMap.get(expr.toString());
            RexNode convExpr = bb.convertExpression(expr);
            addExpr(convExpr, name);

            if (expr instanceof SqlCall) {
                SqlCall call = (SqlCall) expr;
                for (Pair<SqlNode, AuxiliaryConverter> p : SqlStdOperatorTable.convertGroupToAuxiliaryCalls(call)) {
                    addAuxiliaryGroupExpr(p.left, index, p.right);
                }
            }

            return index;
        }

        void addAuxiliaryGroupExpr(SqlNode node, int index, AuxiliaryConverter converter) {
            for (SqlNode node2 : auxiliaryGroupExprs.keySet()) {
                if (node2.equalsDeep(node, Litmus.IGNORE)) {
                    return;
                }
            }
            auxiliaryGroupExprs.put(node, Ord.of(index, converter));
        }

        /**
         * Adds an expression, deducing an appropriate name if possible.
         *
         * @param expr Expression
         * @param name Suggested name
         */
        private void addExpr(RexNode expr, String name) {
            if ((name == null) && (expr instanceof RexInputRef)) {
                final int i = ((RexInputRef) expr).getIndex();
                name = bb.root.getRowType().getFieldList().get(i).getName();
            }
            if (Pair.right(convertedInputExprs).contains(name)) {
                // In case like 'SELECT ... GROUP BY x, y, x', don't add
                // name 'x' twice.
                name = null;
            }
            convertedInputExprs.add(Pair.of(expr, name));
        }

        public Void visit(SqlIdentifier id) {
            return null;
        }

        public Void visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                nodeList.get(i).accept(this);
            }
            return null;
        }

        public Void visit(SqlLiteral lit) {
            return null;
        }

        public Void visit(SqlDataTypeSpec type) {
            return null;
        }

        public Void visit(SqlDynamicParam param) {
            return null;
        }

        public Void visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }

        public Void visit(SqlCall call) {
            switch (call.getKind()) {
            case FILTER:
                translateAgg((SqlCall) call.operand(0), call.operand(1), call);
                return null;
            case SELECT:
                // rchen 2006-10-17:
                // for now do not detect aggregates in sub-queries.
                return null;
            }
            final boolean prevInOver = inOver;
            // Ignore window aggregates and ranking functions (associated with
            // OVER
            // operator). However, do not ignore nested window aggregates.
            if (call.getOperator().getKind() == SqlKind.OVER) {
                // Track aggregate nesting levels only within an OVER operator.
                inOver = true;
            }

            // Do not translate the top level window aggregate. Only do so for
            // nested aggregates, if present
            if (call.getOperator().isAggregator()) {
                if (inOver) {
                    // Add the parent aggregate level before visiting its
                    // children
                    inOver = false;
                } else {
                    // We're beyond the one ignored level
                    translateAgg(call, null, call);
                    return null;
                }
            }
            for (SqlNode operand : call.getOperandList()) {
                // Operands are occasionally null, e.g. switched CASE arg 0.
                if (operand != null) {
                    operand.accept(this);
                }
            }
            // Remove the parent aggregate level after visiting its children
            inOver = prevInOver;
            return null;
        }

        private void translateAgg(SqlCall call, SqlNode filter, SqlCall outerCall) {
            assert bb.agg == this;
            final List<Integer> args = new ArrayList<>();
            // for GROUP_CONCAT
            final List<Integer> orderList = new ArrayList<>();
            String separator = null;
            ArrayList<String> ascOrDescList = null;

            int filterArg = -1;
            final List<RelDataType> argTypes = call
                .getOperator() instanceof SqlCountAggFunction ? new ArrayList<RelDataType>(
                call.getOperandList().size()) : null;
            try {
                // switch out of agg mode
                bb.agg = null;
                for (SqlNode operand : call.getOperandList()) {

                    // special case for COUNT(*): delete the *
                    if (operand instanceof SqlIdentifier) {
                        SqlIdentifier id = (SqlIdentifier) operand;
                        if (id.isStar()) {
                            assert call.operandCount() == 1;
                            assert args.isEmpty();
                            break;
                        }
                    }
                    RexNode convertedExpr = bb.convertExpression(operand);
                    assert convertedExpr != null;
                    if (argTypes != null) {
                        argTypes.add(convertedExpr.getType());
                    }
                    args.add(lookupOrCreateGroupExpr(convertedExpr));
                }

                if (call instanceof GroupConcatCall) {
                    GroupConcatCall groupConcatCall = (GroupConcatCall) call;
                    for (SqlNode operand : groupConcatCall.getOrderOperands()) {
                        RexNode convertedExpr = bb.convertExpression(operand);
                        assert convertedExpr != null;
                        if (convertedExpr instanceof RexLiteral) {
                            int n = Integer.parseInt(((RexLiteral) convertedExpr).getValue().toString());
                            orderList.add(args.get(n - 1));
                        } else {
                            orderList.add(lookupOrCreateGroupExpr(convertedExpr));
                        }
                    }

                    separator = groupConcatCall.getSeparator();
                    ascOrDescList = groupConcatCall.getAscOrDescList();
                }

                if (filter != null) {
                    RexNode convertedExpr = bb.convertExpression(filter);
                    assert convertedExpr != null;
                    if (convertedExpr.getType().isNullable()) {
                        convertedExpr = rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, convertedExpr);
                    }
                    filterArg = lookupOrCreateGroupExpr(convertedExpr);
                }
            } finally {
                // switch back into agg mode
                bb.agg = this;
            }

            SqlAggFunction aggFunction = (SqlAggFunction) call.getOperator();
            final RelDataType type = validator.deriveType(bb.scope, call);
            boolean distinct = false;
            SqlLiteral quantifier = call.getFunctionQuantifier();
            if ((null != quantifier) && (quantifier.getValue() == SqlSelectKeyword.DISTINCT)) {
                distinct = true;
            }
            boolean approximate = false;
            if (aggFunction == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
                aggFunction = SqlStdOperatorTable.COUNT;
                distinct = true;
                approximate = true;
            }

            final AggregateCall aggCall;

            if (call instanceof GroupConcatCall) {
                aggCall = GroupConcatAggregateCall.create(aggFunction,
                    distinct,
                    approximate,
                    args,
                    filterArg,
                    type,
                    nameMap.get(validator.expand(outerCall, aggregatingSelectScope).toString()),
                    orderList,
                    separator,
                    ascOrDescList
                );
            } else {
                aggCall = AggregateCall.create(aggFunction,
                    distinct,
                    approximate,
                    args,
                    filterArg,
                    type,
                    nameMap.get(validator.expand(outerCall, aggregatingSelectScope).toString()));
            }

            final AggregatingSelectScope.Resolved r = aggregatingSelectScope.resolved.get();
            RexNode rex = rexBuilder.addAggCall(aggCall, groupExprs.size(), false, aggCalls, aggCallMapping, argTypes);
            aggMapping.put(outerCall, rex);
        }

        private int lookupOrCreateGroupExpr(RexNode expr) {
            int index = 0;
            for (RexNode convertedInputExpr : Pair.left(convertedInputExprs)) {
                if (expr.toString().equals(convertedInputExpr.toString())) {
                    return index;
                }
                ++index;
            }

            // not found -- add it
            addExpr(expr, null);
            return index;
        }

        /**
         * If an expression is structurally identical to one of the group-by
         * expressions, returns a reference to the expression, otherwise returns
         * null.
         */
        public int lookupGroupExpr(SqlNode expr) {
            for (int i = 0; i < groupExprs.size(); i++) {
                SqlNode groupExpr = groupExprs.get(i);
                if (expr.equalsDeep(groupExpr, Litmus.IGNORE)) {
                    return i;
                }
            }

            /**
             * Find internal agg function.
             */
            for (Map.Entry<SqlNode, RexNode> entry : aggMapping.entrySet()) {
                SqlNode key = entry.getKey();
                if (!key.getKind().equals(SqlKind.__FIRST_VALUE)) {
                    continue;
                }
                SqlCall call = (SqlCall) key;
                if (expr.equalsDeep(call.operand(0), Litmus.IGNORE)) {
                    RexNode value = entry.getValue();
                    if (value instanceof RexInputRef) {
                        return ((RexInputRef) value).getIndex();
                    }
                }
            }

            return -1;
        }

        public RexNode lookupAggregates(SqlCall call) {
            // assert call.getOperator().isAggregator();
            assert bb.agg == this;

            for (Map.Entry<SqlNode, Ord<AuxiliaryConverter>> e : auxiliaryGroupExprs.entrySet()) {
                if (call.equalsDeep(e.getKey(), Litmus.IGNORE)) {
                    AuxiliaryConverter converter = e.getValue().e;
                    final int groupOrdinal = e.getValue().i;
                    return converter.convert(rexBuilder,
                        convertedInputExprs.get(groupOrdinal).left,
                        rexBuilder.makeInputRef(bb.root, groupOrdinal));
                }
            }

            return aggMapping.get(call);
        }

        public List<Pair<RexNode, String>> getPreExprs() {
            return convertedInputExprs;
        }

        public List<AggregateCall> getAggCalls() {
            return aggCalls;
        }

        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }
    }

    /**
     * Context to find a relational expression to a field offset.
     */
    private static class LookupContext {

        private final List<Pair<RelNode, Integer>> relOffsetList = new ArrayList<>();

        /**
         * Creates a LookupContext with multiple input relational expressions.
         *
         * @param bb Context for translating this sub-query
         * @param rels Relational expressions
         * @param systemFieldCount Number of system fields
         */
        LookupContext(Blackboard bb, List<RelNode> rels, int systemFieldCount) {
            bb.flatten(rels, systemFieldCount, new int[] {0}, relOffsetList);
        }

        /**
         * Returns the relational expression with a given offset, and the
         * ordinal in the combined row of its first field.
         * <p>
         * For example, in {@code Emp JOIN Dept}, findRel(1) returns the
         * relational expression for {@code Dept} and offset 6 (because
         * {@code Emp} has 6 fields, therefore the first field of {@code Dept}
         * is field 6.
         *
         * @param offset Offset of relational expression in FROM clause
         * @return Relational expression and the ordinal of its first field
         */
        Pair<RelNode, Integer> findRel(int offset) {
            return relOffsetList.get(offset);
        }
    }

    /**
     * Shuttle which walks over a tree of {@link RexNode}s and applies 'over' to
     * all agg functions.
     * <p>
     * This is necessary because the returned expression is not necessarily a
     * call to an agg function. For example,
     * <blockquote><code>AVG(x)</code></blockquote>
     * <p>
     * becomes <blockquote><code>SUM(x) / COUNT(x)</code></blockquote>
     * <p>
     * Any aggregate functions are converted to calls to the internal <code>
     * $Histogram</code> aggregation function and accessors such as <code>
     * $HistogramMin</code>; for example,
     * <blockquote><code>MIN(x), MAX(x)</code></blockquote>
     * <p>
     * are converted to <blockquote><code>$HistogramMin($Histogram(x)),
     * $HistogramMax($Histogram(x))</code></blockquote>
     * <p>
     * Common sub-expression elimination will ensure that only one histogram is
     * computed.
     */
    private class HistogramShuttle extends RexShuttle {

        /**
         * Whether to convert calls to MIN(x) to HISTOGRAM_MIN(HISTOGRAM(x)).
         * Histograms allow rolling computation, but require more space.
         */
        static final boolean ENABLE_HISTOGRAM_AGG = false;

        private final List<RexNode> partitionKeys;
        private final ImmutableList<RexFieldCollation> orderKeys;
        private final RexWindowBound lowerBound;
        private final RexWindowBound upperBound;
        private final SqlWindow window;
        private final boolean distinct;

        HistogramShuttle(List<RexNode> partitionKeys, ImmutableList<RexFieldCollation> orderKeys,
                         RexWindowBound lowerBound, RexWindowBound upperBound, SqlWindow window, boolean distinct) {
            this.partitionKeys = partitionKeys;
            this.orderKeys = orderKeys;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.window = window;
            this.distinct = distinct;
        }

        public RexNode visitCall(RexCall call) {
            final SqlOperator op = call.getOperator();
            if (!(op instanceof SqlAggFunction)) {
                return super.visitCall(call);
            }
            final SqlAggFunction aggOp = (SqlAggFunction) op;
            final RelDataType type = call.getType();
            List<RexNode> exprs = call.getOperands();
            if (aggOp.getKind() == SqlKind.RANK || aggOp.getKind() == SqlKind.DENSE_RANK
                || aggOp.getKind() == SqlKind.PERCENT_RANK || aggOp.getKind() == SqlKind.CUME_DIST) {
                //For cume_dist if not have order, we need set to empty
                if (orderKeys.size() == 0) {
                    exprs = ImmutableList.of();
                } else {
                    exprs = this.orderKeys.stream().map(Pair::getKey).collect(Collectors.toList());
                }
            }
            SqlFunction histogramOp = !ENABLE_HISTOGRAM_AGG ? null : getHistogramOp(aggOp);

            if (histogramOp != null) {
                final RelDataType histogramType = computeHistogramType(type);

                // For DECIMAL, since it's already represented as a bigint we
                // want to do a reinterpretCast instead of a cast to avoid
                // losing any precision.
                boolean reinterpretCast = type.getSqlTypeName() == SqlTypeName.DECIMAL;

                // Replace original expression with CAST of not one
                // of the supported types
                if (histogramType != type) {
                    exprs = new ArrayList<>(exprs);
                    exprs.set(0,
                        reinterpretCast ? rexBuilder.makeReinterpretCast(histogramType,
                            exprs.get(0),
                            rexBuilder.makeLiteral(false)) : rexBuilder.makeCast(histogramType, exprs.get(0)));
                }

                RexCallBinding bind = new RexCallBinding(rexBuilder.getTypeFactory(),
                    SqlStdOperatorTable.HISTOGRAM_AGG,
                    exprs,
                    ImmutableList.<RelCollation>of());

                RexNode over = rexBuilder.makeOver(SqlStdOperatorTable.HISTOGRAM_AGG.inferReturnType(bind),
                    SqlStdOperatorTable.HISTOGRAM_AGG,
                    exprs,
                    partitionKeys,
                    orderKeys,
                    lowerBound,
                    upperBound,
                    window.isRows(),
                    window.isAllowPartial(),
                    false,
                    distinct);

                RexNode histogramCall = rexBuilder.makeCall(histogramType, histogramOp, ImmutableList.of(over));

                // If needed, post Cast result back to original
                // type.
                if (histogramType != type) {
                    if (reinterpretCast) {
                        histogramCall = rexBuilder
                            .makeReinterpretCast(type, histogramCall, rexBuilder.makeLiteral(false));
                    } else {
                        histogramCall = rexBuilder.makeCast(type, histogramCall);
                    }
                }

                return histogramCall;
            } else {
                boolean needSum0 = aggOp == SqlStdOperatorTable.SUM && type.isNullable();
                SqlAggFunction aggOpToUse = needSum0 ? SqlStdOperatorTable.SUM0 : aggOp;
                return rexBuilder.makeOver(type,
                    aggOpToUse,
                    exprs,
                    partitionKeys,
                    orderKeys,
                    lowerBound,
                    upperBound,
                    window.isRows(),
                    window.isAllowPartial(),
                    false,
//                    needSum0,
                    distinct);
            }
        }

        /**
         * Returns the histogram operator corresponding to a given aggregate
         * function.
         * <p>
         * For example, <code>getHistogramOp
         * ({@link SqlStdOperatorTable#MIN}}</code> returns
         * {@link SqlStdOperatorTable#HISTOGRAM_MIN}.
         *
         * @param aggFunction An aggregate function
         * @return Its histogram function, or null
         */
        SqlFunction getHistogramOp(SqlAggFunction aggFunction) {
            if (aggFunction == SqlStdOperatorTable.MIN) {
                return SqlStdOperatorTable.HISTOGRAM_MIN;
            } else if (aggFunction == SqlStdOperatorTable.MAX) {
                return SqlStdOperatorTable.HISTOGRAM_MAX;
            } else if (aggFunction == SqlStdOperatorTable.FIRST_VALUE) {
                return SqlStdOperatorTable.HISTOGRAM_FIRST_VALUE;
            } else if (aggFunction == SqlStdOperatorTable.LAST_VALUE) {
                return SqlStdOperatorTable.HISTOGRAM_LAST_VALUE;
            } else {
                return null;
            }
        }

        /**
         * Returns the type for a histogram function. It is either the actual
         * type or an an approximation to it.
         */
        private RelDataType computeHistogramType(RelDataType type) {
            if (SqlTypeUtil.isExactNumeric(type) && type.getSqlTypeName() != SqlTypeName.BIGINT) {
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            } else if (SqlTypeUtil.isApproximateNumeric(type) && type.getSqlTypeName() != SqlTypeName.DOUBLE) {
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            } else {
                return type;
            }
        }
    }

    /**
     * A sub-query, whether it needs to be translated using 2- or 3-valued
     * logic.
     */
    private static class SubQuery {

        final SqlNode node;
        final RelOptUtil.Logic logic;
        RexNode expr;

        private SubQuery(SqlNode node, RelOptUtil.Logic logic) {
            this.node = node;
            this.logic = logic;
        }
    }

    /**
     * Visitor that collects all aggregate functions in a {@link SqlNode} tree.
     */
    private static class AggregateFinder extends SqlBasicVisitor<Void> {

        final SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
        final SqlNodeList filterList = new SqlNodeList(SqlParserPos.ZERO);

        @Override
        public Void visit(SqlCall call) {
            // ignore window aggregates and ranking functions (associated with
            // OVER operator)
            if (call.getOperator().getKind() == SqlKind.OVER) {
                return null;
            }

            if (call.getOperator().getKind() == SqlKind.FILTER) {
                // the WHERE in a FILTER must be tracked too so we can call
                // replaceSubQueries on it.
                // see https://issues.apache.org/jira/browse/CALCITE-1910
                final SqlNode aggCall = call.getOperandList().get(0);
                final SqlNode whereCall = call.getOperandList().get(1);
                list.add(aggCall);
                filterList.add(whereCall);
                return null;
            }

            if (call.getOperator().isAggregator()) {
                list.add(call);
                return null;
            }

            // Don't traverse into sub-queries, even if they contain aggregate
            // functions.
            if (call instanceof SqlSelect) {
                return null;
            }

            return call.getOperator().acceptCall(this, call);
        }
    }

    /**
     * Use of a row as a correlating variable by a given relational expression.
     */
    private static class CorrelationUse {

        private final CorrelationId id;
        private final ImmutableBitSet requiredColumns;
        private final RelNode r;

        CorrelationUse(CorrelationId id, ImmutableBitSet requiredColumns, RelNode r) {
            this.id = id;
            this.requiredColumns = requiredColumns;
            this.r = r;
        }
    }

    /**
     * Creates a builder for a {@link Config}.
     */
    public static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }

    /**
     * Interface to define the configuration for a SqlToRelConverter. Provides
     * methods to set each configuration option.
     *
     * @see ConfigBuilder
     * @see SqlToRelConverter#configBuilder()
     */
    public interface Config {

        /**
         * Default configuration.
         */
        Config DEFAULT = configBuilder().build();

        /**
         * Returns the {@code convertTableAccess} option. Controls whether table
         * access references are converted to physical rels immediately. The
         * optimizer doesn't like leaf rels to have {@link Convention#NONE}.
         * However, if we are doing further conversion passes (e.g.
         * {@link RelStructuredTypeFlattener}), then we may need to defer
         * conversion.
         */
        boolean isConvertTableAccess();

        /**
         * Returns the {@code decorrelationEnabled} option. Controls whether to
         * disable sub-query decorrelation when needed. e.g. if outer joins are
         * not supported.
         */
        boolean isDecorrelationEnabled();

        /**
         * Returns the {@code trimUnusedFields} option. Controls whether to trim
         * unused fields as part of the conversion process.
         */
        boolean isTrimUnusedFields();

        /**
         * Returns the {@code createValuesRel} option. Controls whether
         * instances of {@link org.apache.calcite.rel.logical.LogicalValues} are
         * generated. These may not be supported by all physical
         * implementations.
         */
        boolean isCreateValuesRel();

        /**
         * Returns the {@code explain} option. Describes whether the current
         * statement is part of an EXPLAIN PLAN statement.
         */
        boolean isExplain();

        /**
         * Returns the {@code expand} option. Controls whether to expand
         * sub-queries. If false, each sub-query becomes a
         * {@link org.apache.calcite.rex.RexSubQuery}.
         */
        boolean isExpand();

        /**
         * Returns the {@code inSubQueryThreshold} option, default
         * {@link #DEFAULT_IN_SUB_QUERY_THRESHOLD}. Controls the list size
         * threshold under which {@link #convertInToOr} is used. Lists of this
         * size or greater will instead be converted to use a join against an
         * inline table ({@link org.apache.calcite.rel.logical.LogicalValues})
         * rather than a predicate. A threshold of 0 forces usage of an inline
         * table in all cases; a threshold of {@link Integer#MAX_VALUE} forces
         * usage of OR in all cases.
         */
        int getInSubQueryThreshold();
    }

    /**
     * Builder for a {@link Config}.
     */
    public static class ConfigBuilder {

        private boolean convertTableAccess = true;
        private boolean decorrelationEnabled = true;
        private boolean trimUnusedFields = false;
        private boolean createValuesRel = true;
        private boolean explain;
        private boolean expand = true;
        private int inSubQueryThreshold = DEFAULT_IN_SUB_QUERY_THRESHOLD;

        private ConfigBuilder() {
        }

        /**
         * Sets configuration identical to a given {@link Config}.
         */
        public ConfigBuilder withConfig(Config config) {
            this.convertTableAccess = config.isConvertTableAccess();
            this.decorrelationEnabled = config.isDecorrelationEnabled();
            this.trimUnusedFields = config.isTrimUnusedFields();
            this.createValuesRel = config.isCreateValuesRel();
            this.explain = config.isExplain();
            this.expand = config.isExpand();
            this.inSubQueryThreshold = config.getInSubQueryThreshold();
            return this;
        }

        public ConfigBuilder withConvertTableAccess(boolean convertTableAccess) {
            this.convertTableAccess = convertTableAccess;
            return this;
        }

        public ConfigBuilder withDecorrelationEnabled(boolean enabled) {
            this.decorrelationEnabled = enabled;
            return this;
        }

        public ConfigBuilder withTrimUnusedFields(boolean trimUnusedFields) {
            this.trimUnusedFields = trimUnusedFields;
            return this;
        }

        public ConfigBuilder withCreateValuesRel(boolean createValuesRel) {
            this.createValuesRel = createValuesRel;
            return this;
        }

        public ConfigBuilder withExplain(boolean explain) {
            this.explain = explain;
            return this;
        }

        public ConfigBuilder withExpand(boolean expand) {
            this.expand = expand;
            return this;
        }

        @Deprecated // to be removed before 2.0
        public ConfigBuilder withInSubqueryThreshold(int inSubQueryThreshold) {
            return withInSubQueryThreshold(inSubQueryThreshold);
        }

        public ConfigBuilder withInSubQueryThreshold(int inSubQueryThreshold) {
            this.inSubQueryThreshold = inSubQueryThreshold;
            return this;
        }

        /**
         * Builds a {@link Config}.
         */
        public Config build() {
            return new ConfigImpl(convertTableAccess,
                decorrelationEnabled,
                trimUnusedFields,
                createValuesRel,
                explain,
                expand,
                inSubQueryThreshold);
        }
    }

    /**
     * Implementation of {@link Config}. Called by builder; all values are in
     * private final fields.
     */
    private static class ConfigImpl implements Config {

        private final boolean convertTableAccess;
        private final boolean decorrelationEnabled;
        private final boolean trimUnusedFields;
        private final boolean createValuesRel;
        private final boolean explain;
        private final int inSubQueryThreshold;
        private final boolean expand;

        private ConfigImpl(boolean convertTableAccess, boolean decorrelationEnabled, boolean trimUnusedFields,
                           boolean createValuesRel, boolean explain, boolean expand, int inSubQueryThreshold) {
            this.convertTableAccess = convertTableAccess;
            this.decorrelationEnabled = decorrelationEnabled;
            this.trimUnusedFields = trimUnusedFields;
            this.createValuesRel = createValuesRel;
            this.explain = explain;
            this.expand = expand;
            this.inSubQueryThreshold = inSubQueryThreshold;
        }

        public boolean isConvertTableAccess() {
            return convertTableAccess;
        }

        public boolean isDecorrelationEnabled() {
            return decorrelationEnabled;
        }

        public boolean isTrimUnusedFields() {
            return trimUnusedFields;
        }

        public boolean isCreateValuesRel() {
            return createValuesRel;
        }

        public boolean isExplain() {
            return explain;
        }

        public boolean isExpand() {
            return expand;
        }

        public int getInSubQueryThreshold() {
            return inSubQueryThreshold;
        }
    }

    private static class AliasContext {

        private final SqlIdentifier alias;
        private final boolean tableAlias;

        public AliasContext(SqlIdentifier alias, boolean tableAlias) {
            this.alias = alias;
            this.tableAlias = tableAlias;
        }

        public static AliasContext empty() {
            return new AliasContext(SqlIdentifier.star(SqlParserPos.ZERO), false);
        }

        public SqlIdentifier getAlias() {
            return alias;
        }

        public boolean isTableAlias() {
            return tableAlias;
        }
    }

    public static class HintBlackboard {

        private final Deque<Map<String, SqlNodeList>> hintStack = new ArrayDeque<>();

        private final Deque<AliasContext> aliasStack = new ArrayDeque<>();

        private final Deque<RexNode> asOfStack = new ArrayDeque<>();

        public void beginSelect() {
            hintStack.push(new HashMap<>(2));
        }

        public void beginSelect(SqlSelect select) {
            if (select instanceof TDDLSqlSelect) {
                hintStack.push(getGroupMap(((TDDLSqlSelect) select).getHints()));
            } else {
                hintStack.push(new HashMap<>(2));
            }
        }

        public Map<String, SqlNodeList> endFrom() {
            return hintStack.pop();
        }

        public Map<String, SqlNodeList> currentGroups() {
            return hintStack.peek();
        }

        public void beginAsOf(RexNode flashback) {
            asOfStack.push(flashback);
        }

        public RexNode endAsOf() {
            return this.asOfStack.pop();
        }

        public boolean hasAsOf() {
            return !asOfStack.isEmpty();
        }

        public RexNode peekAsOf() {
            return this.asOfStack.peek();
        }

        public void beginAlias(SqlNode operand0, SqlNode operand1) {
            // remember current alias in from
            if (operand1 instanceof SqlIdentifier) {
                aliasStack.push(new AliasContext((SqlIdentifier) operand1, operand0 instanceof SqlIdentifier));
            } else {
                aliasStack.push(AliasContext.empty());
            }
        }

        public AliasContext endAlias() {
            return this.aliasStack.pop();
        }

        /**
         * Get table hint with priority:
         *
         * <pre>
         * 1. hint marked by same alias or table name (first com first get)
         * 2. hint marked by default name
         * </pre>
         */
        public SqlNodeList currentHints(String tableName) {
            tableName = tableName.toLowerCase();

            final AliasContext aliasCtx = aliasStack.peek();
            final Map<String, SqlNodeList> hintGroup = hintStack.peek();

            if (null == hintGroup) {
                return new SqlNodeList(SqlParserPos.ZERO);
            }

            // default hint
            SqlNodeList hints = null;
            final List<SqlNode> derivedAliasHints = new ArrayList<>();

            final Iterator<Entry<String, SqlNodeList>> it = hintGroup.entrySet().iterator();
            while (it.hasNext()) {
                final Entry<String, SqlNodeList> e = it.next();
                final String k = e.getKey();
                final SqlNodeList v = e.getValue();

                // hint contains tableName
                if (null == hints && TStringUtil.equalsIgnoreCase(k, tableName)) {
                    if (derivedAliasHints.isEmpty()) {
                        hints = v;
                        // should not break from here, cause hints with alias need to be removed
                        continue;
                    } else {
                        final List<SqlNode> tableAliasHints = new ArrayList<>();
                        for (SqlNode hint : v) {
                            if (onlyTableAlias((SqlBasicCall) hint)) {
                                tableAliasHints.add(hint);
                            }
                        }

                        hints = new SqlNodeList(
                            ImmutableList.<SqlNode>builder().addAll(derivedAliasHints).addAll(tableAliasHints).build(),
                            SqlParserPos.ZERO);

                        // all task (find hints and remove hints with alias) finished
                        break;
                    }
                }

                // alias exists
                if (null != aliasCtx) {
                    final String alias = Util.last(aliasCtx.getAlias().names);
                    if (TStringUtil.equalsIgnoreCase(k, alias)) {
                        // hints key matches alias
                        if (null == hints) {
                            // find hints
                            if (aliasCtx.isTableAlias()) {
                                // table alias or table name only
                                for (SqlNode hint : v) {
                                    final SqlBasicCall hintCall = (SqlBasicCall) hint;
                                    final String opName = hintCall.getOperator().getName();

                                    if ("index".equalsIgnoreCase(opName) && hintCall.getOperandList().size() > 0) {
                                        // replace alias with table name
                                        hintCall.getOperands()[0] = new SqlIdentifier(tableName, SqlParserPos.ZERO);
                                    }
                                }

                                hints = v;

                                // take hint with alias away
                                it.remove();

                                // all task (find hints and remove hints with alias) finished
                                break;
                            } else {
                                // derived table alias
                                final List<SqlNode> unmatchedHints = new ArrayList<>();

                                for (SqlNode hint : v) {
                                    final SqlBasicCall hintCall = (SqlBasicCall) hint;

                                    if (onlyTableAlias(hintCall)) {
                                        unmatchedHints.add(hintCall);
                                    } else {
                                        derivedAliasHints.add(hintCall);
                                    }
                                }

                                // tack hint with alias away
                                e.setValue(new SqlNodeList(unmatchedHints, SqlParserPos.ZERO));
                            }
                        } else {
                            // hints with table name specified already exists
                            // remove following hints with alias specified
                            it.remove();

                            // all task (find hints and remove hints with alias) finished
                            break;
                        }
                    }
                }
            }

            if (null == hints && !derivedAliasHints.isEmpty()) {
                hints = new SqlNodeList(ImmutableList.<SqlNode>builder().addAll(derivedAliasHints).build(),
                    SqlParserPos.ZERO);
            }

            // empty hint
            if (null == hints) {
                hints =
                    Optional.ofNullable(hintGroup.get(DEFAULT_HINT_GROUP)).orElse(new SqlNodeList(SqlParserPos.ZERO));
            }
            return hints;
        }

        private boolean onlyTableAlias(SqlBasicCall hintCall) {
            final String opName = hintCall.getOperator().getName();
            return "index".equalsIgnoreCase(opName) && hintCall.getOperandList().size() > 0;
        }

        /**
         * if has join return default hint and remove it from stack
         */
        public SqlNodeList joinHints(SqlNode left, Blackboard bb) {
            SqlNodeList hints = hintStack.peek().get(DEFAULT_HINT_GROUP);

            if (null != hints) {
                hintStack.peek().remove(DEFAULT_HINT_GROUP);
            } else {
                if (bb.getDbjHintName() != null) {
                    hints = bb.getDbjHintName().get("dbj");
                } else {
                    hints = new SqlNodeList(SqlParserPos.ZERO);
                }
            }
            return hints;
        }

        /**
         * Generate map[{first param of construct}, {hints between this and next
         * construct}]
         */
        private Map<String, SqlNodeList> getGroupMap(SqlNodeList hints) {
            Map<String, SqlNodeList> result = new LinkedHashMap<>();

            for (SqlNode node : hints.getList()) {
                final SqlNodeList hint = (SqlNodeList) node;

                for (SqlNode groupNode : hint.getList()) {
                    final SqlNodeList group = (SqlNodeList) groupNode;

                    String tableName = DEFAULT_HINT_GROUP;
                    for (SqlNode groupHint : group.getList()) {
                        final SqlBasicCall hintCall = (SqlBasicCall) groupHint;
                        final String name = hintCall.getOperator().getName();
                        if ("construct".equalsIgnoreCase(name) && hintCall.getOperandList().size() > 0) {
                            SqlNode operand = hintCall.getOperandList().get(0);
                            tableName = getTableName(tableName, operand);
                        } else if (onlyTableAlias(hintCall)) {
                            SqlNode operand = hintCall.getOperandList().get(0);
                            tableName = getTableName(tableName, operand);
                        }
                    } // end of for

                    if (result.containsKey(tableName)) {
                        List<SqlNode> tmp = new ArrayList<>(result.get(tableName).getList());
                        tmp.addAll(group.getList());
                        result.put(tableName, new SqlNodeList(tmp, SqlParserPos.ZERO));
                    } else {
                        result.put(tableName, group);
                    }
                } // end of for
            } // end of for

            return result;
        }

        private String getTableName(String tableName, SqlNode operand) {
            if (operand instanceof SqlLiteral) {
                tableName = ((SqlLiteral) operand).toValue().toLowerCase();
            } else if (operand instanceof SqlIdentifier) {
                tableName = Util.last(((SqlIdentifier) operand).names).toLowerCase();
            }
            return tableName;
        }
    }

    private RelNode convertUnArchive(SqlUnArchive query) {
        final RelDataType targetRowType = validator.getValidatedNodeType(query);
        assert targetRowType != null;
        return UnArchive.create(query, targetRowType, getCluster());
    }
}

// End SqlToRelConverter.java
