package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarStoreUtils;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.FlashbackScanPreProcessor;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.SetType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.FILE_META;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.openMockFile;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.prepareFileSystem;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.readMockFile;
import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;
import static com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil.parseEnumType;
import static com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil.parseSetType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class CsvScanTestBase {

    protected final static String DEL_FILE_NAME = "fb2604e133e6.del";
    protected final static String DATA_FILE_NAME = "6659a663977d.csv";
    protected final static String FULL_TYPE_FILE_NAME = "1023a18dcf60.csv";
    protected FlashbackScanPreProcessor flashbackScanPreProcessor;
    protected DefaultScanPreProcessor defaultScanPreProcessor;
    @Mock
    protected Configuration configuration;
    @Mock
    protected FileSystem fileSystem;

    protected static DynamicColumnarManager columnarManager = mock(DynamicColumnarManager.class);

    protected static final String SCHEMA_NAME = "";
    protected static final String LOGICAL_TABLE_NAME = "1";
    private static final String PARTITION_NAME = "p1";
    protected static final Long TSO = 7182618688200114304L;
    private static final Long FILE_LENGTH = 4088L;
    protected static final int DELETE_COUNT = 1219;
    protected static final Path TEST_FILE_PATH = new Path("/" + DEL_FILE_NAME);
    protected static final Path DATA_FILE_PATH = new Path("/" + DATA_FILE_NAME);
    protected static final Path FULL_TYPE_FILE_PATH = new Path("/", FULL_TYPE_FILE_NAME);

    protected MockedStatic<ColumnarManager> columnarManagerMockedStatic;
    protected MockedStatic<FileSystemUtils> mockFsUtils;

    public final static FileMeta FULL_TYPE_FILE_META;

    static {
        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

        FULL_TYPE_FILE_META = new FileMeta(
            "", "1", "", "", "p1", FULL_TYPE_FILE_NAME,
            0, 0,
            7257742230016753728L, null, 7257742230016753728L,
            "", "", Engine.LOCAL_DISK, 0L);

        // refs: https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
        List<ColumnMeta> columnMetas = new ArrayList<>();
        addColumnMeta(columnMetas, "", "tso", SqlTypeName.BIGINT);
        addColumnMeta(columnMetas, "", "position", SqlTypeName.BIGINT);
        addColumnMeta(columnMetas, "", "id", SqlTypeName.BIGINT);
        addColumnMeta(columnMetas, "", "c_bit_1", SqlTypeName.BIT, 1);
        addColumnMeta(columnMetas, "", "c_bit_8", SqlTypeName.BIG_BIT, 8);
        addColumnMeta(columnMetas, "", "c_bit_16", SqlTypeName.BIG_BIT, 16);
        addColumnMeta(columnMetas, "", "c_bit_32", SqlTypeName.BIG_BIT, 32);
        addColumnMeta(columnMetas, "", "c_bit_64", SqlTypeName.BIG_BIT, 64);
        addColumnMeta(columnMetas, "", "c_tinyint_1", SqlTypeName.TINYINT, 1);
        addColumnMeta(columnMetas, "", "c_tinyint_1_un", SqlTypeName.TINYINT_UNSIGNED, 1);
        addColumnMeta(columnMetas, "", "c_tinyint_4", SqlTypeName.TINYINT, 4);
        addColumnMeta(columnMetas, "", "c_tinyint_4_un", SqlTypeName.TINYINT_UNSIGNED, 4);
        addColumnMeta(columnMetas, "", "c_tinyint_8", SqlTypeName.TINYINT, 8);
        addColumnMeta(columnMetas, "", "c_tinyint_8_un", SqlTypeName.TINYINT_UNSIGNED, 8);
        addColumnMeta(columnMetas, "", "c_smallint_1", SqlTypeName.SMALLINT);
        addColumnMeta(columnMetas, "", "c_smallint_16", SqlTypeName.SMALLINT);
        addColumnMeta(columnMetas, "", "c_smallint_16_un", SqlTypeName.SMALLINT_UNSIGNED);
        addColumnMeta(columnMetas, "", "c_mediumint_1", SqlTypeName.MEDIUMINT);
        addColumnMeta(columnMetas, "", "c_mediumint_24", SqlTypeName.MEDIUMINT);
        addColumnMeta(columnMetas, "", "c_mediumint_24_un", SqlTypeName.MEDIUMINT_UNSIGNED);
        addColumnMeta(columnMetas, "", "c_int_1", SqlTypeName.INTEGER);
        addColumnMeta(columnMetas, "", "c_int_32", SqlTypeName.INTEGER);
        addColumnMeta(columnMetas, "", "c_int_32_un", SqlTypeName.INTEGER_UNSIGNED);
        addColumnMeta(columnMetas, "", "c_bigint_1", SqlTypeName.BIGINT);
        addColumnMeta(columnMetas, "", "c_bigint_64", SqlTypeName.BIGINT);
        addColumnMeta(columnMetas, "", "c_bigint_64_un", SqlTypeName.BIGINT_UNSIGNED);
        addColumnMeta(columnMetas, "", "c_decimal", SqlTypeName.DECIMAL, 10, 0);
        addColumnMeta(columnMetas, "", "c_decimal_pr", SqlTypeName.DECIMAL, 65, 30);
        addColumnMeta(columnMetas, "", "c_float", SqlTypeName.FLOAT);
        addColumnMeta(columnMetas, "", "c_float_pr", SqlTypeName.FLOAT, 10, 3);
        addColumnMeta(columnMetas, "", "c_float_un", SqlTypeName.FLOAT, 10, 3);
        addColumnMeta(columnMetas, "", "c_double", SqlTypeName.DOUBLE);
        addColumnMeta(columnMetas, "", "c_double_pr", SqlTypeName.DOUBLE, 10, 3);
        addColumnMeta(columnMetas, "", "c_double_un", SqlTypeName.DOUBLE, 10, 3);
        addColumnMeta(columnMetas, "", "c_date", SqlTypeName.DATE);
        addColumnMeta(columnMetas, "", "c_datetime", SqlTypeName.DATETIME);
        addColumnMeta(columnMetas, "", "c_datetime_1", SqlTypeName.DATETIME, 1);
        addColumnMeta(columnMetas, "", "c_datetime_3", SqlTypeName.DATETIME, 3);
        addColumnMeta(columnMetas, "", "c_datetime_6", SqlTypeName.DATETIME, 6);
        addColumnMeta(columnMetas, "", "c_timestamp", SqlTypeName.TIMESTAMP);
        addColumnMeta(columnMetas, "", "c_timestamp_1", SqlTypeName.TIMESTAMP, 1);
        addColumnMeta(columnMetas, "", "c_timestamp_3", SqlTypeName.TIMESTAMP, 3);
        addColumnMeta(columnMetas, "", "c_timestamp_6", SqlTypeName.TIMESTAMP, 6);
        addColumnMeta(columnMetas, "", "c_time", SqlTypeName.TIME);
        addColumnMeta(columnMetas, "", "c_time_1", SqlTypeName.TIME, 1);
        addColumnMeta(columnMetas, "", "c_time_3", SqlTypeName.TIME, 3);
        addColumnMeta(columnMetas, "", "c_time_6", SqlTypeName.TIME, 6);
        addColumnMeta(columnMetas, "", "c_year", SqlTypeName.YEAR);
        addColumnMeta(columnMetas, "", "c_year_4", SqlTypeName.YEAR);
        addColumnMeta(columnMetas, "", "c_char", SqlTypeName.CHAR, 10);
        addColumnMeta(columnMetas, "", "c_varchar", SqlTypeName.VARCHAR, 10);
        addColumnMeta(columnMetas, "", "c_binary", SqlTypeName.BINARY, 10);
        addColumnMeta(columnMetas, "", "c_varbinary", SqlTypeName.VARBINARY, 10);
        addColumnMeta(columnMetas, "", "c_blob_tiny", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_blob", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_blob_medium", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_blob_long", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_text_tiny", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_text", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_text_medium", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_text_long", SqlTypeName.BLOB);
        addColumnMeta(columnMetas, "", "c_enum", buildEnumType(factory));
        addColumnMeta(columnMetas, "", "c_set", buildSetType(factory));
        addColumnMeta(columnMetas, "", "c_json", SqlTypeName.JSON);
        addColumnMeta(columnMetas, "", "c_geometry", SqlTypeName.GEOMETRY);
        addColumnMeta(columnMetas, "", "c_point", SqlTypeName.BINARY);
        addColumnMeta(columnMetas, "", "c_linestring", SqlTypeName.BINARY);
        addColumnMeta(columnMetas, "", "c_polygon", SqlTypeName.BINARY);
        addColumnMeta(columnMetas, "", "c_multipoint", SqlTypeName.BINARY);
        addColumnMeta(columnMetas, "", "c_multilinestring", SqlTypeName.BINARY);
        addColumnMeta(columnMetas, "", "c_multipolygon", SqlTypeName.BINARY);
        addColumnMeta(columnMetas, "", "c_geometrycollection", SqlTypeName.BINARY);

        FULL_TYPE_FILE_META.initColumnMetas(ColumnarStoreUtils.IMPLICIT_COLUMN_CNT, columnMetas);
    }

    private static RelDataType buildEnumType(RelDataTypeFactory factory) {
        EnumType enumType = parseEnumType("enum('a','b','c')");
        final ImmutableList.Builder<String> builder = ImmutableList.builder();

        final Set<String> strings = enumType.getEnumValues().keySet();
        for (String enumValue : strings) {
            builder.add(enumValue);
        }

        final ImmutableList<String> build = builder.build();
        return factory.createEnumSqlType(SqlTypeName.ENUM, build);
    }

    private static RelDataType buildSetType(RelDataTypeFactory factory) {
        SetType setType = parseSetType("set('a','b','c')");
        return factory.createSetSqlType(SqlTypeName.CHAR, (int) Long.min(1, Integer.MAX_VALUE),
            setType.getSetValues());
    }

    private static void addColumnMeta(List<ColumnMeta> columnMetas, String tableName, String columnName,
                                      SqlTypeName sqlTypeName) {
        columnMetas.add(new ColumnMeta(tableName, columnName, null,
            new Field(TYPE_FACTORY.createSqlType(sqlTypeName))));
    }

    private static void addColumnMeta(List<ColumnMeta> columnMetas, String tableName, String columnName,
                                      SqlTypeName sqlTypeName, int precision) {
        columnMetas.add(new ColumnMeta(tableName, columnName, null,
            new Field(TYPE_FACTORY.createSqlType(sqlTypeName, precision))));
    }

    private static void addColumnMeta(List<ColumnMeta> columnMetas, String tableName, String columnName,
                                      SqlTypeName sqlTypeName, int precision, int scale) {
        columnMetas.add(new ColumnMeta(tableName, columnName, null,
            new Field(TYPE_FACTORY.createSqlType(sqlTypeName, precision, scale))));
    }

    private static void addColumnMeta(List<ColumnMeta> columnMetas, String tableName, String columnName,
                                      RelDataType relDataType) {
        columnMetas.add(new ColumnMeta(tableName, columnName, null, new Field(relDataType)));
    }

    @Before
    public void setUp() {
        when(columnarManager.fileMetaOf(anyString())).thenReturn(FILE_META);
        when(columnarManager.fileNameOf(anyString(), anyLong(), anyString(), anyInt())).thenReturn(
            Optional.of(DATA_FILE_NAME));
        Map<String, List<Pair<String, Long>>> allDelPositions = new HashMap<>();
        allDelPositions.put(PARTITION_NAME, Collections.singletonList(new Pair<>(DEL_FILE_NAME, FILE_LENGTH)));
        List<ColumnMeta> columns =
            Collections.singletonList(new ColumnMeta("t1", "pk", "pk", new Field(DataTypes.LongType)));
        List<RexNode> rexList = Collections.emptyList(); // Mocked or real instance as needed
        Map<Integer, ParameterContext> params = new HashMap<>();
        double groupsRatio = 0.5;
        double deletionRatio = 0.5;

        flashbackScanPreProcessor = new FlashbackScanPreProcessor(configuration,
            fileSystem, SCHEMA_NAME, LOGICAL_TABLE_NAME, true, true, columns,
            rexList, params, groupsRatio, deletionRatio, columnarManager, TSO + 1,
            Collections.singletonList(1L), allDelPositions);

        defaultScanPreProcessor = new DefaultScanPreProcessor(configuration,
            fileSystem, SCHEMA_NAME, LOGICAL_TABLE_NAME, true, true, columns,
            rexList, params, groupsRatio, deletionRatio, columnarManager, TSO, Collections.singletonList(1L));

        columnarManagerMockedStatic = Mockito.mockStatic(ColumnarManager.class);
        columnarManagerMockedStatic.when(ColumnarManager::getInstance).thenReturn(columnarManager);

        mockFsUtils = Mockito.mockStatic(FileSystemUtils.class);
        mockFsUtils.when(() -> FileSystemUtils.fileExists(anyString(), any(Engine.class), anyBoolean()))
            .thenReturn(true);
        mockFsUtils.when(
            () -> FileSystemUtils.readFile(anyString(), anyInt(), anyInt(), any(byte[].class), any(Engine.class),
                anyBoolean())
        ).thenAnswer(mockFileReadAnswer);
        mockFsUtils.when(
            () -> FileSystemUtils.openStreamFileWithBuffer(anyString(), any(Engine.class), anyBoolean())
        ).thenAnswer(mockOpenFileAnswer);
        mockFsUtils.when(
            () -> FileSystemUtils.readFullyFile(anyString(), any(Engine.class), anyBoolean())
        ).thenAnswer(mockReadFullyAnswer);
    }

    @BeforeClass
    public static void beforeClass() {
        prepareFileSystem();
    }

    @After
    public void tearDown() {
        if (columnarManagerMockedStatic != null) {
            columnarManagerMockedStatic.close();
        }
        if (mockFsUtils != null) {
            mockFsUtils.close();
        }
    }

    protected static final Answer<Void> mockFileReadAnswer = invocation -> {
        Object[] args = invocation.getArguments();
        String fileName = (String) args[0];
        int offset = (Integer) args[1];
        int length = (Integer) args[2];
        byte[] output = (byte[]) args[3];

        readMockFile(fileName, offset, length, output);
        return null;
    };

    protected static final Answer<FSDataInputStream> mockOpenFileAnswer = invocation -> {
        Object[] args = invocation.getArguments();
        String fileName = (String) args[0];

        return openMockFile(fileName);
    };

    protected static final Answer<byte[]> mockReadFullyAnswer = invocation -> {
        Object[] args = invocation.getArguments();
        String fileName = (String) args[0];

        return readMockFile(fileName);
    };
}
