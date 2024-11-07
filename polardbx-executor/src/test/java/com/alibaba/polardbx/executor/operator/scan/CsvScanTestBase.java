package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.impl.FlashbackScanPreProcessor;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rex.RexNode;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.FILE_META;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.openMockFile;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.prepareFileSystem;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.readMockFile;
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
    protected FlashbackScanPreProcessor flashbackScanPreProcessor;
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

    protected MockedStatic<ColumnarManager> columnarManagerMockedStatic;
    protected MockedStatic<FileSystemUtils> mockFsUtils;

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
            rexList, params, groupsRatio, deletionRatio, columnarManager, TSO,
            Collections.singletonList(1L), allDelPositions);

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
}
