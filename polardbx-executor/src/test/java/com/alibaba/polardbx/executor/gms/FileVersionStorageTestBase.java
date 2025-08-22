package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.orc.OrcConf;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public abstract class FileVersionStorageTestBase {
    protected final static Random R = new Random();
    protected final static String CSV_FILE_NAME = "6659a663977d.csv";
    protected final static int FIRST_CSV_PART_OVER_1000 = 40;
    protected final static int CSV_FILE_ID = 10;
    protected final static String DEL_FILE_NAME = "fb2604e133e6.del";
    public final static FileMeta FILE_META;
    protected static FileSystem FILESYSTEM;
    protected FileVersionStorage fileVersionStorage;
    protected MockedConstruction<ColumnarAppendedFilesAccessor> mockCafCtor;
    protected MockedConstruction<ColumnarFileMappingAccessor> mockCfmCtor;
    protected MockedStatic<DynamicColumnarManager> mockCm;
    protected MockedStatic<FileSystemUtils> mockFsUtils;
    protected MockedStatic<MetaDbUtil> mockMetaDbUtil;
    protected MockedStatic<FileSystemManager> mockFsManager;

    protected final Answer<Void> mockFileReadAnswer = invocation -> {
        Object[] args = invocation.getArguments();
        String fileName = (String) args[0];
        int offset = (Integer) args[1];
        int length = (Integer) args[2];
        byte[] output = (byte[]) args[3];

        readMockFile(fileName, offset, length, output);
        return null;
    };

    protected final Answer<FSDataInputStream> mockOpenFileAnswer = invocation -> {
        Object[] args = invocation.getArguments();
        String fileName = (String) args[0];

        return openMockFile(fileName);
    };

    public static final MockAppendedFilesStatus[] CSV_STATUSES = {
        new MockAppendedFilesStatus(7182618497799684160L, 0, 1775, 25),
        new MockAppendedFilesStatus(7182618502824460352L, 1775, 1775, 50),
        new MockAppendedFilesStatus(7182618507824070720L, 3550, 1775, 75),
        new MockAppendedFilesStatus(7182618512785932416L, 5325, 1775, 100),
        new MockAppendedFilesStatus(7182618517525495872L, 7100, 1775, 125),
        new MockAppendedFilesStatus(7182618522462191680L, 8875, 1775, 150),
        new MockAppendedFilesStatus(7182618527130452160L, 10650, 1775, 175),
        new MockAppendedFilesStatus(7182618530053881920L, 12425, 1704, 199),
        new MockAppendedFilesStatus(7182618572412158016L, 14129, 1775, 224),
        new MockAppendedFilesStatus(7182618575117484096L, 15904, 1775, 249),
        new MockAppendedFilesStatus(7182618577906696256L, 17679, 1775, 274),
        new MockAppendedFilesStatus(7182618580742045824L, 19454, 1775, 299),
        new MockAppendedFilesStatus(7182618583518675008L, 21229, 1775, 324),
        new MockAppendedFilesStatus(7182618586685374528L, 23004, 1775, 349),
        new MockAppendedFilesStatus(7182618589675913344L, 24779, 1775, 374),
        new MockAppendedFilesStatus(7182618592637091968L, 26554, 1775, 399),
        new MockAppendedFilesStatus(7182618594721661120L, 28329, 1775, 424),
        new MockAppendedFilesStatus(7182618597640896576L, 30104, 1775, 449),
        new MockAppendedFilesStatus(7182618600925036672L, 31879, 1775, 474),
        new MockAppendedFilesStatus(7182618603844272192L, 33654, 1775, 499),
        new MockAppendedFilesStatus(7182618606524432512L, 35429, 1704, 523),
        new MockAppendedFilesStatus(7182618609389142144L, 37133, 1775, 548),
        new MockAppendedFilesStatus(7182618612283211968L, 38908, 1704, 572),
        new MockAppendedFilesStatus(7182618615156310080L, 40612, 1775, 597),
        new MockAppendedFilesStatus(7182618617865830464L, 42387, 1775, 622),
        new MockAppendedFilesStatus(7182618620747317312L, 44162, 1775, 647),
        new MockAppendedFilesStatus(7182618623591055424L, 45937, 1775, 672),
        new MockAppendedFilesStatus(7182618626489319488L, 47712, 1775, 697),
        new MockAppendedFilesStatus(7182618629186257024L, 49487, 1775, 722),
        new MockAppendedFilesStatus(7182618632000634944L, 51262, 1775, 747),
        new MockAppendedFilesStatus(7182618634882121856L, 53037, 1775, 772),
        new MockAppendedFilesStatus(7182618637763608640L, 54812, 1775, 797),
        new MockAppendedFilesStatus(7182618640464740416L, 56587, 1775, 822),
        new MockAppendedFilesStatus(7182618643321061568L, 58362, 1704, 846),
        new MockAppendedFilesStatus(7182618646198353984L, 60066, 1775, 871),
        new MockAppendedFilesStatus(7182618648895291520L, 61841, 1775, 896),
        new MockAppendedFilesStatus(7182618651734835264L, 63616, 1775, 921),
        new MockAppendedFilesStatus(7182618654570184768L, 65391, 1775, 946),
        new MockAppendedFilesStatus(7182618657254539584L, 67166, 1775, 971),
        new MockAppendedFilesStatus(7182618660060528704L, 68941, 1704, 995),
        new MockAppendedFilesStatus(7182618662749077696L, 70645, 1775, 1020),
        new MockAppendedFilesStatus(7182618665571844160L, 72420, 1775, 1045),
        new MockAppendedFilesStatus(7182618668402999360L, 74195, 1704, 1069),
        new MockAppendedFilesStatus(7182618671091548288L, 75899, 1775, 1094),
        new MockAppendedFilesStatus(7182618673952063680L, 77674, 1775, 1119),
        new MockAppendedFilesStatus(7182618676816773248L, 79449, 1775, 1144),
        new MockAppendedFilesStatus(7182618679710843008L, 81224, 1775, 1169),
        new MockAppendedFilesStatus(7182618682424557760L, 82999, 1775, 1194),
        new MockAppendedFilesStatus(7182618685276684416L, 84774, 1775, 1219),
        new MockAppendedFilesStatus(7182618688200114304L, 86549, 1775, 1244),
    };

    public static final MockAppendedFilesStatus[] DEL_STATUS = {
        new MockAppendedFilesStatus(7182618497799684164L, 82, 0, 0),
        new MockAppendedFilesStatus(7182618502824460352L, 82, 82, 25),
        new MockAppendedFilesStatus(7182618507824070720L, 164, 82, 50),
        new MockAppendedFilesStatus(7182618512785932416L, 246, 82, 75),
        new MockAppendedFilesStatus(7182618517525495872L, 328, 82, 100),
        new MockAppendedFilesStatus(7182618522462191680L, 410, 82, 125),
        new MockAppendedFilesStatus(7182618527130452160L, 492, 82, 150),
        new MockAppendedFilesStatus(7182618530053881920L, 574, 80, 174),
        new MockAppendedFilesStatus(7182618572412158016L, 654, 82, 199),
        new MockAppendedFilesStatus(7182618575117484096L, 736, 82, 224),
        new MockAppendedFilesStatus(7182618577906696256L, 818, 82, 249),
        new MockAppendedFilesStatus(7182618580742045824L, 900, 82, 274),
        new MockAppendedFilesStatus(7182618583518675008L, 982, 82, 299),
        new MockAppendedFilesStatus(7182618586685374528L, 1064, 82, 324),
        new MockAppendedFilesStatus(7182618589675913344L, 1146, 82, 349),
        new MockAppendedFilesStatus(7182618592637091968L, 1228, 82, 374),
        new MockAppendedFilesStatus(7182618594721661120L, 1310, 82, 399),
        new MockAppendedFilesStatus(7182618597640896576L, 1392, 82, 424),
        new MockAppendedFilesStatus(7182618600925036672L, 1474, 82, 449),
        new MockAppendedFilesStatus(7182618603844272192L, 1556, 82, 474),
        new MockAppendedFilesStatus(7182618606524432512L, 1638, 80, 498),
        new MockAppendedFilesStatus(7182618609389142144L, 1718, 82, 523),
        new MockAppendedFilesStatus(7182618612283211968L, 1800, 80, 547),
        new MockAppendedFilesStatus(7182618615156310080L, 1880, 82, 572),
        new MockAppendedFilesStatus(7182618617865830464L, 1962, 82, 597),
        new MockAppendedFilesStatus(7182618620747317312L, 2044, 82, 622),
        new MockAppendedFilesStatus(7182618623591055424L, 2126, 82, 647),
        new MockAppendedFilesStatus(7182618626489319488L, 2208, 82, 672),
        new MockAppendedFilesStatus(7182618629186257024L, 2290, 82, 697),
        new MockAppendedFilesStatus(7182618632000634944L, 2372, 82, 722),
        new MockAppendedFilesStatus(7182618634882121856L, 2454, 82, 747),
        new MockAppendedFilesStatus(7182618637763608640L, 2536, 82, 772),
        new MockAppendedFilesStatus(7182618640464740416L, 2618, 82, 797),
        new MockAppendedFilesStatus(7182618643321061568L, 2700, 80, 821),
        new MockAppendedFilesStatus(7182618646198353984L, 2780, 82, 846),
        new MockAppendedFilesStatus(7182618648895291520L, 2862, 82, 871),
        new MockAppendedFilesStatus(7182618651734835264L, 2944, 82, 896),
        new MockAppendedFilesStatus(7182618654570184768L, 3026, 82, 921),
        new MockAppendedFilesStatus(7182618657254539584L, 3108, 82, 946),
        new MockAppendedFilesStatus(7182618660060528704L, 3190, 80, 970),
        new MockAppendedFilesStatus(7182618662749077696L, 3270, 82, 995),
        new MockAppendedFilesStatus(7182618665571844160L, 3352, 82, 1020),
        new MockAppendedFilesStatus(7182618668402999360L, 3434, 80, 1044),
        new MockAppendedFilesStatus(7182618671091548288L, 3514, 82, 1069),
        new MockAppendedFilesStatus(7182618673952063680L, 3596, 82, 1094),
        new MockAppendedFilesStatus(7182618676816773248L, 3678, 82, 1119),
        new MockAppendedFilesStatus(7182618679710843008L, 3760, 82, 1144),
        new MockAppendedFilesStatus(7182618682424557760L, 3842, 82, 1169),
        new MockAppendedFilesStatus(7182618685276684416L, 3924, 82, 1194),
        new MockAppendedFilesStatus(7182618688200114304L, 4006, 82, 1219),
    };

    static {
        FILE_META = new FileMeta(
            "", "1", "", "", "p1", CSV_FILE_NAME,
            0, 0,
            7182618497799684160L, 7182618497799684160L, 7182618497799684160L,
            "", "", Engine.LOCAL_DISK, 0L);

        List<ColumnMeta> columnMetas = new ArrayList<>();

        columnMetas.add(new ColumnMeta("", "tso", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));
        columnMetas.add(new ColumnMeta("", "position", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));
        columnMetas.add(new ColumnMeta("", "id", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));
        columnMetas.add(new ColumnMeta("", "balance", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));
        columnMetas.add(new ColumnMeta("", "version", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))));
        columnMetas.add(new ColumnMeta("", "gmt_modified", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME))));

        FILE_META.initColumnMetas(ColumnarStoreUtils.IMPLICIT_COLUMN_CNT, columnMetas);
    }

    @BeforeClass
    public static void prepareFileSystem() {
        Path filePath = new Path(ClassLoader.getSystemResource(CSV_FILE_NAME).getPath());

        Configuration CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.MAX_MERGE_DISTANCE.setLong(CONFIGURATION, 64L * 1024); // 64KB
        OrcConf.USE_ZEROCOPY.setBoolean(CONFIGURATION, true);
        OrcConf.STRIPE_SIZE.setLong(CONFIGURATION, 8L * 1024 * 1024);

        try {
            FILESYSTEM = FileSystem.get(
                filePath.toUri(), CONFIGURATION
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void prepareFileVersionStorage() {
        mockCm = Mockito.mockStatic(DynamicColumnarManager.class);

        DynamicColumnarManager mockDynamicColumnarManager = Mockito.mock(DynamicColumnarManager.class);
        mockCm.when(DynamicColumnarManager::getInstance).thenReturn(mockDynamicColumnarManager);

        Mockito.when(mockDynamicColumnarManager.fileMetaOf(anyString())).thenReturn(FILE_META);
        Mockito.when(
            mockDynamicColumnarManager.delFileNames(anyLong(), anyString(), anyString(), anyString())
        ).thenReturn(
            Collections.singletonList(DEL_FILE_NAME)
        );
        Mockito.when(
            mockDynamicColumnarManager.fileNameOf(anyString(), anyLong(), anyString(), eq(CSV_FILE_ID))
        ).thenReturn(
            Optional.of(CSV_FILE_NAME)
        );
        Mockito.when(mockDynamicColumnarManager.getMinTso()).thenReturn(Long.MIN_VALUE);

        this.fileVersionStorage = new FileVersionStorage(mockDynamicColumnarManager);
        this.fileVersionStorage.open();

        mockFsUtils = Mockito.mockStatic(FileSystemUtils.class);
        mockFsUtils.when(
            () -> FileSystemUtils.fileExists(anyString(), any(Engine.class), anyBoolean())
        ).thenReturn(
            true
        );
        mockFsUtils.when(
            () -> FileSystemUtils.readFile(
                anyString(), anyInt(), anyInt(), any(byte[].class), any(Engine.class), anyBoolean()
            )
        ).thenAnswer(
            mockFileReadAnswer
        );
        mockFsUtils.when(
            () -> FileSystemUtils.openStreamFileWithBuffer(
                anyString(), any(Engine.class), anyBoolean()
            )
        ).thenAnswer(
            mockOpenFileAnswer
        );

        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(null);

        mockCafCtor = Mockito.mockConstruction(ColumnarAppendedFilesAccessor.class, (mock, context) -> {

            /**
             * Mock method for csv
             */
            Mockito.when(
                mock.queryLatestByFileNameBetweenTso(anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long lowerTso = (long) args[1];
                    long upperTso = (long) args[2];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (int i = CSV_STATUSES.length - 1; i >= 0; i--) {
                        MockAppendedFilesStatus appendedFilesStatus = CSV_STATUSES[i];
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                            break;
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            /**
             * Mock method for csv
             */
            Mockito.when(
                mock.queryByFileNameBetweenTso(anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long lowerTso = (long) args[1];
                    long upperTso = (long) args[2];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (MockAppendedFilesStatus appendedFilesStatus : CSV_STATUSES) {
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            /**
             * Mock method for delete bitmap
             */
            Mockito.when(
                mock.queryByFileNameAndMaxTso(anyString(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long tso = (long) args[1];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (int i = DEL_STATUS.length - 1; i >= 0; i--) {
                        MockAppendedFilesStatus appendedFilesStatus = DEL_STATUS[i];
                        if (appendedFilesStatus.checkpointTso <= tso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                            break;
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            /**
             * Mock method for delete bitmap
             */
            Mockito.when(
                mock.queryDelByPartitionBetweenTso(anyString(), anyString(), anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    long lowerTso = (long) args[3];
                    long upperTso = (long) args[4];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (MockAppendedFilesStatus appendedFilesStatus : DEL_STATUS) {
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(DEL_FILE_NAME));
                        }
                    }
                    return appendedFilesRecords;
                }
            );
        });
    }

    @After
    public void clearFileVersionStorage() {
        fileVersionStorage.close();
        if (mockCm != null) {
            mockCm.close();
        }
        if (mockFsUtils != null) {
            mockFsUtils.close();
        }
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
        if (mockCafCtor != null) {
            mockCafCtor.close();
        }
        if (mockFsManager != null) {
            mockFsManager.close();
        }
        if (mockCfmCtor != null) {
            mockCfmCtor.close();
        }
    }

    public static class MockAppendedFilesStatus {
        public long checkpointTso;
        public long appendOffset;
        public long appendLength;
        public long totalRows;

        public MockAppendedFilesStatus(long checkpointTso, long appendOffset, long appendLength, long totalRows) {
            this.checkpointTso = checkpointTso;
            this.appendOffset = appendOffset;
            this.appendLength = appendLength;
            this.totalRows = totalRows;
        }

        public ColumnarAppendedFilesRecord toAppendedFilesRecord(String fileName) {
            ColumnarAppendedFilesRecord columnarAppendedFilesRecord = new ColumnarAppendedFilesRecord();
            columnarAppendedFilesRecord.setFileName(fileName);
            columnarAppendedFilesRecord.setCheckpointTso(checkpointTso);
            columnarAppendedFilesRecord.setAppendOffset(appendOffset);
            columnarAppendedFilesRecord.setAppendLength(appendLength);
            columnarAppendedFilesRecord.setLogicalSchema("");
            columnarAppendedFilesRecord.setLogicalTable("1");
            columnarAppendedFilesRecord.setPartName("p1");
            return columnarAppendedFilesRecord;
        }
    }

    public static void readMockFile(String fileName, int offset, int length, byte[] output) {
        try (InputStream in = FILESYSTEM.open(new Path(ClassLoader.getSystemResource(fileName).getPath()))) {
            ((Seekable) in).seek(offset);
            IOUtils.read(in, output, 0, length);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] readMockFile(String fileName) {
        try (InputStream in = FILESYSTEM.open(new Path(ClassLoader.getSystemResource(fileName).getPath()))) {
            return IOUtils.toByteArray(in);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static FSDataInputStream openMockFile(String fileName) {
        try {
            return FILESYSTEM.open(new Path(ClassLoader.getSystemResource(fileName).getPath()));
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
