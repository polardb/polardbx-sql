package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ZONEMAP_MAX_GROUP_SIZE;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.FILESYSTEM;
import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.prepareFileSystem;
import static com.alibaba.polardbx.executor.operator.scan.ScanTestBase.getFileFromClasspath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiVersionColumnarSchemaTest {
    private MultiVersionColumnarSchema multiVersionColumnarSchema;

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedConstruction<ColumnarTableEvolutionAccessor> tableEvolutionAccessorMockedConstruction;
    private MockedConstruction<ColumnarColumnEvolutionAccessor> columnEvolutionAccessorMockedConstruction;
    private MockedConstruction<ColumnarTableMappingAccessor> tableMappingAccessorMockedConstruction;
    private MockedConstruction<ColumnarPartitionEvolutionAccessor> partitionEvolutionAccessorMockedConstruction;
    private MockedConstruction<PartitionGroupAccessor> partitionGroupAccessorMockedConstruction;

    private static final Long TABLE_ID = 1L;
    private static final int DDL_ROUND_COUNT = 15;
    private static final List<Long> SOLID_FIELD_IDS = Arrays.asList(1L, 2L, 3L, 4L);

    private static final ColumnarTableEvolutionRecord[] TABLE_EVOLUTION_RECORDS = {
        new ColumnarTableEvolutionRecord(7193543211267129408L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608426958393344L, "CREATE_INDEX", 7193543213288783936L, Arrays.asList(1L, 2L, 3L, 4L),
            Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543479677419648L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608695356104704L, "ALTER_TABLE_ADD_COLUMN", 7193543481715851328L,
            Arrays.asList(1L, 2L, 3L, 4L, 5L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543486010818688L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608701685309440L, "ALTER_TABLE_ADD_COLUMN", 7193543488070221888L,
            Arrays.asList(6L, 7L, 8L, 9L, 10L, 11L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543493531205696L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608709188919296L, "ALTER_TABLE_ADD_COLUMN", 7193543495498334272L,
            Arrays.asList(6L, 12L, 13L, 14L, 15L, 16L, 17L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543500346949760L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608716004663296L, "ALTER_TABLE_DROP_COLUMN", 7193543504839049280L,
            Arrays.asList(6L, 12L, 13L, 14L, 15L, 16L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543510081929280L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608725844500480L, "ALTER_TABLE_DROP_COLUMN", 7193543513747751040L,
            Arrays.asList(20L, 21L, 22L, 23L, 24L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543518810275968L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608734459600896L, "ALTER_TABLE_MODIFY_COLUMN", 7193543520727072896L,
            Arrays.asList(20L, 21L, 25L, 26L, 27L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543524187373632L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608739840892928L, "ALTER_TABLE_MODIFY_COLUMN", 7193543525730877504L,
            Arrays.asList(20L, 21L, 28L, 29L, 30L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543530847928512L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608746514030592L, "ALTER_TABLE_MODIFY_COLUMN", 7193543532404015168L,
            Arrays.asList(31L, 32L, 33L, 34L, 35L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543535960785024L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608751610109952L, "ALTER_TABLE_MODIFY_COLUMN", 7193543537814667328L,
            Arrays.asList(36L, 37L, 38L, 39L, 40L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543542566813760L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608758224527360L, "ALTER_TABLE_CHANGE_COLUMN", 7193543544571691072L,
            Arrays.asList(41L, 42L, 43L, 44L, 45L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543547780333632L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608763429658624L, "ALTER_TABLE_CHANGE_COLUMN", 7193543549244145728L,
            Arrays.asList(46L, 47L, 48L, 49L, 50L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543553971126336L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608769637228544L, "ALTER_TABLE_ADD_COLUMN", 7193543555581739072L,
            Arrays.asList(46L, 47L, 48L, 49L, 50L, 51L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543559344029760L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608774997549056L, "ALTER_TABLE_DROP_COLUMN", 7193543563106320448L,
            Arrays.asList(46L, 53L, 54L, 55L, 56L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543569020289088L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608784665419776L, "ALTER_TABLE_DROP_COLUMN", 7193543573315256384L,
            Arrays.asList(46L, 53L, 54L, 55L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543578759462976L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608794408787968L, "ALTER_TABLE_MODIFY_COLUMN", 7193543580323938560L,
            Arrays.asList(46L, 58L, 59L, 60L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543583968788544L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608799722971136L, "ALTER_TABLE_ADD_COLUMN", 7193543585772339264L,
            Arrays.asList(46L, 58L, 59L, 60L, 61L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543589274583104L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608804953268224L, "ALTER_TABLE_ADD_COLUMN", 7193543590679675008L,
            Arrays.asList(62L, 63L, 64L, 65L, 66L, 67L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543593737322688L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608809399230464L, "ALTER_TABLE_ADD_COLUMN", 7193543595851251776L,
            Arrays.asList(62L, 68L, 69L, 70L, 71L, 72L, 73L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543599181529216L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608814826659840L, "ALTER_TABLE_DROP_COLUMN", 7193543602138513472L,
            Arrays.asList(62L, 68L, 69L, 70L, 71L, 72L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543605280047168L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608820925177856L, "ALTER_TABLE_DROP_COLUMN", 7193543608513855552L,
            Arrays.asList(76L, 77L, 78L, 79L, 80L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543611466645568L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608827115970560L, "ALTER_TABLE_MODIFY_COLUMN", 7193543613047898176L,
            Arrays.asList(76L, 77L, 81L, 82L, 83L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543616805994560L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608832455319552L, "ALTER_TABLE_MODIFY_COLUMN", 7193543618370470016L,
            Arrays.asList(76L, 77L, 84L, 85L, 86L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543622447333504L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608838084075520L, "ALTER_TABLE_MODIFY_COLUMN", 7193543624234106944L,
            Arrays.asList(87L, 88L, 89L, 90L, 91L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543627287560320L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608842928496640L, "ALTER_TABLE_MODIFY_COLUMN", 7193543643221721152L,
            Arrays.asList(92L, 93L, 94L, 95L, 96L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543645553754176L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608861198884864L, "ALTER_TABLE_CHANGE_COLUMN", 7193543646430363712L,
            Arrays.asList(97L, 98L, 99L, 100L, 101L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543648217137216L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608863866462208L, "ALTER_TABLE_CHANGE_COLUMN", 7193543649295073344L,
            Arrays.asList(102L, 103L, 104L, 105L, 106L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543650888908864L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608866550816768L, "ALTER_TABLE_ADD_COLUMN", 7193543651815850048L,
            Arrays.asList(102L, 103L, 104L, 105L, 106L, 107L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543653569069120L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608869222588416L, "ALTER_TABLE_DROP_COLUMN", 7193543655351648320L,
            Arrays.asList(102L, 109L, 110L, 111L, 112L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543656660271168L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608872305401856L, "ALTER_TABLE_DROP_COLUMN", 7193543658417684544L,
            Arrays.asList(102L, 109L, 110L, 111L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543660166709312L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608875816034304L, "ALTER_TABLE_MODIFY_COLUMN", 7193543661244645440L,
            Arrays.asList(102L, 114L, 115L, 116L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543662834286656L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608878487805952L, "ALTER_TABLE_ADD_COLUMN", 7193543663752839232L,
            Arrays.asList(102L, 114L, 115L, 116L, 117L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543665086627904L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608880744341504L, "ALTER_TABLE_ADD_COLUMN", 7193543666227478592L,
            Arrays.asList(118L, 119L, 120L, 121L, 122L, 123L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543667762593856L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608883416113152L, "ALTER_TABLE_ADD_COLUMN", 7193543668916027456L,
            Arrays.asList(118L, 124L, 125L, 126L, 127L, 128L, 129L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543670430171200L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608886079496192L, "ALTER_TABLE_DROP_COLUMN", 7193543672191778880L,
            Arrays.asList(118L, 124L, 125L, 126L, 127L, 128L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543673936609344L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608889585934336L, "ALTER_TABLE_DROP_COLUMN", 7193543675698217024L,
            Arrays.asList(132L, 133L, 134L, 135L, 136L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543677023617088L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608892664553472L, "ALTER_TABLE_MODIFY_COLUMN", 7193543678097358912L,
            Arrays.asList(132L, 133L, 137L, 138L, 139L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543679687000128L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608895336325120L, "ALTER_TABLE_MODIFY_COLUMN", 7193543680764936256L,
            Arrays.asList(132L, 133L, 140L, 141L, 142L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543682354577472L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608897995513856L, "ALTER_TABLE_MODIFY_COLUMN", 7193543683424124992L,
            Arrays.asList(143L, 144L, 145L, 146L, 147L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543685017960512L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608900663091200L, "ALTER_TABLE_MODIFY_COLUMN", 7193543686091702336L,
            Arrays.asList(148L, 149L, 150L, 151L, 152L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543687677149248L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608903322279936L, "ALTER_TABLE_CHANGE_COLUMN", 7193543688536981568L,
            Arrays.asList(153L, 154L, 155L, 156L, 157L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543690336337984L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608905989857280L, "ALTER_TABLE_CHANGE_COLUMN", 7193543691418468416L,
            Arrays.asList(158L, 159L, 160L, 161L, 162L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543693008109632L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608908661628928L, "ALTER_TABLE_ADD_COLUMN", 7193543693922467904L,
            Arrays.asList(158L, 159L, 160L, 161L, 162L, 163L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543695675686976L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608911320817664L, "ALTER_TABLE_DROP_COLUMN", 7193543697441488960L,
            Arrays.asList(158L, 165L, 166L, 167L, 168L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543699177930816L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608914823061504L, "ALTER_TABLE_DROP_COLUMN", 7193543700931149888L,
            Arrays.asList(158L, 165L, 166L, 167L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543702260744256L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608917905874944L, "ALTER_TABLE_MODIFY_COLUMN", 7193543703330291776L,
            Arrays.asList(158L, 170L, 171L, 172L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543704919932992L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608920590229504L, "ALTER_TABLE_ADD_COLUMN", 7193543705846874176L,
            Arrays.asList(158L, 170L, 171L, 172L, 173L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543707604287552L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608923266195456L, "ALTER_TABLE_ADD_COLUMN", 7193543708736749632L,
            Arrays.asList(174L, 175L, 176L, 177L, 178L, 179L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543710284447808L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608925946355712L, "ALTER_TABLE_ADD_COLUMN", 7193543711416909888L,
            Arrays.asList(174L, 180L, 181L, 182L, 183L, 184L, 185L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543712964608064L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608928601350144L, "ALTER_TABLE_DROP_COLUMN", 7193543714709438528L,
            Arrays.asList(174L, 180L, 181L, 182L, 183L, 184L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543716454268992L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608932086816768L, "ALTER_TABLE_DROP_COLUMN", 7193543718194905152L,
            Arrays.asList(188L, 189L, 190L, 191L, 192L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543719939735616L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608935589060608L, "ALTER_TABLE_MODIFY_COLUMN", 7193543721013477440L,
            Arrays.asList(188L, 189L, 193L, 194L, 195L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543722607312960L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608938248249344L, "ALTER_TABLE_MODIFY_COLUMN", 7193543723681054784L,
            Arrays.asList(188L, 189L, 196L, 197L, 198L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543725681737792L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608941326868480L, "ALTER_TABLE_MODIFY_COLUMN", 7193543726742896704L,
            Arrays.asList(199L, 200L, 201L, 202L, 203L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543728336732224L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608943977668608L, "ALTER_TABLE_MODIFY_COLUMN", 7193543729397891136L,
            Arrays.asList(204L, 205L, 206L, 207L, 208L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543730991726656L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608946645245952L, "ALTER_TABLE_CHANGE_COLUMN", 7193543731859947584L,
            Arrays.asList(209L, 210L, 211L, 212L, 213L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543733667692608L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608949312823296L, "ALTER_TABLE_CHANGE_COLUMN", 7193543734728851520L,
            Arrays.asList(214L, 215L, 216L, 217L, 218L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543736326881344L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608951980400640L, "ALTER_TABLE_ADD_COLUMN", 7193543737237045312L,
            Arrays.asList(214L, 215L, 216L, 217L, 218L, 219L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543739002847296L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608954647977984L, "ALTER_TABLE_DROP_COLUMN", 7193543740764454976L,
            Arrays.asList(214L, 221L, 222L, 223L, 224L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543742509285440L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608958154416128L, "ALTER_TABLE_DROP_COLUMN", 7193543744262504512L,
            Arrays.asList(214L, 221L, 222L, 223L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543746003140672L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608961648271360L, "ALTER_TABLE_MODIFY_COLUMN", 7193543747076882496L,
            Arrays.asList(214L, 226L, 227L, 228L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543748666523712L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608964320043008L, "ALTER_TABLE_ADD_COLUMN", 7193543749576687680L,
            Arrays.asList(214L, 226L, 227L, 228L, 229L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543751338295360L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608966996008960L, "ALTER_TABLE_ADD_COLUMN", 7193543752474951744L,
            Arrays.asList(230L, 231L, 232L, 233L, 234L, 235L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543754010067008L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608969671974912L, "ALTER_TABLE_ADD_COLUMN", 7193543755150917696L,
            Arrays.asList(230L, 236L, 237L, 238L, 239L, 240L, 241L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543756694421568L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608972343746560L, "ALTER_TABLE_DROP_COLUMN", 7193543758451834944L,
            Arrays.asList(230L, 236L, 237L, 238L, 239L, 240L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543760205054016L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608975837601792L, "ALTER_TABLE_DROP_COLUMN", 7193543761937301568L,
            Arrays.asList(244L, 245L, 246L, 247L, 248L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543763694714944L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608979339845632L, "ALTER_TABLE_MODIFY_COLUMN", 7193543764764262464L,
            Arrays.asList(244L, 245L, 249L, 250L, 251L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543766349709376L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608981994840064L, "ALTER_TABLE_MODIFY_COLUMN", 7193543767419256896L,
            Arrays.asList(244L, 245L, 252L, 253L, 254L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543769013092416L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608984649834496L, "ALTER_TABLE_MODIFY_COLUMN", 7193543770074251328L,
            Arrays.asList(255L, 256L, 257L, 258L, 259L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543771668086848L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608987329994752L, "ALTER_TABLE_MODIFY_COLUMN", 7193543772758605888L,
            Arrays.asList(260L, 261L, 262L, 263L, 264L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543774344052800L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608989989183488L, "ALTER_TABLE_CHANGE_COLUMN", 7193543775199690816L,
            Arrays.asList(265L, 266L, 267L, 268L, 269L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543777003241536L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608992648372224L, "ALTER_TABLE_CHANGE_COLUMN", 7193543778076983360L,
            Arrays.asList(270L, 271L, 272L, 273L, 274L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543779666624576L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608995336921088L, "ALTER_TABLE_ADD_COLUMN", 7193543780610342976L,
            Arrays.asList(270L, 271L, 272L, 273L, 274L, 275L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543781931548736L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724608997580873728L, "ALTER_TABLE_DROP_COLUMN", 7193543783688962112L,
            Arrays.asList(270L, 277L, 278L, 279L, 280L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543785429598272L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724609001074728960L, "ALTER_TABLE_DROP_COLUMN", 7193543787195400256L,
            Arrays.asList(270L, 277L, 278L, 279L), Arrays.asList(1L, 2L, 3L, 4L)),
        new ColumnarTableEvolutionRecord(7193543788931842112L, 1L, "transfer", "accounts", "accounts_col_index_$c9a1",
            null, 1724609004572778496L, "ALTER_TABLE_MODIFY_COLUMN", 7193543789993001024L,
            Arrays.asList(270L, 282L, 283L, 284L), Arrays.asList(1L, 2L, 3L, 4L)),

    };

    private static final ColumnarColumnEvolutionRecord[] COLUMN_EVOLUTION_RECORDS;

    private static final ColumnarPartitionEvolutionRecord[] COLUMNAR_PARTITION_EVOLUTION_RECORDS = {
        new ColumnarPartitionEvolutionRecord(1L, 1L, "", 7193543211267129408L, 1724608426958393344L,
            ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 10, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": -1, \"partDesc\": \"\", \"partExpr\": \"\", \"partName\": \"\", \"phyTable\": \"\", \"nextLevel\": 1, \"partFlags\": 0, \"partLevel\": 0, \"tableName\": \"accounts_col_index_$c9a1\", \"partEngine\": \"\", \"partExtras\": \"{\\\"charset\\\":\\\"utf8\\\",\\\"locality\\\":\\\"\\\",\\\"partitionPattern\\\":\\\"accounts_col_index_$c9a1_2zTE\\\",\\\"timeZone\\\":\\\"SYSTEM\\\"}\", \"partMethod\": \"\", \"partStatus\": 1, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"transfer\", \"partPosition\": -1, \"partTempName\": \"\"}"),
            1),
        new ColumnarPartitionEvolutionRecord(1L, 2L, "p0", 7193543211267129408L, 1724608426958393344L,
            ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 29, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": 44, \"partDesc\": \"10000\", \"partExpr\": \"`id`\", \"partName\": \"p0\", \"phyTable\": \"accounts_col_index_$c9a1_2zTE_00000\", \"nextLevel\": -1, \"partFlags\": 0, \"partLevel\": 1, \"tableName\": \"accounts_col_index_$c9a1\", \"partEngine\": \"Columnar\", \"partExtras\": \"{\\\"locality\\\":\\\"\\\"}\", \"partMethod\": \"RANGE\", \"partStatus\": 0, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"transfer\", \"partPosition\": 1, \"partTempName\": \"\"}"),
            1),
        new ColumnarPartitionEvolutionRecord(1L, 3L, "p1", 7193543211267129408L, 1724608426958393344L,
            ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 30, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": 44, \"partDesc\": \"20000\", \"partExpr\": \"`id`\", \"partName\": \"p1\", \"phyTable\": \"accounts_col_index_$c9a1_2zTE_00001\", \"nextLevel\": -1, \"partFlags\": 0, \"partLevel\": 1, \"tableName\": \"accounts_col_index_$c9a1\", \"partEngine\": \"Columnar\", \"partExtras\": \"{\\\"locality\\\":\\\"\\\"}\", \"partMethod\": \"RANGE\", \"partStatus\": 0, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"transfer\", \"partPosition\": 2, \"partTempName\": \"\"}"),
            1),
        new ColumnarPartitionEvolutionRecord(1L, 4L, "p2", 7193543211267129408L, 1724608426958393344L,
            ColumnarPartitionEvolutionRecord.deserializeFromJson(
                "{\"groupId\": 31, \"tblType\": 7, \"autoFlag\": 0, \"parentId\": 44, \"partDesc\": \"30000\", \"partExpr\": \"`id`\", \"partName\": \"p2\", \"phyTable\": \"accounts_col_index_$c9a1_2zTE_00002\", \"nextLevel\": -1, \"partFlags\": 0, \"partLevel\": 1, \"tableName\": \"accounts_col_index_$c9a1\", \"partEngine\": \"Columnar\", \"partExtras\": \"{\\\"locality\\\":\\\"\\\"}\", \"partMethod\": \"RANGE\", \"partStatus\": 0, \"spTempFlag\": -1, \"metaVersion\": 1, \"partComment\": \"\", \"tableSchema\": \"transfer\", \"partPosition\": 3, \"partTempName\": \"\"}"),
            1),
    };

    public static final PartitionGroupRecord[] PARTITION_GROUP_RECORDS = {
        new PartitionGroupRecord(),
        new PartitionGroupRecord(),
        new PartitionGroupRecord(),
    };

    static {
        Yaml yaml = new Yaml(new Constructor((List.class)));
        List<List<Object>> recordObjectList =
            yaml.load(MultiVersionColumnarSchemaTest.class.getResourceAsStream("columnar_column_evolution.yml"));

        COLUMN_EVOLUTION_RECORDS = new ColumnarColumnEvolutionRecord[recordObjectList.size()];
        for (int i = 0; i < recordObjectList.size(); i++) {
            List<Object> recordObject = recordObjectList.get(i);
            COLUMN_EVOLUTION_RECORDS[i] = new ColumnarColumnEvolutionRecord(
                ((Integer) recordObject.get(0)).longValue(),
                ((Integer) recordObject.get(1)).longValue(),
                (String) recordObject.get(2),
                (Long) recordObject.get(3),
                (Long) recordObject.get(4),
                ColumnarColumnEvolutionRecord.deserializeFromJson((String) recordObject.get(5))
            );
        }

        for (int i = 0; i < COLUMN_EVOLUTION_RECORDS.length; i++) {
            COLUMN_EVOLUTION_RECORDS[i].id = i + 1;
        }

        for (int i = 0; i < COLUMNAR_PARTITION_EVOLUTION_RECORDS.length; i++) {
            COLUMNAR_PARTITION_EVOLUTION_RECORDS[i].id = i + 1;
        }
    }

    @Before
    public void setUp() {
        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(Mockito.mock(Connection.class));
        mockMetaDbUtil.when(() -> MetaDbUtil.queryMetaDbWrapper(Mockito.any(), Mockito.any())).thenCallRealMethod();
        multiVersionColumnarSchema = new MultiVersionColumnarSchema(null);
        tableEvolutionAccessorMockedConstruction = Mockito.mockConstruction(
            ColumnarTableEvolutionAccessor.class,
            (mock, context) -> {
                Mockito.when(mock.queryTableIdAndGreaterThanTso(Mockito.anyLong(), Mockito.anyLong())).thenAnswer(
                    invocation -> Arrays.asList(TABLE_EVOLUTION_RECORDS)
                );
            });
        columnEvolutionAccessorMockedConstruction = Mockito.mockConstruction(
            ColumnarColumnEvolutionAccessor.class,
            (mock, context) -> {
                Mockito.when(mock.queryTableIdAndVersionIdsOrderById(Mockito.anyLong(), Mockito.anyList())).thenAnswer(
                    invocation -> Arrays.asList(COLUMN_EVOLUTION_RECORDS)
                );
            });
        tableMappingAccessorMockedConstruction = Mockito.mockConstruction(
            ColumnarTableMappingAccessor.class,
            (mock, context) -> {
                List<ColumnarTableMappingRecord> records = new ArrayList<>();
                records.add(new ColumnarTableMappingRecord("transfer", "accounts", "accounts_col_index_$c9a1",
                    7193543788931842112L, "PUBLIC"));
                records.get(0).tableId = 1L;
                Mockito.when(mock.querySchemaIndex(anyString(), anyString())).thenReturn(records);
            });
        partitionEvolutionAccessorMockedConstruction = Mockito.mockConstruction(
            ColumnarPartitionEvolutionAccessor.class,
            (mock, context) -> {
                Mockito.when(mock.queryTableIdAndVersionIdsOrderById(Mockito.anyLong(), Mockito.anyList()))
                    .thenReturn(Arrays.asList(COLUMNAR_PARTITION_EVOLUTION_RECORDS));
            });
        partitionGroupAccessorMockedConstruction = Mockito.mockConstruction(
            PartitionGroupAccessor.class,
            (mock, context) -> {
                Mockito.when(mock.getPartitionGroupsByTableGroupId(Mockito.anyLong(), Mockito.anyBoolean()))
                    .thenReturn(Arrays.asList(PARTITION_GROUP_RECORDS));
            });
    }

    @After
    public void tearDown() throws Exception {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }

        if (tableEvolutionAccessorMockedConstruction != null) {
            tableEvolutionAccessorMockedConstruction.close();
        }

        if (columnEvolutionAccessorMockedConstruction != null) {
            columnEvolutionAccessorMockedConstruction.close();
        }

        if (tableMappingAccessorMockedConstruction != null) {
            tableMappingAccessorMockedConstruction.close();
        }

        if (partitionEvolutionAccessorMockedConstruction != null) {
            partitionEvolutionAccessorMockedConstruction.close();
        }

        if (partitionGroupAccessorMockedConstruction != null) {
            partitionGroupAccessorMockedConstruction.close();
        }
    }

    @Test
    public void getSortKeyColumns() throws ExecutionException {
        multiVersionColumnarSchema.getSortKeyColumns(0, "transfer", "accounts");
    }

    @Test
    public void getTableId() throws ExecutionException {
        assertEquals(1L, multiVersionColumnarSchema.getTableId(0, "transfer", "accounts").longValue());
    }

    @Test
    public void getColumnMetas() {
        for (int i = 0; i < DDL_ROUND_COUNT; i++) {
            List<ColumnMeta> pivotMetas =
                multiVersionColumnarSchema.getColumnMetas(TABLE_EVOLUTION_RECORDS[i].commitTs, TABLE_ID);
            for (int j = 1; i + j * DDL_ROUND_COUNT < TABLE_EVOLUTION_RECORDS.length; j++) {
                List<ColumnMeta> currentMetas =
                    multiVersionColumnarSchema.getColumnMetas(TABLE_EVOLUTION_RECORDS[i + j * DDL_ROUND_COUNT].commitTs,
                        TABLE_ID);
                assertEquals(currentMetas.size(), pivotMetas.size());
                for (int k = 0; k < currentMetas.size(); k++) {
                    assertEquals(currentMetas.get(k).getFullName(), pivotMetas.get(k).getFullName());
                    assertSame(currentMetas.get(k).getDataType().getClass(),
                        pivotMetas.get(k).getDataType().getClass());
                    assertEquals(currentMetas.get(k).getLength(), pivotMetas.get(k).getLength());
                    assertEquals(currentMetas.get(k).getStatus(), pivotMetas.get(k).getStatus());
                    assertEquals(currentMetas.get(k).isFillDefault(), currentMetas.get(k).isFillDefault());
                    assertEquals(currentMetas.get(k).getOriginColumnName(), pivotMetas.get(k).getOriginColumnName());
                    assertEquals(currentMetas.get(k).getOriginTableName(), pivotMetas.get(k).getOriginTableName());
                    assertEquals(currentMetas.get(k).getTableName(), pivotMetas.get(k).getTableName());
                    assertEquals(currentMetas.get(k).getMappingName(), pivotMetas.get(k).getMappingName());
                }
            }
        }
    }

    @Test
    public void getColumnIndexMap() {
        for (ColumnarTableEvolutionRecord record : TABLE_EVOLUTION_RECORDS) {
            long schemaTs = record.commitTs;
            Map<Long, Integer> result = multiVersionColumnarSchema.getColumnIndexMap(schemaTs, TABLE_ID);
            for (Long fieldId : SOLID_FIELD_IDS) {
                assertTrue(result.containsKey(fieldId));
            }
        }
    }

    @Test
    public void testGetColumnFieldIdList() {
        // For any schema, it must contain this four columns: 1, 2, 3, 4
        for (ColumnarTableEvolutionRecord record : TABLE_EVOLUTION_RECORDS) {
            long versionId = record.versionId;
            List<Long> result = multiVersionColumnarSchema.getColumnFieldIdList(versionId, TABLE_ID);
            assertTrue(result.containsAll(SOLID_FIELD_IDS));
        }
    }

    @Test
    public void getInitColumnMeta() {
        for (Long fieldId : SOLID_FIELD_IDS) {
            ColumnMetaWithTs result = multiVersionColumnarSchema.getInitColumnMeta(TABLE_ID, fieldId);
            ColumnMeta meta = result.getMeta();
            ColumnarColumnEvolutionRecord record = COLUMN_EVOLUTION_RECORDS[(int) (fieldId - 1)];
            assertEquals(meta.getFullName(), record.columnsRecord.tableName + "." + record.columnName);
        }
    }

    @Test
    public void getPrimaryKeyColumns() {
        for (ColumnarTableEvolutionRecord record : TABLE_EVOLUTION_RECORDS) {
            long schemaTs = record.commitTs;
            int[] result = multiVersionColumnarSchema.getPrimaryKeyColumns(schemaTs, TABLE_ID);
            assertEquals(1, result.length);
            List<ColumnMeta> columnMetas = multiVersionColumnarSchema.getColumnMetas(schemaTs, TABLE_ID);
            assertEquals("id", columnMetas.get(result[0] - 1).getName());
        }
    }

    @Test
    public void getPartitionInfos() {
        long schemaTs = TABLE_EVOLUTION_RECORDS[TABLE_EVOLUTION_RECORDS.length - 1].commitTs;
        SortedMap<Long, PartitionInfo> partitionInfos =
            multiVersionColumnarSchema.getPartitionInfos(schemaTs, TABLE_ID);
        assertEquals(1, partitionInfos.size());
        Map.Entry<Long, PartitionInfo> entry = partitionInfos.entrySet().stream().findFirst().get();
        assertEquals(TABLE_EVOLUTION_RECORDS[0].commitTs, entry.getKey().longValue());
        assertEquals(
            "PARTITION BY RANGE(`id`)\n"
                + "(PARTITION `p0` VALUES LESS THAN (10000) ENGINE = Columnar,\n"
                + " PARTITION `p1` VALUES LESS THAN (20000) ENGINE = Columnar,\n"
                + " PARTITION `p2` VALUES LESS THAN (30000) ENGINE = Columnar)",
            entry.getValue().getPartitionBy().toString()
        );
    }

    @Test
    public void purge() {
        // TODO(siyun):
        boolean failed = false;
        try {
            multiVersionColumnarSchema.purge(0);
        } catch (NotSupportException exception) {
            assertEquals(exception.getMessage(),
                ErrorCode.ERR_NOT_SUPPORT.getMessage("purge columnar schema not supported now!"));
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testIndexPruning() throws ExecutionException, InterruptedException {
        try (MockedStatic<InstConfUtil> mockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            mockedStatic.when(() -> InstConfUtil.getInt(ZONEMAP_MAX_GROUP_SIZE)).thenReturn(5000);
            prepareFileSystem();
            final Map<Long, Integer> indexMap =
                multiVersionColumnarSchema.getColumnIndexMap(TABLE_EVOLUTION_RECORDS[0].commitTs, TABLE_ID);
            ColumnarManager columnarManager = Mockito.mock(ColumnarManager.class);
            Mockito.when(columnarManager.getSortKeyColumns(anyLong(), anyString(), anyString())).thenReturn(
                Collections.singletonList(0));
            Mockito.when(columnarManager.getPhysicalColumnIndexes(anyString()))
                .thenReturn(indexMap);
            Path orcPath = new Path(getFileFromClasspath("5a91a2f23573.orc"));
            RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
            RexBuilder rexBuilder = new RexBuilder(factory);
            RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                new RexInputRef(0, factory.createSqlType(SqlTypeName.INTEGER)),
                rexBuilder.makeIntLiteral(50));

            List<RexNode> rexNodes = Lists.newArrayList();
            rexNodes.add(rexNode);
            for (ColumnarTableEvolutionRecord record : TABLE_EVOLUTION_RECORDS) {
                long versionId = record.versionId;
                long commitTs = record.commitTs;
                List<ColumnMeta> columnMetas = multiVersionColumnarSchema.getColumnMetas(commitTs, TABLE_ID);
                ScanPreProcessor preProcessor = new DefaultScanPreProcessor(
                    FILESYSTEM.getConf(), FILESYSTEM, "transfer", "accounts_col_index_$c9a1", true, true,
                    columnMetas.subList(2, columnMetas.size()),
                    rexNodes, null, 0D, 0D, columnarManager,
                    commitTs, multiVersionColumnarSchema.getColumnFieldIdList(versionId, TABLE_ID)
                );

                preProcessor.addFile(orcPath);
                ExecutorService executorService = mock(ExecutorService.class);
                when(executorService.submit(any(Runnable.class))).thenAnswer(
                    invocation -> {
                        Runnable runnable = invocation.getArgument(0);
                        runnable.run();
                        return null;
                    }
                );
                ListenableFuture future =
                    preProcessor.prepare(executorService, "traceId", new ColumnarTracer("default"));
                future.get();
                Assert.assertTrue(preProcessor.isPrepared());
                SortedMap<Integer, boolean[]> result = preProcessor.getPruningResult(orcPath);
                Assert.assertEquals(1, result.size());
                Assert.assertArrayEquals(new boolean[] {true}, result.get(0));
            }
        }
    }
}