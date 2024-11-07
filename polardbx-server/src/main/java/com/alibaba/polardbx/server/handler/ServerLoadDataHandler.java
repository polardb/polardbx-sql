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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.LoadDataHandler;
import com.alibaba.polardbx.net.packet.CommandPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.LoadDataContext;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.utils.LoadDataCacheManager;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.base.Splitter;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.optimizer.context.LoadDataContext.END;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * mysql client need add --local-infile=1
 * CHARACTER SET 'gbk' in load data sql  the charset need ', otherwise the druid will error
 */
public final class ServerLoadDataHandler implements LoadDataHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServerLoadDataHandler.class);

    private static String UPPER_LOAD_DATA = "LOAD DATA";
    private static String LOWER_LOAD_DATA = "load data";
    private static byte[] END_BYTE = new byte[4];

    private ServerConnection serverConnection;
    private String fileName;
    private volatile byte loadPackID = 0;
    private volatile boolean bStart = false;
    private LoadData loadData;
    private String tableName;
    private int skippedEmptyLines = 0;
    private LoadDataResultHandler handler;
    private byte[] restData = new byte[0];

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private HashMap<String, Object> cmdObjects = new HashMap<>();

    private long startTime = System.nanoTime();

    private LoadDataContext dataContext;

    private BlockingQueue<byte[]> cacheData = new LinkedBlockingQueue<>();

    private List<String> lines = new ArrayList<>();

    public ServerLoadDataHandler(ServerConnection serverConnection) {
        this.serverConnection = serverConnection;
        // 创建一个handler，用于向客户端发包
        this.handler = new LoadDataResultHandler();
    }

    @Override
    public boolean isStart() {
        return bStart;
    }

    @Override
    public byte getPacketId() {
        return loadPackID;
    }

    @Override
    public void setPacketId(byte loadDataPacketId) {
        this.loadPackID = loadDataPacketId;
    }

    @Override
    public void putData(byte[] data) {
        try {
            if (dataContext.getThrowable() != null) {
                throw dataContext.getThrowable();
            }
            cacheData.put(data);
            dataContext.getDataCacheManager().allocateMemory(data.length);
            dataContext.getDataCacheManager().settableFuture();
        } catch (Throwable e) {
            String sql = loadData != null ? loadData.getSql() : "";
            handler.handleError(ErrorCode.ERR_HANDLE_DATA, e, sql, false);
        }
    }

    /**
     * 等待run方法结束，并回包给客户端
     */
    @Override
    public void end() {
        try {
            cacheData.put(END_BYTE);
        } catch (Throwable t) {
            String sql = loadData != null ? loadData.getSql() : "";
            handler.handleError(ErrorCode.ERR_HANDLE_DATA, t, sql, false);
        }
    }

    public Object readFileData() {
        File loadDataFile = new File(fileName);
        InputStream is = null;
        try {
            is = new FileInputStream(loadDataFile);
            byte[] data = new byte[2048];
            int len;
            while ((len = is.read(data)) != -1) {
                byte[] dataPacket = new byte[4 + len];
                System.arraycopy(data, 0, dataPacket, 4, len);
                putData(dataPacket);
                if (isFull() && isBlocked()) {
                    //阻塞当前channel
                    logger.warn("Block the channel");
                    dataContext.getDataCacheManager().getNotFull().get();
                }
            }
            end();
        } catch (Exception e) {
            String sql = loadData != null ? loadData.getSql() : "";
            handler.handleError(ErrorCode.ERR_HANDLE_DATA, e, sql, false);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("Load data file close failed!");
                }
            }
        }
        return null;
    }

    public Object handle() {
        try {
            while (true) {
                if (dataContext.getThrowable() != null) {
                    throw dataContext.getThrowable();
                }
                byte[] data = cacheData.take();
                if (data == END_BYTE) {
                    parser(data, loadData, true);
                    break;
                } else {
                    parser(data, loadData, false);
                    dataContext.getDataCacheManager().releaseMemory(data.length);
                }
            }
        } catch (Throwable t) {
            String sql = loadData != null ? loadData.getSql() : "";
            dataContext.finish(t);
            handler.handleError(ErrorCode.ERR_HANDLE_DATA, t, sql, false);
        }
        return null;
    }

    private int bytesLastIndexOf(byte[] source, byte[] target) {
        if (source.length < target.length) {
            return -1;
        }
        int idx = source.length - 1;
        while (idx >= target.length - 1) {
            boolean flag = true;
            for (int i = target.length - 1; i >= 0; i--) {
                if (target[i] != source[idx - (target.length - 1 - i)]) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                return idx + 1;
            }
            idx--;
        }
        return -1;
    }

    public void parser(byte[] data, LoadData loadData, boolean isEnd) {
        // 如果RestData数据量过大，也即单条数据很长，则直接抛弃并报错
        byte[] tempRestData = new byte[0];
        byte[] lineTerminatedBy = loadData.getOriginLineTerminatedBy().getBytes();
        int lastIndex = bytesLastIndexOf(data, lineTerminatedBy);
        int startPoint = lastIndex == -1 ? 4 : lastIndex;

        if (restData.length > dataContext.getDataCacheManager().getNotFullThreshold()) {
            throw new RuntimeException("RestData is more than notFullThreshold!");
        }
        if (lastIndex == -1) {
            if (!isEnd) {
                tempRestData = new byte[data.length - startPoint + restData.length];
                System.arraycopy(restData, 0, tempRestData, 0, restData.length);
                System.arraycopy(data, startPoint, tempRestData, restData.length, data.length - startPoint);
                restData = tempRestData;
                return;
            }
        }
        tempRestData = new byte[data.length - startPoint];
        System.arraycopy(data, startPoint, tempRestData, 0, data.length - startPoint);

        String content;
        if (restData.length == 0) {
            content = new String(
                data, 4, startPoint - 4, Charset.forName(CharsetUtil.getJavaCharset(loadData.getCharset())));
        } else {
            // 上一条记录中不完整的记录，与本次记录进行合并
            byte[] currentBytes = new byte[restData.length + startPoint - 4];
            System.arraycopy(restData, 0, currentBytes, 0, restData.length);
            System.arraycopy(data, 4, currentBytes, restData.length, startPoint - 4);
            content =
                new String(currentBytes, 0, currentBytes.length,
                    Charset.forName(CharsetUtil.getJavaCharset(loadData.getCharset())));
        }

        List<String> lineContents = Splitter.on(loadData.getOriginLineTerminatedBy()).splitToList(content);

        int realLineNumber = lineContents.size();

        for (int lineNumber = 0; lineNumber < realLineNumber; lineNumber++) {
            if (dataContext.isFinish()) {
                throw new RuntimeException("DataContext is finished!");
            }
            if (loadData.getIgnoreLineNumber() != 0) {
                loadData.setIgnoreLineNumber(loadData.getIgnoreLineNumber() - 1);
                continue;
            }
            if (lineContents.get(lineNumber).equals("")) {
                // 与Mysql的处理略有不同，mysql处理时可以会插入默认值，mysql支持insert into table values (); 而我们目前不支持
                loadData.incrementEmptyLine();
                continue;
            }

            String realLine = lineContents.get(lineNumber);
            // 处理 starting by条件
            if (loadData.getLinesStartingBy() != null) {
                int lineStartIndex = realLine.indexOf(loadData.getLinesStartingBy());
                if (lineStartIndex == -1 || (lineStartIndex + loadData.getLinesStartingBy().length() == realLine
                    .length())) {
                    loadData.incrementEmptyLine();
                    continue;
                } else {
                    realLine =
                        realLine.substring(lineStartIndex + loadData.getLinesStartingBy().length());
                }
            }

            String[] fields = null;
            if (loadData.getEnclose() != null) {
                List<String> stringList = Splitter.on(loadData.getOriginFieldTerminatedBy()).splitToList(realLine);
                fields = new String[stringList.size()];
                for (int fieldIndex = 0; fieldIndex < stringList.size(); fieldIndex++) {
                    fields[fieldIndex] = stringList.get(fieldIndex);
                    // 如果有enclosed by，则进行处理
                    if (!"".equals(loadData.getEnclose())) {
                        if (fields[fieldIndex].length() < 2) {
                            continue;
                        }
                        if ((fields[fieldIndex].startsWith(loadData.getEnclose())) && fields[fieldIndex]
                            .endsWith(loadData.getEnclose())) {
                            fields[fieldIndex] = fields[fieldIndex].substring(1, fields[fieldIndex].length() - 1);
                        }
                    }
                }
            }

            if (loadData.getEscape() != null) {
                if (fields == null) {
                    List<String> stringList = Splitter.on(loadData.getOriginFieldTerminatedBy()).splitToList(realLine);
                    fields = new String[stringList.size()];
                    for (int fieldIndex = 0; fieldIndex < stringList.size(); fieldIndex++) {
                        fields[fieldIndex] = StringEscapeUtils.unescapeJava(stringList.get(fieldIndex));
                    }
                } else {
                    for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
                        fields[fieldIndex] = StringEscapeUtils.unescapeJava(fields[fieldIndex]);
                    }
                }
            }

            if (loadData.getOutputColumnsIndex() == null || loadData.getOutputColumnsIndex().size() == 0) {
                if (fields != null) {
                    realLine = String.join(loadData.getOriginFieldTerminatedBy(), fields);
                }
            } else {
                if (fields == null) {
                    List<String> stringList = Splitter.on(loadData.getOriginFieldTerminatedBy()).splitToList(realLine);
                    fields = new String[stringList.size()];
                    for (int fieldIndex = 0; fieldIndex < stringList.size(); fieldIndex++) {
                        fields[fieldIndex] = stringList.get(fieldIndex);
                    }
                }
                List<Integer> outColumnsIndex = loadData.getOutputColumnsIndex();
                boolean isFirst = true;
                StringBuilder tempLine = new StringBuilder("");
                for (Integer columnIndex : outColumnsIndex) {
                    if (isFirst) {
                        tempLine.append(fields[columnIndex]);
                        isFirst = false;
                    } else {
                        tempLine.append(loadData.getOriginFieldTerminatedBy()).append(fields[columnIndex]);
                    }
                }
                realLine = tempLine.toString();
            }

            long length = SizeOf.sizeOfCharArray(realLine.length());
            dataContext.getDataCacheManager().allocateMemory(length);
            lines.add(realLine);
            if (lines.size() >= dataContext.getBatchInsertNum()) {
                dataContext.getParameters().add(lines);
                lines = new ArrayList<>();
            }
        }
        if (cacheData.isEmpty() && dataContext.getDataCacheManager().isFull() && lines.size() > 0) {
            dataContext.getParameters().add(lines);
            lines = new ArrayList<>();
        }
        if (isEnd) {
            if (lines.size() > 0) {
                dataContext.getParameters().add(lines);
                lines = new ArrayList<>();
            }
            dataContext.getParameters().add(END);
        }
        restData = tempRestData;
        if (isEnd && restData.length > 0) {
            throw new TddlNestableRuntimeException("restData should be empty here!");
        }
    }

    private String replaceRegularCharacters(String str) {
        return str.replace("\\", "\\\\").
            replace("*", "\\*").replace("+", "\\+").
            replace("|", "\\|").replace("{", "\\{").
            replace("}", "\\}").replace("(", "\\(").
            replace(")", "\\)").replace("^", "\\^").
            replace("$", "\\$").replace("[", "\\[").
            replace("]", "\\]").replace("?", "\\?").
            replace(",", "\\,").replace(".", "\\.").replace("&", "\\&");
    }

    private void parseLoadDataPram(MySqlLoadDataInFileStatement statement) {
        SQLTextLiteralExpr rawLineEnd = (SQLTextLiteralExpr) statement.getLinesTerminatedBy();
        String lineTerminatedBy = rawLineEnd == null ? "\n" : rawLineEnd.getText();
        loadData.setOriginLineTerminatedBy(lineTerminatedBy);
        lineTerminatedBy = replaceRegularCharacters(lineTerminatedBy);
        loadData.setLineTerminatedBy(lineTerminatedBy);

        SQLTextLiteralExpr rawFieldEnd = (SQLTextLiteralExpr) statement.getColumnsTerminatedBy();
        String fieldTerminatedBy = rawFieldEnd == null ? "\t" : rawFieldEnd.getText();
        loadData.setOriginFieldTerminatedBy(fieldTerminatedBy);
        fieldTerminatedBy = replaceRegularCharacters(fieldTerminatedBy);
        loadData.setFieldTerminatedBy(fieldTerminatedBy);

        SQLTextLiteralExpr rawEnclosed = (SQLTextLiteralExpr) statement.getColumnsEnclosedBy();
        String enclose = ((rawEnclosed == null) || rawEnclosed.getText().isEmpty()) ? null : rawEnclosed.getText();
        if (enclose != null && enclose.length() > 1) {
            throw new TddlNestableRuntimeException("Field separator argument is not what is expected!");
        }
        loadData.setEnclose(enclose);

        SQLTextLiteralExpr escapedExpr = (SQLTextLiteralExpr) statement.getColumnsEscaped();
        if (escapedExpr != null) {
            if (!escapedExpr.getText().equals("\\")) {
                throw new TddlNestableRuntimeException("Esacped character escaped is not support yet");
            } else {
                loadData.setEscape(escapedExpr.getText());
            }
        }

        // 默认使用utf8编码
        String charset = statement.getCharset() != null ? statement.getCharset() : "utf8";
        loadData.setCharset(CharsetUtil.getJavaCharset(charset));

        SQLTextLiteralExpr columnsEnclosedByExpr = (SQLTextLiteralExpr) statement.getColumnsEnclosedBy();
        String columnsEnclosedBy = columnsEnclosedByExpr == null ? null : columnsEnclosedByExpr.getText();
        loadData.setEnclose(columnsEnclosedBy);

        List<SQLExpr> columnsListExpr = statement.getColumns();
        List<String> columnsList = new ArrayList<>();
        for (int i = 0; i < columnsListExpr.size(); i++) {
            columnsList.add(columnsListExpr.get(i).toString());
        }

        loadData.setReplace(statement.isReplicate());
        loadData.setIgnore(statement.isIgnore());

        SQLExpr ignoreLinesNumberExpr = statement.getIgnoreLinesNumber();
        int ignoreLinesNumber = ignoreLinesNumberExpr == null ? 0 : Integer.parseInt(ignoreLinesNumberExpr.toString());
        loadData.setIgnoreLineNumber(ignoreLinesNumber);

        SQLTextLiteralExpr linesStartingByExpr = (SQLTextLiteralExpr) statement.getLinesStartingBy();
        String linesStartingBy = linesStartingByExpr == null ? null : linesStartingByExpr.getText();
        loadData.setLinesStartingBy(linesStartingBy);

        // columnList (col1, @val1, col2)，则插入列为(col1,col2)，对应于文件中的第0列和第2列
        List outputColumnsIndex = new ArrayList<Integer>();
        List<String> outputColumns = new ArrayList<String>();
        for (int columnIndex = 0; columnIndex < columnsList.size(); columnIndex++) {
            if (columnsList.get(columnIndex).charAt(0) != '@') {
                outputColumnsIndex.add(columnIndex);
                outputColumns.add(columnsList.get(columnIndex));
            }
        }
        loadData.setOutputColumnsIndex(outputColumnsIndex);

        TableMeta tableMeta = OptimizerContext.getContext(
            serverConnection.getSchema()).getLatestSchemaManager().getTable(tableName);
        List<ColumnMeta> loadColumnMetas = new ArrayList<>();
        List<String> tableColumns =
            tableMeta.getAllColumns().stream().map(t -> t.getOriginColumnName()).collect(Collectors.toList());

        ParamManager paramManager = new ParamManager(cmdObjects);
        boolean enableAutoFillIncCol =
            paramManager.getBoolean(ConnectionParams.LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN);

        if (outputColumns.size() > 0) {
            // only load data to assigned column
            for (ColumnMeta meta : tableMeta.getAllColumns()) {
                if (containIgnoreCase(outputColumns, meta.getOriginColumnName())) {
                    loadColumnMetas.add(meta);
                }
            }

            if (enableAutoFillIncCol) {
                boolean containsAutoIncCol =
                    loadColumnMetas.stream().map(ColumnMeta::isAutoIncrement).collect(Collectors.toList())
                        .contains(true);
                if (!containsAutoIncCol && tableMeta.getAutoIncrementColumn() != null) {
                    // load column not contains auto inc column, but this table has an auto inc column
                    // so we should auto fill it by default
                    ColumnMeta autoIncrementColumn = tableMeta.getAutoIncrementColumn();
                    loadColumnMetas.add(0, autoIncrementColumn);
                    outputColumns.add(0, autoIncrementColumn.getOriginColumnName());
                    // add this auto increment column to the head
                    loadData.setAutoFillColumnIndex(0);
                }
            }
            loadData.setColumnsList(outputColumns);
            List<Integer> positions = outputColumns.stream().map(tableColumns::indexOf).collect(Collectors.toList());
            List<Integer> orderedPositions = positions.stream().sorted().collect(Collectors.toList());
            loadData.setSwapColumns(!positions.equals(orderedPositions));
        } else {
            loadColumnMetas = tableMeta.getAllColumns();
            loadData.setColumnsList(tableColumns);
            loadData.setSwapColumns(false);
            if (enableAutoFillIncCol) {
                // implicit primary key must be auto inc column, and one table can only has one auto inc col
                // so we should auto fill this implicit column
                // load file should not contains this implicit column
                boolean containsImplicitKey =
                    loadColumnMetas.stream().map(columnMeta -> columnMeta.getName().equalsIgnoreCase(IMPLICIT_COL_NAME))
                        .collect(Collectors.toList()).contains(true);
                if (containsImplicitKey) {
                    int autoColIndex = loadColumnMetas.stream().map(ColumnMeta::isAutoIncrement).collect(
                        Collectors.toList()).indexOf(true);
                    loadData.setAutoFillColumnIndex(autoColIndex);
                }
            }
        }
        loadData.setColumnMetas(loadColumnMetas);
        loadData.setLocal(statement.isLocal());
    }

    private boolean containIgnoreCase(List<String> source, String target) {
        for (String item : source) {
            if (target.equalsIgnoreCase(item)) {
                return true;
            }
        }
        return false;
    }

    private String parseFileName(String sql) {
        String uSql = sql.toUpperCase();
        int index0 = uSql.indexOf("INFILE");

        for (int i = index0 + 6; i < sql.length(); i++) {
            char quoteChar = sql.charAt(i);
            if (quoteChar > 0x0020) {
                String quoteStr = String.valueOf(quoteChar);
                return sql.substring(i + 1, sql.indexOf(quoteStr, i + 1));
            }
        }
        return null;
    }

    private void parseLoadDataSql(String originStrSql) {
        ContextParameters contextParameters = new ContextParameters(false);
        int start = -1;
        if (originStrSql.indexOf(LOWER_LOAD_DATA) > 0) {
            start = originStrSql.indexOf(LOWER_LOAD_DATA);
        } else if (originStrSql.indexOf(UPPER_LOAD_DATA) > 0) {
            start = originStrSql.indexOf(UPPER_LOAD_DATA);
        }

        String strSql = originStrSql;
        if (start > 0) {
            String hint = originStrSql.substring(0, start);
            strSql = originStrSql.substring(start, strSql.length());
            try {
                List<SQLStatement> stmtList = FastsqlUtils.parseSql(hint);
                for (SQLStatement statement : stmtList) {
                    List<SQLCommentHint> hintList = ((MySqlHintStatement) statement).getHints();
                    SqlNodeList sqlNodes =
                        FastSqlConstructUtils.convertHints(hintList, contextParameters, new ExecutionContext());
                    HintConverter.HintCollection collection = new HintConverter.HintCollection();
                    HintUtil.collectHint(sqlNodes, collection, false, new ExecutionContext());
                    List<HintCmdOperator> hintCmdOperators = collection.cmdHintResult;
                    HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean("", cmdObjects, "");
                    for (HintCmdOperator op : hintCmdOperators) {
                        op.handle(cmdBean);
                    }
                }
            } catch (Exception e) {
                logger.error("parser the load data hint", e);
                strSql = originStrSql;
            }
        }

        MySqlLoadDataInFileStatement statement;
        try {
            statement = (MySqlLoadDataInFileStatement) FastsqlUtils.parseSql(ByteString.from(strSql)).get(0);
            tableName = statement.getTableName().getSimpleName();
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        fileName = parseFileName(strSql);
        if (fileName == null) {
            throw new TddlNestableRuntimeException("File name is null !");
        }
        loadData = new LoadData(originStrSql);
        parseLoadDataPram(statement);
    }

    public void buildInsertIntoTemplate(boolean pureInsert) {
        // load data 的建表语句
        StringBuilder builder = new StringBuilder();
        if (pureInsert) {
            builder.append("insert into ").append(tableName).append(" ");
        } else if (!loadData.isReplace()) {
            builder.append("insert ignore into ").append(tableName).append(" ");
        } else {
            builder.append("replace into ").append(tableName).append(" ");
        }
        if (loadData.getColumnsList().size() != 0) {
            builder.append("(" + String.join(",", loadData.getColumnsList()) + ") ");
        }
        builder.append(" values ");
        builder.append("(");
        for (int i = 0; i < loadData.getColumnsList().size(); i++) {
            if (i == loadData.getColumnsList().size() - 1) {
                builder.append("?");
            } else {
                builder.append("?,");
            }
        }
        builder.append(")");
        loadData.setTemplateSql(ByteString.from(builder.toString()));
    }

    @Override
    public void open(String strSql) {
        //if database is readonly, we cannot load data
        String schemaName = serverConnection.getSchema();
        if (DbInfoManager.getInstance().ifDatabaseIsReadOnly(SQLUtils.normalize(schemaName))) {
            throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_OPTIMIZER,
                String.format("load data failed because database [%s] is read only now", schemaName));
        }

        parseLoadDataSql(strSql);

        ParamManager paramManager = new ParamManager(cmdObjects);

        buildInsertIntoTemplate(paramManager.getBoolean(ConnectionParams.LOAD_DATA_PURE_INSERT_MODE));

        MemoryPool memoryPool =
            MemoryManager.getInstance().createQueryMemoryPool(false, serverConnection.getTraceId(), cmdObjects);

        long loadDataMaxMemory = paramManager.getLong(ConnectionParams.LOAD_DATA_CACHE_BUFFER_SIZE);
        long batchInsertSize = paramManager.getLong(ConnectionParams.LOAD_DATA_BATCH_INSERT_SIZE);

        this.dataContext = new LoadDataContext(new LoadDataCacheManager(memoryPool, loadDataMaxMemory),
            new LinkedBlockingQueue<>(), batchInsertSize, loadData.getSql(),
            loadData.getColumnMetas().stream().map(t -> SqlTypeName.get(t.getDataType().getStringSqlType())).collect(
                Collectors.toList()), loadData.getOriginFieldTerminatedBy(),
            Charset.forName(CharsetUtil.getJavaCharset(loadData.getCharset())), loadData.getColumnMetas(), tableName,
            cmdObjects, loadData.getAutoFillColumnIndex());
        dataContext.setSwapColumns(loadData.isSwapColumns());
        if (loadData.isLocal()) {
            handler.sendRequestFilePacket(strSql);
        } else {
            PolarPrivileges polarPrivileges = (PolarPrivileges) serverConnection.getPrivileges();
            PolarAccountInfo polarUserInfo = polarPrivileges.checkAndGetMatchUser(
                serverConnection.getUser(), serverConnection.getHost());
            if (!polarUserInfo.getAccountType().isGod()) {
                if (!InstConfUtil.getValBool(TddlConstants.ENABLE_LOAD_DATA_FILE)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                        "load data infile is not enabled!");
                }
            }
        }
        if (!isClosed.get()) {
            bStart = true;
            final Map mdcContext = MDC.getCopyOfContextMap();
            serverConnection.getSchemaConfig().getDataSource().borrowExecutorService().submitListenableFuture(
                serverConnection.getSchema(), serverConnection.getTraceId(), -1, () -> {
                    MDC.setContextMap(mdcContext);
                    return handle();
                }, null);
            if (!loadData.isLocal()) {
                serverConnection.getSchemaConfig().getDataSource().borrowExecutorService().submitListenableFuture(
                    serverConnection.getSchema(), serverConnection.getTraceId(), -1, () -> {
                        MDC.setContextMap(mdcContext);
                        return readFileData();
                    }, null);
            }
            serverConnection.getSchemaConfig().getDataSource().borrowExecutorService().submitListenableFuture(
                serverConnection.getSchema(), serverConnection.getTraceId(), -1, () -> {
                    MDC.setContextMap(mdcContext);
                    return run();
                }, null);
        }
    }

    /**
     * 不停的从buffer中获取拼接好的batch insert sql 执行
     */
    private Object run() {
        try {
            serverConnection.setSqlSample(loadData.getSql());
            serverConnection.innerExecute(loadData.getTemplateSql(), null, handler, dataContext);
        } finally {
            serverConnection.setSqlSample(null);
        }
        return null;
    }

    @Override
    public void close() {
        tryClose();
        handler.sendPacketEnd(true);
    }

    public void tryClose() {
        bStart = false;
        if (dataContext != null) {
            this.dataContext.clear();
        }
        this.lines.clear();
        this.cacheData.clear();
        this.cacheData.add(END_BYTE);
        restData = null;
    }

    @Override
    public boolean isFull() {
        if (dataContext != null) {
            return dataContext.getDataCacheManager().isFull();
        }
        return false;
    }

    @Override
    public boolean isBlocked() {
        if (dataContext != null) {
            return dataContext.getDataCacheManager().isBlocked();
        }
        return false;
    }

    public void disableConnection() {
        if (dataContext != null) {
            LoadDataCacheManager dataCacheManager = dataContext.getDataCacheManager();
            synchronized (dataCacheManager.getLock()) {
                if (dataCacheManager.isBlocked()) {
                    logger.warn("disable the connection " + serverConnection.getTraceId());
                    serverConnection.disableRead();

                    dataCacheManager.getNotFull().addListener(new Runnable() {
                        @Override
                        public void run() {
                            serverConnection.enableRead();
                            logger.warn("enable the connection " + serverConnection.getTraceId());
                        }
                    }, directExecutor());
                }
            }
        }
    }

    class LoadDataResultHandler implements QueryResultHandler {

        private OkPacket ok = null;

        public LoadDataResultHandler() {
        }

        public void sendRequestFilePacket(String strSql) {
            CommandPacket cmdPacket = new CommandPacket();
            final byte FIELD_COUNT = (byte) 251;
            loadPackID++;
            cmdPacket.packetId = serverConnection.getNewPacketId();
            cmdPacket.command = FIELD_COUNT;
            try {
                cmdPacket.arg = ServerConnection.encodeString(fileName, serverConnection.getResultSetCharset());
            } catch (Exception e) {
                handleError(e, strSql, false);
                return;
            }
            try {
                cmdPacket.write(PacketOutputProxyFactory.getInstance().createProxy(serverConnection));
            } catch (Exception e) {
                handleError(e, strSql, false);
            }
        }

        @Override
        public void sendUpdateResult(long affectedRows) {
            ok = new OkPacket();
            ok.packetId = serverConnection.getNewPacketId();
            ok.insertId = serverConnection.getTddlConnection().getReturnedLastInsertId();
            ok.affectedRows = affectedRows;
            ok.serverStatus = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
            if (!serverConnection.isAutocommit()) {
                ok.serverStatus = MySQLPacket.SERVER_STATUS_IN_TRANS;
            }
            ok.warningCount = serverConnection.getTddlConnection().getWarningCount();
            ok.message = ServerConnection.encodeString(
                "skipped lines: " + skippedEmptyLines, serverConnection.getResultSetCharset());
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows, long sqlSelectLimit)
            throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendPacketEnd(boolean closed) {
            try {
                if (isClosed.compareAndSet(false, true)) {
                    String loadSql = loadData != null ? loadData.getSql() : "Data Load Error!";
                    serverConnection.setLastSqlStartTime(startTime);
                    tryClose();
                    if (ok != null) {
                        ok.write(PacketOutputProxyFactory.getInstance().createProxy(serverConnection));
                        ok = null;
                    } else {
                        serverConnection.handleError(
                            ErrorCode.ERR_HANDLE_DATA, new Exception("force close!"), loadSql, true);
                    }
                    ok = null;
                }
            } catch (Throwable t) {
                logger.error("load data handleError！", t);
            }

        }

        @Override
        public void handleError(Throwable ex, ByteString sql, boolean fatal) {
            handleError(ex, sql.toString(), fatal);
        }

        public void handleError(Throwable ex, String sql, boolean fatal) {
            handleError(ErrorCode.ERR_HANDLE_DATA, ex, sql, fatal);
        }

        public void handleError(ErrorCode errCode, Throwable ex, String sql, boolean fatal) {
            try {
                if (isClosed.compareAndSet(false, true)) {
                    serverConnection.setLastSqlStartTime(startTime);
                    tryClose();
                    serverConnection.handleError(errCode, ex, sql, fatal);
                    long rowNums = dataContext != null ? dataContext.getLoadDataAffectRows().get() : 0;
                    logger.warn("Maybe " + rowNums + " rows affected!");
                }
            } catch (Throwable t) {
                logger.error("load data handleError！", t);
            }
        }
    }

    @Override
    public Throwable throwError() {
        if (dataContext != null) {
            return dataContext.getThrowable();
        }
        return null;
    }

}
