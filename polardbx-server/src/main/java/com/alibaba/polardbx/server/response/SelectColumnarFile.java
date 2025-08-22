package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.columnar.DeletionFileReader;
import com.alibaba.polardbx.executor.columnar.SimpleCSVFileReader;
import com.alibaba.polardbx.executor.columnar.SimpleDeletionFileReader;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarStoreUtils;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import io.airlift.slice.Slice;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectColumnarFile {
    private static final String fileNameRegex = "SELECT\\s+COLUMNAR_FILE\\s*\\([\"'](.*)[\"']\\)";
    private static final Pattern fileNamePattern = Pattern.compile(fileNameRegex, Pattern.CASE_INSENSITIVE);

    public static boolean execute(ServerConnection c, boolean hasMore, ByteString stmt) {
        String input = stmt.toString();
        Matcher matcher = fileNamePattern.matcher(input);
        String fileName = null;
        if (matcher.find()) {
            try {
                fileName = matcher.group(1);
            } catch (IndexOutOfBoundsException e) {
                // ignore
            }
        }
        if (fileName == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Wrong format, example: select columnar_file('1.orc')");
        }
        ColumnarFileType type = FileSystemUtils.getFileType(fileName);
        if (type == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Unsupported columnar file suffix: " + fileName);
        }
        switch (type) {
        case CSV:
            return readCsvFile(c, hasMore, fileName);
        case DEL:
            return readDeleteBitmap(c, hasMore, fileName);
        case ORC:
            return readOrcFile(c, hasMore, fileName);
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Unsupported columnar file type: " + type);
        }
    }

    private static IPacketOutputProxy buildHeader(IPacketOutputProxy proxy, List<ColumnMeta> columnMetas) {
        byte packetId = 0;

        ResultSetHeaderPacket header = PacketUtil.getHeader(columnMetas.size());
        header.packetId = ++packetId;
        proxy = header.write(proxy);

        for (ColumnMeta columnMeta : columnMetas) {
            FieldPacket field =
                PacketUtil.getField(columnMeta.getFullName(), Fields.FIELD_TYPE_VAR_STRING);
            field.packetId = ++packetId;

            proxy = field.write(proxy);
        }
        return proxy;
    }

    private static IPacketOutputProxy buildDeleteBitmapHeader(IPacketOutputProxy proxy) {
        byte packetId = 0;
        FieldPacket field;

        ResultSetHeaderPacket header = PacketUtil.getHeader(3);
        header.packetId = ++packetId;
        proxy = header.write(proxy);

        field = PacketUtil.getField("TSO", Fields.FIELD_TYPE_VAR_STRING);
        field.packetId = ++packetId;
        proxy = field.write(proxy);

        field = PacketUtil.getField("File ID", Fields.FIELD_TYPE_VAR_STRING);
        field.packetId = ++packetId;
        proxy = field.write(proxy);

        field = PacketUtil.getField("Bitmap", Fields.FIELD_TYPE_VAR_STRING);
        field.packetId = ++packetId;
        proxy = field.write(proxy);

        return proxy;
    }

    private static boolean readCsvFile(ServerConnection c, boolean hasMore, String fileName) {
        FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(fileName);
        Engine engine = fileMeta.getEngine();
        List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();
        int filedCount = columnMetas.size();

        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = buildHeader(proxy, columnMetas);

        // What if column size is more than 127?
        byte tmpPacketId = (byte) (filedCount + 1);
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        long maxReadPosition;
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);
            ColumnarAppendedFilesRecord lastRecord =
                columnarAppendedFilesAccessor.queryByFileNameAndMaxTso(fileName,
                    ColumnarManager.getInstance().latestTso()).get(0);
            maxReadPosition = lastRecord.appendOffset + lastRecord.appendLength;
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to query latest position of csv file: %s", fileName));
        }

        String charset = c.getResultSetCharset();
        try (SimpleCSVFileReader csvFileReader = new SimpleCSVFileReader()) {
            csvFileReader.open(ColumnarStoreUtils.newEcForCache(), columnMetas,
                FileVersionStorage.CSV_CHUNK_LIMIT, engine, fileName, 0, (int) maxReadPosition);
            Chunk result;
            while ((result = csvFileReader.nextUntilPosition(maxReadPosition)) != null) {
                for (int pos = 0; pos < result.getPositionCount(); pos++) {
                    RowDataPacket row = new RowDataPacket(filedCount);
                    for (int field = 0; field < filedCount; field++) {
                        // This type conversion may be unexpected.
                        Object resultObject = result.getBlock(field).getObject(pos);
                        byte[] resultBytes;
                        if (resultObject == null) {
                            resultBytes = "NULL".getBytes(charset);
                        } else if (resultObject instanceof Slice) {
                            resultBytes = ((Slice) resultObject).toStringUtf8().getBytes(charset);
                        } else {
                            resultBytes = resultObject.toString().getBytes(charset);
                        }
                        row.add(resultBytes);
                    }
                    row.packetId = ++tmpPacketId;
                    proxy = row.write(proxy);
                }
            }

        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed to read csv file, file name: %s, max position: %d", fileName, maxReadPosition));
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        proxy.packetEnd();
        return true;
    }

    private static boolean readOrcFile(ServerConnection c, boolean hasMore, String fileName) {
        FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(fileName);
        Engine engine = fileMeta.getEngine();
        List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();
        int fieldCount = columnMetas.size();

        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = buildHeader(proxy, columnMetas);

        byte tmpPacketId = (byte) (fieldCount + 1);
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        // TODO(siyun): raw read ORC file
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        proxy.packetEnd();
        return true;
    }

    private static boolean readDeleteBitmap(ServerConnection c, boolean hasMore, String fileName) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = buildDeleteBitmapHeader(proxy);

        byte tmpPacketId = (byte) (3 + 1);
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        long maxReadPosition;
        Engine engine;
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);
            ColumnarAppendedFilesRecord lastRecord =
                columnarAppendedFilesAccessor.queryByFileNameAndMaxTso(fileName,
                    ColumnarManager.getInstance().latestTso()).get(0);
            maxReadPosition = lastRecord.appendOffset + lastRecord.appendLength;
            engine = Engine.of(lastRecord.engine);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to query latest position of delete bitmap: %s", fileName));
        }

        try (SimpleDeletionFileReader fileReader = new SimpleDeletionFileReader()) {
            try {
                fileReader.open(engine, fileName, 0, (int) maxReadPosition);
            } catch (IOException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e,
                    String.format("Failed to open delete bitmap file, filename: %s, offset: %d, length: %d",
                        fileName, 0, maxReadPosition));
            }
            DeletionFileReader.DeletionEntry entry;
            while (fileReader.position() < maxReadPosition && (entry = fileReader.next()) != null) {
                final long delTso = entry.getTso();
                final int fileId = entry.getFileId();
                final RoaringBitmap bitmap = entry.getBitmap();
                if (bitmap != null) {
                    RowDataPacket row = new RowDataPacket(3);
                    row.add(String.valueOf(delTso).getBytes());
                    row.add(String.valueOf(fileId).getBytes());
                    row.add(bitmap.toString().getBytes());
                    row.packetId = ++tmpPacketId;
                    proxy = row.write(proxy);
                }
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, t,
                String.format("Failed to read delete bitmap, file name: %s, max position: %d", fileName,
                    maxReadPosition));
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        proxy.packetEnd();
        return true;
    }
}
