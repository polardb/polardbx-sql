package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.columnar.DeletionFileReader;
import com.alibaba.polardbx.executor.columnar.SimpleDeletionFileReader;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

public class FlashbackDeleteBitmapManager extends AbstractLifecycle {

    private final long flashbackTso;
    private final String logicalSchema;
    private final String logicalTable;
    private final String partName;
    private final List<Pair<String, Long>> delPositions;

    private final Map<String, RoaringBitmap> loadedBitmaps = new HashMap<>();

    static final Map<String, WeakReference<FlashbackDeleteBitmapManager>> MANAGER_MAP = new WeakHashMap<>();

    public FlashbackDeleteBitmapManager(long flashbackTso, String logicalSchema, String logicalTable, String partName,
                                        List<Pair<String, Long>> delPositions) {
        this.flashbackTso = flashbackTso;
        this.logicalSchema = logicalSchema;
        this.logicalTable = logicalTable;
        this.partName = partName;
        this.delPositions = delPositions;
    }

    synchronized public static FlashbackDeleteBitmapManager getInstance(long flashbackTso, String logicalSchema,
                                                                        String logicalTable,
                                                                        String partName,
                                                                        List<Pair<String, Long>> delPositions) {
        String key = logicalSchema + "." + logicalTable + "." + partName + "." + flashbackTso;
        WeakReference<FlashbackDeleteBitmapManager> ref = MANAGER_MAP.get(key);
        FlashbackDeleteBitmapManager manager = ref == null ? null : ref.get();
        if (manager == null) {
            manager =
                new FlashbackDeleteBitmapManager(flashbackTso, logicalSchema, logicalTable, partName, delPositions);
            MANAGER_MAP.entrySet().removeIf(entry -> entry.getValue().get() == null);

            MANAGER_MAP.put(key, new WeakReference<>(manager));
            // init() can not be called here since it is synchronized and would block the loading of delete bitmap
        }
        return manager;
    }

    @Override
    protected void doInit() {
        if (delPositions == null) {
            return;
        }

        for (Pair<String, Long> delPosition : delPositions) {
            String deleteFileName = delPosition.getKey();
            Long readPos = delPosition.getValue();
            FileMeta deleteFileMeta = ColumnarManager.getInstance().fileMetaOf(deleteFileName);
            long tableId = Long.parseLong(deleteFileMeta.getLogicalTableName());
            Engine engine = deleteFileMeta.getEngine();

            try (SimpleDeletionFileReader fileReader = new SimpleDeletionFileReader()) {
                try {
                    fileReader.open(
                        engine,
                        deleteFileName,
                        0,
                        readPos.intValue()
                    );
                } catch (IOException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e,
                        String.format("Failed to open delete bitmap file, filename: %s, offset: %d, length: %d",
                            deleteFileName, 0, readPos));
                }

                DeletionFileReader.DeletionEntry entry;
                while (fileReader.position() < readPos && (entry = fileReader.next()) != null) {
                    final int fileId = entry.getFileId();
                    final RoaringBitmap bitmap = entry.getBitmap();
                    ColumnarManager.getInstance().fileNameOf(logicalSchema, tableId, partName, fileId)
                        .ifPresent(fileName -> loadedBitmaps.compute(fileName, (k, v) -> {
                            if (v == null) {
                                return bitmap;
                            } else {
                                v.or(bitmap);
                                return v;
                            }
                        }));
                }
            }

        }
    }

    @Override
    protected void doDestroy() {
        loadedBitmaps.clear();
    }

    public RoaringBitmap getDeleteBitmapOf(String fileName) {
        init();
        return loadedBitmaps.getOrDefault(fileName, new RoaringBitmap());
    }
}
