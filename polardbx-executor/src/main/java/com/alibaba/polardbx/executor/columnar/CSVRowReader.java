package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Arrays;

public class CSVRowReader {
    private static final int BUFFER_INITIAL_SIZE = 1 << 10;

    private final String csvFileName;
    private InputStream inputStream;
    private int fieldNum;

    private int currentPosition;
    private final int fileEndOffset;

    private final static int END_OF_LINE_LENGTH = 1;
    private SliceOutput fileBufferOutput;
    private Slice fileBuffer;

    public CSVRowReader(String csvFileName, InputStream inputStream, int fieldNum) throws IOException {
        this.csvFileName = csvFileName;
        this.inputStream = inputStream;
        this.fieldNum = fieldNum;

        this.currentPosition = 0;
        this.fileEndOffset = inputStream.available();

        this.fileBufferOutput = new DynamicSliceOutput(fieldNum * Byte.BYTES);
    }

    public boolean isReadable() {
        return currentPosition < fileEndOffset;
    }

    public CSVRow nextRow() throws IOException {
        if (!isReadable()) {
            return null;
        }

        int endOffset = nextLine();

        SliceOutput buffer = new DynamicSliceOutput(BUFFER_INITIAL_SIZE);
        int currentOffset = currentPosition;

        // For column offsets.
        int[] offsets = new int[fieldNum];

        // For null value
        byte[] nulls = new byte[fieldNum];
        Arrays.fill(nulls, (byte) 0);

        for (int columnIndex = 0; columnIndex < fieldNum; columnIndex++) {
            if (currentOffset >= endOffset) {
                throw GeneralUtil.nestedException("bad format error for this csv file");
            }

            // Record the beginning position of current column.
            final int columnBeginPosition = currentOffset;

            byte currentByte = nextByte(currentOffset);

            // Handle the case where the first character is a quote.
            if (currentByte == '"') {
                // Increment past the first quote
                currentOffset++;

                // Loop through the row to extract the values for the current field
                for (; currentOffset < endOffset; currentOffset++) {
                    currentByte = nextByte(currentOffset);

                    // Check for end of the current field
                    if (currentByte == '"'
                        && (currentOffset == endOffset - 1
                        || nextByte(currentOffset + 1) == ',')) {

                        // Move past the ',' and the '"'
                        // It means the empty byte array.
                        currentOffset += 2;
                        break;
                    }

                    if (currentByte == '\\' && currentOffset != (endOffset - 1)) {
                        currentOffset++;
                        currentByte = nextByte(currentOffset);

                        if (currentByte == 'r') {
                            buffer.appendByte('\r');
                        } else if (currentByte == 'n') {
                            buffer.appendByte('\n');
                        } else if (currentByte == '\\' || currentByte == '"') {
                            buffer.appendByte(currentByte);
                        } else {
                            buffer.appendByte('\\');
                            buffer.appendByte(currentByte);
                        }
                    } else {
                        if (currentOffset == endOffset - 1) {
                            // If we are at final symbol and no last quote was found,
                            // we are working with a damaged file.
                            throw GeneralUtil.nestedException("bad format error for this csv file");
                        }
                        // For ordinary symbol
                        buffer.appendByte(currentByte);
                    }
                }
            } else {
                for (; currentOffset < endOffset; currentOffset++) {
                    currentByte = nextByte(currentOffset);
                    // Move past the ','
                    if (currentByte == ',') {
                        if (columnBeginPosition == currentOffset) {
                            // It means null value because there is comma without any character
                            // (including quote character)
                            nulls[columnIndex] = 1;
                        }

                        currentOffset++;
                        break;
                    }
                    if (currentByte == '\\' && currentOffset != (endOffset - 1)) {
                        currentOffset++;
                        currentByte = nextByte(currentOffset);
                        if (currentByte == 'r') {
                            buffer.appendByte('\r');
                        } else if (currentByte == 'n') {
                            buffer.appendByte('\n');
                        } else if (currentByte == '\\' || currentByte == '"') {
                            buffer.appendByte(currentByte);
                        } else  /* This could only happed with an externally created file */ {
                            buffer.appendByte('\\');
                            buffer.appendByte(currentByte);
                        }
                    } else {
                        if (currentOffset == endOffset - 1 && currentByte == '"') {
                            // We are at the final symbol and a quote was found for the
                            // unquoted field => We are working with a damaged field.
                            throw GeneralUtil.nestedException("bad format error for this csv file");
                        }

                        buffer.appendByte(currentByte);
                    }
                }
            }

            offsets[columnIndex] = buffer.size();
        }

        currentPosition = endOffset + END_OF_LINE_LENGTH;

        Slice result = buffer.slice();
        return new CSVRow(result, offsets, nulls, fieldNum);
    }

    private int nextLine() throws IOException {
        fileBufferOutput.reset();
        for (int position = currentPosition; position < fileEndOffset; position++) {
            byte b = (byte) inputStream.read();
            fileBufferOutput.appendByte(b);

            // Line delimiter
            if (b == '\n') {
                fileBuffer = fileBufferOutput.slice();
                return position;
            }
        }
        throw GeneralUtil.nestedException(MessageFormat.format("File {0} is damaged", csvFileName));
    }

    private byte nextByte(int position) throws IOException {
        return fileBuffer.getByte(position - currentPosition);
    }

}
