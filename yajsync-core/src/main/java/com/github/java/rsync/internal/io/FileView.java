/*
 * Slidable file buffer which defers I/O errors until it is closed and
 * provides direct access to buffer contents
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.java.rsync.internal.io;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.internal.util.RuntimeInterruptException;

public class FileView implements AutoCloseable {
    public final static int DEFAULT_BLOCK_SIZE = 8 * 1024;
    private static final Logger LOG = Logger.getLogger(FileView.class.getName());
    private final byte[] buf;
    private int endOffset = -1; // length == endOffset - startOffset + 1
    private final String fileName;
    private IOException ioError = null;
    private final InputStream is;
    private int markOffset = -1;
    private int readOffset = -1;
    private long remainingBytes;
    private int startOffset = 0;
    private final int windowLength; // size of sliding window (<= buf.length)

    public FileView(Path path, long fileSize, int windowLength, int bufferSize) throws FileViewOpenFailed {
        assert path != null;
        assert fileSize >= 0;
        assert windowLength >= 0;
        assert bufferSize >= 0;
        assert windowLength <= bufferSize;

        try {
            fileName = path.toString();
            remainingBytes = fileSize;

            if (fileSize > 0) {
                is = Files.newInputStream(path);
                this.windowLength = windowLength;
                buf = new byte[bufferSize];
                slide(0);
                assert startOffset == 0;
                assert endOffset >= 0;
            } else {
                is = null;
                this.windowLength = 0;
                buf = new byte[0];
            }

        } catch (FileNotFoundException | NoSuchFileException e) { // TODO: which exception should we really catch
            throw new FileViewNotFound(e.getMessage());
        } catch (IOException e) {
            throw new FileViewOpenFailed(e.getMessage());
        }
    }

    @Override
    public void close() throws FileViewException {
        if (is != null) {
            try {
                is.close();
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                throw new FileViewException(e);
            }
        }

        if (ioError != null) {
            throw new FileViewException(ioError);
        }
    }

    private void compact() {
        assert getNumBytesPrefetched() >= 0;
        assert getTotalBytes() >= 0; // unless we'd support skipping

        int shiftOffset = getFirstOffset();
        int numShifts = getNumBytesMarked() + getNumBytesPrefetched();

        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("compact of %s before - buf[%d] %d bytes to buf[0], " + "buf.length = %d", this, shiftOffset, numShifts, buf.length));
        }

        System.arraycopy(buf, shiftOffset, buf, 0, numShifts);
        startOffset -= shiftOffset;
        endOffset -= shiftOffset;
        readOffset -= shiftOffset;
        if (markOffset >= 0) {
            markOffset -= shiftOffset;
        }

        assert startOffset >= 0;
        assert endOffset >= -1;
        assert readOffset >= -1;
        assert markOffset >= -1;

        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("compacted %d bytes, result after: %s", numShifts, this));
        }
    }

    public byte[] getArray() {
        return buf;
    }

    private int getBufferSpaceAvailable() {
        assert readOffset <= buf.length - 1;
        return buf.length - 1 - readOffset;
    }

    public int getEndOffset() {
        assert endOffset >= -1;
        return endOffset;
    }

    // TODO: the names startOffset and firstOffset are confusingly similar
    public int getFirstOffset() {
        assert startOffset >= 0;
        assert markOffset >= -1;
        return markOffset >= 0 ? Math.min(startOffset, markOffset) : startOffset;
    }

    // might return -1
    public int getMarkOffset() {
        assert markOffset >= -1;
        assert markOffset <= buf.length - 1 || is == null;
        return markOffset;
    }

    public int getNumBytesMarked() {
        return startOffset - getFirstOffset();
    }

    public int getNumBytesPrefetched() {
        return readOffset - startOffset + 1;
    }

    // TODO: the names startOffset and firstOffset are confusingly similar
    public int getStartOffset() {
        assert startOffset >= 0;
        assert startOffset <= buf.length - 1 || is == null;
        return startOffset;
    }

    public int getTotalBytes() {
        return endOffset - getFirstOffset() + 1;
    }

    public int getWindowLength() {
        int length = endOffset - startOffset + 1;
        assert length >= 0;
        assert length <= windowLength : length + " maxWindowLength=" + windowLength;
        return length;
    }

    public boolean isFull() {
        assert getTotalBytes() <= buf.length;
        return getTotalBytes() == buf.length; // || windowLength() == 0 && remainingBytes == 0
    }

    private void readBetween(int min, int max) throws IOException {
        assert min >= 0 && min <= max;
        assert max <= remainingBytes;
        assert max <= getBufferSpaceAvailable();

        int numBytesRead = 0;
        while (numBytesRead < min) {
            int len = is.read(buf, readOffset + 1, max - numBytesRead);
            if (len <= 0) {
                throw new EOFException(String.format("File ended prematurely " + "(%d)", len));
            }
            numBytesRead += len;
            readOffset += len;
            remainingBytes -= len;
        }

        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("prefetched %d bytes (min=%d, max=%d)", numBytesRead, min, max));
        }
        assert remainingBytes >= 0;
    }

    private void readZeroes(int amount) {
        assert amount <= remainingBytes;
        assert amount <= getBufferSpaceAvailable();

        Arrays.fill(buf, readOffset + 1, readOffset + 1 + amount, (byte) 0);
        readOffset += amount;
        remainingBytes -= amount;
    }

    public void setMarkRelativeToStart(int relativeOffset) {
        assert relativeOffset >= 0;
        // it's allowed to move 1 passed endOffset, as length is defined as:
        // endOffset - startOffset + 1
        assert startOffset + relativeOffset <= endOffset + 1;
        markOffset = startOffset + relativeOffset;
    }

    /*
     * slide window to right slideAmount number of bytes startOffset is increased by
     * slideAmount endOffset is set to the minimum of this fileView's window size
     * and the remaining number of bytes from the file markOffset position relative
     * to startOffset is left unchanged _errorOffset might be set if an IO error
     * occurred readOffset will be >= endOffset and marks the position of the last
     * available prefetched byte read data might be compacted if there's not enough
     * room left in the buffer
     */
    public void slide(int slideAmount) {
        assert slideAmount >= 0;
        assert slideAmount <= getWindowLength();

        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("sliding %s %d", this, slideAmount));
        }

        startOffset += slideAmount;
        int windowLength = (int) Math.min(this.windowLength, getNumBytesPrefetched() + remainingBytes);
        assert windowLength >= 0;
        assert getNumBytesPrefetched() >= 0; // a negative value would imply a skip, which we don't (yet at least) support
        int minBytesToRead = windowLength - getNumBytesPrefetched();

        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("next window length %d, minimum bytes to read %d", windowLength, minBytesToRead));
        }

        if (minBytesToRead > 0) {
            if (minBytesToRead > getBufferSpaceAvailable()) {
                compact();
            }

            int saveOffset = readOffset;
            try {
                if (ioError == null) {
                    readBetween(minBytesToRead, (int) Math.min(remainingBytes, getBufferSpaceAvailable()));
                } else {
                    readZeroes(minBytesToRead);
                }
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                ioError = e;
                int numBytesRead = readOffset - saveOffset;
                readZeroes(minBytesToRead - numBytesRead);
            }
        }

        endOffset = startOffset + windowLength - 1;

        assert getWindowLength() == windowLength;
        assert endOffset <= readOffset;
    }

    @Override
    public String toString() {
        return String.format("%s (fileName=%s, startOffset=%d, markOffset=%d," + " endOffset=%d, windowLength=%d, " + "prefetchedOffset=%d, remainingBytes=%d)", this.getClass().getSimpleName(),
                fileName, startOffset, markOffset, getEndOffset(), getWindowLength(), readOffset, remainingBytes);
    }

    public byte valueAt(int offset) {
        assert offset >= getFirstOffset();
        assert offset <= endOffset;
        return buf[offset];
    }
}
