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
    private static final Logger LOG = Logger.getLogger(FileView.class.getName());
    public final static int DEFAULT_BLOCK_SIZE = 8 * 1024;
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
            this.fileName = path.toString();
            this.remainingBytes = fileSize;
            
            if (fileSize > 0) {
                this.is = Files.newInputStream(path);
                this.windowLength = windowLength;
                this.buf = new byte[bufferSize];
                this.slide(0);
                assert this.startOffset == 0;
                assert this.endOffset >= 0;
            } else {
                this.is = null;
                this.windowLength = 0;
                this.buf = new byte[0];
            }
            
        } catch (FileNotFoundException | NoSuchFileException e) { // TODO: which exception should we really catch
            throw new FileViewNotFound(e.getMessage());
        } catch (IOException e) {
            throw new FileViewOpenFailed(e.getMessage());
        }
    }
    
    public byte[] getArray() {
        return this.buf;
    }
    
    private int getBufferSpaceAvailable() {
        assert this.readOffset <= this.buf.length - 1;
        return this.buf.length - 1 - this.readOffset;
    }
    
    @Override
    public void close() throws FileViewException {
        if (this.is != null) {
            try {
                this.is.close();
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                throw new FileViewException(e);
            }
        }
        
        if (this.ioError != null) {
            throw new FileViewException(this.ioError);
        }
    }
    
    private void compact() {
        assert this.getNumBytesPrefetched() >= 0;
        assert this.getTotalBytes() >= 0; // unless we'd support skipping
        
        int shiftOffset = this.getFirstOffset();
        int numShifts = this.getNumBytesMarked() + this.getNumBytesPrefetched();
        
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("compact of %s before - buf[%d] %d bytes to buf[0], " + "buf.length = %d", this, shiftOffset, numShifts, this.buf.length));
        }
        
        System.arraycopy(this.buf, shiftOffset, this.buf, 0, numShifts);
        this.startOffset -= shiftOffset;
        this.endOffset -= shiftOffset;
        this.readOffset -= shiftOffset;
        if (this.markOffset >= 0) {
            this.markOffset -= shiftOffset;
        }
        
        assert this.startOffset >= 0;
        assert this.endOffset >= -1;
        assert this.readOffset >= -1;
        assert this.markOffset >= -1;
        
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("compacted %d bytes, result after: %s", numShifts, this));
        }
    }
    
    public int getEndOffset() {
        assert this.endOffset >= -1;
        return this.endOffset;
    }
    
    // TODO: the names startOffset and firstOffset are confusingly similar
    public int getFirstOffset() {
        assert this.startOffset >= 0;
        assert this.markOffset >= -1;
        return this.markOffset >= 0 ? Math.min(this.startOffset, this.markOffset) : this.startOffset;
    }
    
    public boolean isFull() {
        assert this.getTotalBytes() <= this.buf.length;
        return this.getTotalBytes() == this.buf.length; // || windowLength() == 0 && remainingBytes == 0
    }
    
    // might return -1
    public int getMarkOffset() {
        assert this.markOffset >= -1;
        assert this.markOffset <= this.buf.length - 1 || this.is == null;
        return this.markOffset;
    }
    
    public int getNumBytesMarked() {
        return this.startOffset - this.getFirstOffset();
    }
    
    public int getNumBytesPrefetched() {
        return this.readOffset - this.startOffset + 1;
    }
    
    private void readBetween(int min, int max) throws IOException {
        assert min >= 0 && min <= max;
        assert max <= this.remainingBytes;
        assert max <= this.getBufferSpaceAvailable();
        
        int numBytesRead = 0;
        while (numBytesRead < min) {
            int len = this.is.read(this.buf, this.readOffset + 1, max - numBytesRead);
            if (len <= 0) {
                throw new EOFException(String.format("File ended prematurely " + "(%d)", len));
            }
            numBytesRead += len;
            this.readOffset += len;
            this.remainingBytes -= len;
        }
        
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("prefetched %d bytes (min=%d, max=%d)", numBytesRead, min, max));
        }
        assert this.remainingBytes >= 0;
    }
    
    private void readZeroes(int amount) {
        assert amount <= this.remainingBytes;
        assert amount <= this.getBufferSpaceAvailable();
        
        Arrays.fill(this.buf, this.readOffset + 1, this.readOffset + 1 + amount, (byte) 0);
        this.readOffset += amount;
        this.remainingBytes -= amount;
    }
    
    public void setMarkRelativeToStart(int relativeOffset) {
        assert relativeOffset >= 0;
        // it's allowed to move 1 passed endOffset, as length is defined as:
        // endOffset - startOffset + 1
        assert this.startOffset + relativeOffset <= this.endOffset + 1;
        this.markOffset = this.startOffset + relativeOffset;
    }
    
    /*
     * slide window to right slideAmount number of bytes startOffset is increased
     * by slideAmount endOffset is set to the minimum of this fileView's window
     * size and the remaining number of bytes from the file markOffset position
     * relative to startOffset is left unchanged _errorOffset might be set if an IO
     * error occurred readOffset will be >= endOffset and marks the position of
     * the last available prefetched byte read data might be compacted if there's
     * not enough room left in the buffer
     */
    public void slide(int slideAmount) {
        assert slideAmount >= 0;
        assert slideAmount <= this.getWindowLength();
        
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("sliding %s %d", this, slideAmount));
        }
        
        this.startOffset += slideAmount;
        int windowLength = (int) Math.min(this.windowLength, this.getNumBytesPrefetched() + this.remainingBytes);
        assert windowLength >= 0;
        assert this.getNumBytesPrefetched() >= 0; // a negative value would imply a skip, which we don't (yet at least) support
        int minBytesToRead = windowLength - this.getNumBytesPrefetched();
        
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("next window length %d, minimum bytes to read %d", windowLength, minBytesToRead));
        }
        
        if (minBytesToRead > 0) {
            if (minBytesToRead > this.getBufferSpaceAvailable()) {
                this.compact();
            }
            
            int saveOffset = this.readOffset;
            try {
                if (this.ioError == null) {
                    this.readBetween(minBytesToRead, (int) Math.min(this.remainingBytes, this.getBufferSpaceAvailable()));
                } else {
                    this.readZeroes(minBytesToRead);
                }
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                this.ioError = e;
                int numBytesRead = this.readOffset - saveOffset;
                this.readZeroes(minBytesToRead - numBytesRead);
            }
        }
        
        this.endOffset = this.startOffset + windowLength - 1;
        
        assert this.getWindowLength() == windowLength;
        assert this.endOffset <= this.readOffset;
    }
    
    // TODO: the names startOffset and firstOffset are confusingly similar
    public int getStartOffset() {
        assert this.startOffset >= 0;
        assert this.startOffset <= this.buf.length - 1 || this.is == null;
        return this.startOffset;
    }
    
    @Override
    public String toString() {
        return String.format("%s (fileName=%s, startOffset=%d, markOffset=%d," + " endOffset=%d, windowLength=%d, " + "prefetchedOffset=%d, remainingBytes=%d)", this.getClass().getSimpleName(),
                this.fileName, this.startOffset, this.markOffset, this.getEndOffset(), this.getWindowLength(), this.readOffset, this.remainingBytes);
    }
    
    public int getTotalBytes() {
        return this.endOffset - this.getFirstOffset() + 1;
    }
    
    public byte valueAt(int offset) {
        assert offset >= this.getFirstOffset();
        assert offset <= this.endOffset;
        return this.buf[offset];
    }
    
    public int getWindowLength() {
        int length = this.endOffset - this.startOffset + 1;
        assert length >= 0;
        assert length <= this.windowLength : length + " maxWindowLength=" + this.windowLength;
        return length;
    }
}
