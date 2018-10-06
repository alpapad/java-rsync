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
package com.github.perlundq.yajsync.internal.io;

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

import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;

public class FileView implements AutoCloseable {
    private static final Logger _log = Logger.getLogger(FileView.class.getName());
    public final static int DEFAULT_BLOCK_SIZE = 8 * 1024;
    private final byte[] _buf;
    private int _endOffset = -1; // length == _endOffset - _startOffset + 1
    private final String _fileName;
    private IOException _ioError = null;
    private final InputStream _is;
    private int _markOffset = -1;
    private int _readOffset = -1;
    private long _remainingBytes;
    private int _startOffset = 0;
    private final int _windowLength; // size of sliding window (<= _buf.length)
    
    public FileView(Path path, long fileSize, int windowLength, int bufferSize) throws FileViewOpenFailed {
        assert path != null;
        assert fileSize >= 0;
        assert windowLength >= 0;
        assert bufferSize >= 0;
        assert windowLength <= bufferSize;
        
        try {
            this._fileName = path.toString();
            this._remainingBytes = fileSize;
            
            if (fileSize > 0) {
                this._is = Files.newInputStream(path);
                this._windowLength = windowLength;
                this._buf = new byte[bufferSize];
                this.slide(0);
                assert this._startOffset == 0;
                assert this._endOffset >= 0;
            } else {
                this._is = null;
                this._windowLength = 0;
                this._buf = new byte[0];
            }
            
        } catch (FileNotFoundException | NoSuchFileException e) { // TODO: which exception should we really catch
            throw new FileViewNotFound(e.getMessage());
        } catch (IOException e) {
            throw new FileViewOpenFailed(e.getMessage());
        }
    }
    
    public byte[] array() {
        return this._buf;
    }
    
    private int bufferSpaceAvailable() {
        assert this._readOffset <= this._buf.length - 1;
        return this._buf.length - 1 - this._readOffset;
    }
    
    @Override
    public void close() throws FileViewException {
        if (this._is != null) {
            try {
                this._is.close();
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                throw new FileViewException(e);
            }
        }
        
        if (this._ioError != null) {
            throw new FileViewException(this._ioError);
        }
    }
    
    private void compact() {
        assert this.numBytesPrefetched() >= 0;
        assert this.totalBytes() >= 0; // unless we'd support skipping
        
        int shiftOffset = this.firstOffset();
        int numShifts = this.numBytesMarked() + this.numBytesPrefetched();
        
        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("compact of %s before - buf[%d] %d bytes to buf[0], " + "buf.length = %d", this, shiftOffset, numShifts, this._buf.length));
        }
        
        System.arraycopy(this._buf, shiftOffset, this._buf, 0, numShifts);
        this._startOffset -= shiftOffset;
        this._endOffset -= shiftOffset;
        this._readOffset -= shiftOffset;
        if (this._markOffset >= 0) {
            this._markOffset -= shiftOffset;
        }
        
        assert this._startOffset >= 0;
        assert this._endOffset >= -1;
        assert this._readOffset >= -1;
        assert this._markOffset >= -1;
        
        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("compacted %d bytes, result after: %s", numShifts, this));
        }
    }
    
    public int endOffset() {
        assert this._endOffset >= -1;
        return this._endOffset;
    }
    
    // TODO: the names startOffset and firstOffset are confusingly similar
    public int firstOffset() {
        assert this._startOffset >= 0;
        assert this._markOffset >= -1;
        return this._markOffset >= 0 ? Math.min(this._startOffset, this._markOffset) : this._startOffset;
    }
    
    public boolean isFull() {
        assert this.totalBytes() <= this._buf.length;
        return this.totalBytes() == this._buf.length; // || windowLength() == 0 && _remainingBytes == 0
    }
    
    // might return -1
    public int markOffset() {
        assert this._markOffset >= -1;
        assert this._markOffset <= this._buf.length - 1 || this._is == null;
        return this._markOffset;
    }
    
    public int numBytesMarked() {
        return this._startOffset - this.firstOffset();
    }
    
    public int numBytesPrefetched() {
        return this._readOffset - this._startOffset + 1;
    }
    
    private void readBetween(int min, int max) throws IOException {
        assert min >= 0 && min <= max;
        assert max <= this._remainingBytes;
        assert max <= this.bufferSpaceAvailable();
        
        int numBytesRead = 0;
        while (numBytesRead < min) {
            int len = this._is.read(this._buf, this._readOffset + 1, max - numBytesRead);
            if (len <= 0) {
                throw new EOFException(String.format("File ended prematurely " + "(%d)", len));
            }
            numBytesRead += len;
            this._readOffset += len;
            this._remainingBytes -= len;
        }
        
        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("prefetched %d bytes (min=%d, max=%d)", numBytesRead, min, max));
        }
        assert this._remainingBytes >= 0;
    }
    
    private void readZeroes(int amount) {
        assert amount <= this._remainingBytes;
        assert amount <= this.bufferSpaceAvailable();
        
        Arrays.fill(this._buf, this._readOffset + 1, this._readOffset + 1 + amount, (byte) 0);
        this._readOffset += amount;
        this._remainingBytes -= amount;
    }
    
    public void setMarkRelativeToStart(int relativeOffset) {
        assert relativeOffset >= 0;
        // it's allowed to move 1 passed _endOffset, as length is defined as:
        // _endOffset - _startOffset + 1
        assert this._startOffset + relativeOffset <= this._endOffset + 1;
        this._markOffset = this._startOffset + relativeOffset;
    }
    
    /*
     * slide window to right slideAmount number of bytes _startOffset is increased
     * by slideAmount _endOffset is set to the minimum of this fileView's window
     * size and the remaining number of bytes from the file _markOffset position
     * relative to _startOffset is left unchanged _errorOffset might be set if an IO
     * error occurred _readOffset will be >= _endOffset and marks the position of
     * the last available prefetched byte read data might be compacted if there's
     * not enough room left in the buffer
     */
    public void slide(int slideAmount) {
        assert slideAmount >= 0;
        assert slideAmount <= this.windowLength();
        
        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("sliding %s %d", this, slideAmount));
        }
        
        this._startOffset += slideAmount;
        int windowLength = (int) Math.min(this._windowLength, this.numBytesPrefetched() + this._remainingBytes);
        assert windowLength >= 0;
        assert this.numBytesPrefetched() >= 0; // a negative value would imply a skip, which we don't (yet at least) support
        int minBytesToRead = windowLength - this.numBytesPrefetched();
        
        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("next window length %d, minimum bytes to read %d", windowLength, minBytesToRead));
        }
        
        if (minBytesToRead > 0) {
            if (minBytesToRead > this.bufferSpaceAvailable()) {
                this.compact();
            }
            
            int saveOffset = this._readOffset;
            try {
                if (this._ioError == null) {
                    this.readBetween(minBytesToRead, (int) Math.min(this._remainingBytes, this.bufferSpaceAvailable()));
                } else {
                    this.readZeroes(minBytesToRead);
                }
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                this._ioError = e;
                int numBytesRead = this._readOffset - saveOffset;
                this.readZeroes(minBytesToRead - numBytesRead);
            }
        }
        
        this._endOffset = this._startOffset + windowLength - 1;
        
        assert this.windowLength() == windowLength;
        assert this._endOffset <= this._readOffset;
    }
    
    // TODO: the names startOffset and firstOffset are confusingly similar
    public int startOffset() {
        assert this._startOffset >= 0;
        assert this._startOffset <= this._buf.length - 1 || this._is == null;
        return this._startOffset;
    }
    
    @Override
    public String toString() {
        return String.format("%s (fileName=%s, startOffset=%d, markOffset=%d," + " endOffset=%d, windowLength=%d, " + "prefetchedOffset=%d, remainingBytes=%d)", this.getClass().getSimpleName(),
                this._fileName, this._startOffset, this._markOffset, this.endOffset(), this.windowLength(), this._readOffset, this._remainingBytes);
    }
    
    public int totalBytes() {
        return this._endOffset - this.firstOffset() + 1;
    }
    
    public byte valueAt(int offset) {
        assert offset >= this.firstOffset();
        assert offset <= this._endOffset;
        return this._buf[offset];
    }
    
    public int windowLength() {
        int length = this._endOffset - this._startOffset + 1;
        assert length >= 0;
        assert length <= this._windowLength : length + " maxWindowLength=" + this._windowLength;
        return length;
    }
}
