/*
 * Rsync Channel with support for reading basic data from peer
 *
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
package com.github.perlundq.yajsync.internal.channels;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ReadableByteChannel;

import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.Environment;
import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;

public class SimpleInputChannel implements Readable {
    private static final int DEFAULT_BUF_SIZE = 1024;
    private final ByteBuffer _byteBuf;
    private final ByteBuffer _charBuf;
    private final ByteBuffer _intBuf;
    private long _numBytesRead;
    private final ReadableByteChannel _sourceChannel;
    
    public SimpleInputChannel(ReadableByteChannel sock) {
        assert sock != null;
        this._sourceChannel = sock;
        if (Environment.isAllocateDirect()) {
            this._byteBuf = ByteBuffer.allocateDirect(Consts.SIZE_BYTE);
            this._charBuf = ByteBuffer.allocateDirect(Consts.SIZE_CHAR);
            this._intBuf = ByteBuffer.allocateDirect(Consts.SIZE_INT);
        } else {
            this._byteBuf = ByteBuffer.allocate(Consts.SIZE_BYTE);
            this._charBuf = ByteBuffer.allocate(Consts.SIZE_CHAR);
            this._intBuf = ByteBuffer.allocate(Consts.SIZE_INT);
        }
        this._charBuf.order(ByteOrder.LITTLE_ENDIAN);
        this._intBuf.order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public void close() throws ChannelException {
        try {
            this._sourceChannel.close();
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }
    
    @Override
    public void get(byte[] dst, int offset, int length) throws ChannelException {
        this.get(ByteBuffer.wrap(dst, offset, length));
    }
    
    protected void get(ByteBuffer dst) throws ChannelException {
        try {
            while (dst.hasRemaining()) {
                int count = this._sourceChannel.read(dst);
                if (count <= 0) {
                    throw new ChannelEOFException(String.format("channel read unexpectedly returned %d (EOF)", count));
                }
                this._numBytesRead += count;
            }
        } catch (EOFException e) {
            throw new ChannelEOFException(e);
        } catch (ClosedByInterruptException e) {
            throw new RuntimeInterruptException(e);
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }
    
    @Override
    public ByteBuffer get(int numBytes) throws ChannelException {
        ByteBuffer result = ByteBuffer.allocate(numBytes);
        this.get(result);
        result.flip();
        return result;
    }
    
    @Override
    public byte getByte() throws ChannelException {
        this._byteBuf.clear();
        this.get(this._byteBuf);
        this._byteBuf.flip();
        return this._byteBuf.get();
    }
    
    @Override
    public char getChar() throws ChannelException {
        this._charBuf.clear();
        this.get(this._charBuf);
        this._charBuf.flip();
        return this._charBuf.getChar();
    }
    
    @Override
    public int getInt() throws ChannelException {
        this._intBuf.clear();
        this.get(this._intBuf);
        this._intBuf.flip();
        return this._intBuf.getInt();
    }
    
    public long numBytesRead() {
        return this._numBytesRead;
    }
    
    @Override
    public void skip(int numBytes) throws ChannelException {
        assert numBytes >= 0;
        int numBytesSkipped = 0;
        while (numBytesSkipped < numBytes) {
            int chunkSize = Math.min(numBytes - numBytesSkipped, DEFAULT_BUF_SIZE);
            this.get(chunkSize); // ignore result
            numBytesSkipped += chunkSize;
        }
    }
}
