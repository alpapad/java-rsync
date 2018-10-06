/*
 * Rsync channel with support for sending and buffer basic data to
 * peer
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;

import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.Environment;
import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.internal.util.Util;

public class BufferedOutputChannel implements Bufferable {
    private static final int DEFAULT_BUF_SIZE = 8 * 1024;
    protected final ByteBuffer _buffer;
    private long _numBytesWritten;
    private final WritableByteChannel _sinkChannel;
    
    public BufferedOutputChannel(WritableByteChannel sock) {
        this(sock, DEFAULT_BUF_SIZE);
    }
    
    public BufferedOutputChannel(WritableByteChannel sock, int bufferSize) {
        this._sinkChannel = sock;
        if (Environment.isAllocateDirect()) {
            this._buffer = ByteBuffer.allocateDirect(bufferSize);
        } else {
            this._buffer = ByteBuffer.allocate(bufferSize);
        }
        this._buffer.order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public void close() throws ChannelException {
        try {
            this.flush();
        } finally {
            try {
                this._sinkChannel.close();
            } catch (IOException e) {
                throw new ChannelException(e);
            }
        }
    }
    
    @Override
    public void flush() throws ChannelException {
        if (this.numBytesBuffered() > 0) {
            this._buffer.flip();
            this.send(this._buffer);
            this._buffer.clear();
        }
    }
    
    @Override
    public int numBytesBuffered() {
        return this._buffer.position();
    }
    
    public long numBytesWritten() {
        return this._numBytesWritten + this.numBytesBuffered();
    }
    
    @Override
    public void put(byte[] src, int offset, int length) throws ChannelException {
        this.put(ByteBuffer.wrap(src, offset, length));
    }
    
    @Override
    public void put(ByteBuffer src) throws ChannelException {
        while (src.hasRemaining()) {
            int l = Math.min(src.remaining(), this._buffer.remaining());
            if (l == 0) {
                this.flush();
            } else {
                ByteBuffer slice = Util.slice(src, src.position(), src.position() + l);
                this._buffer.put(slice);
                src.position(slice.position());
            }
        }
    }
    
    @Override
    public void putByte(byte b) throws ChannelException {
        if (this._buffer.remaining() < Consts.SIZE_BYTE) {
            this.flush();
        }
        this._buffer.put(b);
    }
    
    @Override
    public void putChar(char c) throws ChannelException {
        if (this._buffer.remaining() < Consts.SIZE_CHAR) {
            this.flush();
        }
        this._buffer.putChar(c);
    }
    
    @Override
    public void putInt(int i) throws ChannelException {
        if (this._buffer.remaining() < Consts.SIZE_INT) {
            this.flush();
        }
        this._buffer.putInt(i);
    }
    
    public void send(ByteBuffer buf) throws ChannelException {
        try {
            while (buf.hasRemaining()) {
                int count = this._sinkChannel.write(buf);
                if (count <= 0) {
                    throw new ChannelEOFException(String.format("channel write unexpectedly returned %d (EOF)", count));
                }
                this._numBytesWritten += count;
            }
        } catch (ClosedByInterruptException e) {
            throw new RuntimeInterruptException(e);
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }
}
