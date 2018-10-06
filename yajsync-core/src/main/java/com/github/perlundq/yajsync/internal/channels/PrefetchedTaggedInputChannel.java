/*
 * Rsync Channel with support for tagged rsync Messages sent from peer
 * and automatically prefetching of available amount
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
package com.github.perlundq.yajsync.internal.channels;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.Environment;
import com.github.perlundq.yajsync.internal.util.Util;

public class PrefetchedTaggedInputChannel extends TaggedInputChannel {
    private static final int DEFAULT_BUF_SIZE = 8 * 1024; // TODO: make buffer size configurable
    private final ByteBuffer _buf; // never flipped, never marked and its limit is never changed
    private int _readIndex = 0;
    
    public PrefetchedTaggedInputChannel(ReadableByteChannel sock, MessageHandler handler) {
        this(sock, handler, DEFAULT_BUF_SIZE);
    }
    
    public PrefetchedTaggedInputChannel(ReadableByteChannel sock, MessageHandler handler, int bufferSize) {
        super(sock, handler);
        if (Environment.isAllocateDirect()) {
            this._buf = ByteBuffer.allocateDirect(bufferSize);
        } else {
            this._buf = ByteBuffer.allocate(bufferSize);
        }
        this._buf.order(ByteOrder.LITTLE_ENDIAN);
    }
    
    private void ensureMinimumPrefetched(int numBytes) throws ChannelException {
        assert numBytes <= this._buf.limit();
        this.ensureSpaceFor(numBytes);
        while (this.numBytesPrefetched() < numBytes) {
            this.readNextAvailable(this._buf);
        }
        assert this.numBytesPrefetched() >= numBytes;
    }
    
    private void ensureSpaceFor(int numBytes) {
        assert numBytes >= 0;
        assert numBytes <= this._buf.limit();
        if (this._readIndex + numBytes > this._buf.limit()) {
            ByteBuffer prefetched = this.readableSlice();
            assert this._readIndex == this.writeIndex();
            prefetched.compact();
            this._readIndex = 0;
            this._buf.position(prefetched.position());
        }
    }
    
    @Override
    public void get(ByteBuffer dst) throws ChannelException {
        ByteBuffer prefetched = this.nextReadableSlice(Math.min(this.numBytesPrefetched(), dst.remaining()));
        dst.put(prefetched);
        super.get(dst);
    }
    
    @Override
    public ByteBuffer get(int numBytes) throws ChannelException {
        assert numBytes >= 0;
        assert numBytes <= this._buf.capacity();
        this.ensureMinimumPrefetched(numBytes);
        ByteBuffer slice = this.nextReadableSlice(numBytes);
        assert slice.remaining() == numBytes;
        return slice;
    }
    
    @Override
    public byte getByte() throws ChannelException {
        this.ensureMinimumPrefetched(Consts.SIZE_BYTE);
        byte result = this._buf.get(this._readIndex);
        this._readIndex += Consts.SIZE_BYTE;
        return result;
    }
    
    @Override
    public char getChar() throws ChannelException {
        this.ensureMinimumPrefetched(Consts.SIZE_CHAR);
        char result = this._buf.getChar(this._readIndex);
        this._readIndex += Consts.SIZE_CHAR;
        return result;
    }
    
    @Override
    public int getInt() throws ChannelException {
        this.ensureMinimumPrefetched(Consts.SIZE_INT);
        int result = this._buf.getInt(this._readIndex);
        this._readIndex += Consts.SIZE_INT;
        return result;
    }
    
    private ByteBuffer nextReadableSlice(int length) {
        assert length >= 0;
        assert length <= this.numBytesPrefetched();
        ByteBuffer slice = Util.slice(this._buf, this._readIndex, this._readIndex + length);
        this._readIndex += length;
        assert this._readIndex <= this._buf.limit();
        assert this._readIndex <= this.writeIndex();
        assert slice.remaining() == length;
        return slice;
    }
    
    @Override
    public int numBytesAvailable() {
        return super.numBytesAvailable() + this.numBytesPrefetched();
    }
    
    public int numBytesPrefetched() {
        assert this._readIndex <= this.writeIndex();
        return this.writeIndex() - this._readIndex;
    }
    
    private ByteBuffer readableSlice() {
        return this.nextReadableSlice(this.numBytesPrefetched());
    }
    
    @Override
    public String toString() {
        return String.format("buf=%s, readIndex=%d prefetched=%d, " + "contents:%n\t%s", this._buf, this._readIndex, this.numBytesPrefetched(),
                Text.byteBufferToString(this._buf, this._readIndex, this.writeIndex()));
    }
    
    private int writeIndex() {
        return this._buf.position();
    }
}
