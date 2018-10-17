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
    private final ByteBuffer buf; // never flipped, never marked and its limit is never changed
    private int readIndex = 0;
    
    public PrefetchedTaggedInputChannel(ReadableByteChannel sock, MessageHandler handler) {
        this(sock, handler, DEFAULT_BUF_SIZE);
    }
    
    public PrefetchedTaggedInputChannel(ReadableByteChannel sock, MessageHandler handler, int bufferSize) {
        super(sock, handler);
        if (Environment.isAllocateDirect()) {
            this.buf = ByteBuffer.allocateDirect(bufferSize);
        } else {
            this.buf = ByteBuffer.allocate(bufferSize);
        }
        this.buf.order(ByteOrder.LITTLE_ENDIAN);
    }
    
    private void ensureMinimumPrefetched(int numBytes) throws ChannelException {
        assert numBytes <= this.buf.limit();
        this.ensureSpaceFor(numBytes);
        while (this.getNumBytesPrefetched() < numBytes) {
            this.readNextAvailable(this.buf);
        }
        assert this.getNumBytesPrefetched() >= numBytes;
    }
    
    private void ensureSpaceFor(int numBytes) {
        assert numBytes >= 0;
        assert numBytes <= this.buf.limit();
        if (this.readIndex + numBytes > this.buf.limit()) {
            ByteBuffer prefetched = this.getReadableSlice();
            assert this.readIndex == this.writeIndex();
            prefetched.compact();
            this.readIndex = 0;
            this.buf.position(prefetched.position());
        }
    }
    
    @Override
    public void get(ByteBuffer dst) throws ChannelException {
        ByteBuffer prefetched = this.nextReadableSlice(Math.min(this.getNumBytesPrefetched(), dst.remaining()));
        dst.put(prefetched);
        super.get(dst);
    }
    
    @Override
    public ByteBuffer get(int numBytes) throws ChannelException {
        assert numBytes >= 0;
        assert numBytes <= this.buf.capacity();
        this.ensureMinimumPrefetched(numBytes);
        ByteBuffer slice = this.nextReadableSlice(numBytes);
        assert slice.remaining() == numBytes;
        return slice;
    }
    
    @Override
    public byte getByte() throws ChannelException {
        this.ensureMinimumPrefetched(Consts.SIZE_BYTE);
        byte result = this.buf.get(this.readIndex);
        this.readIndex += Consts.SIZE_BYTE;
        return result;
    }
    
    @Override
    public char getChar() throws ChannelException {
        this.ensureMinimumPrefetched(Consts.SIZE_CHAR);
        char result = this.buf.getChar(this.readIndex);
        this.readIndex += Consts.SIZE_CHAR;
        return result;
    }
    
    @Override
    public int getInt() throws ChannelException {
        this.ensureMinimumPrefetched(Consts.SIZE_INT);
        int result = this.buf.getInt(this.readIndex);
        this.readIndex += Consts.SIZE_INT;
        return result;
    }
    
    private ByteBuffer nextReadableSlice(int length) {
        assert length >= 0;
        assert length <= this.getNumBytesPrefetched();
        ByteBuffer slice = Util.slice(this.buf, this.readIndex, this.readIndex + length);
        this.readIndex += length;
        assert this.readIndex <= this.buf.limit();
        assert this.readIndex <= this.writeIndex();
        assert slice.remaining() == length;
        return slice;
    }
    
    @Override
    public int getNumBytesAvailable() {
        return super.getNumBytesAvailable() + this.getNumBytesPrefetched();
    }
    
    public int getNumBytesPrefetched() {
        assert this.readIndex <= this.writeIndex();
        return this.writeIndex() - this.readIndex;
    }
    
    private ByteBuffer getReadableSlice() {
        return this.nextReadableSlice(this.getNumBytesPrefetched());
    }
    
    @Override
    public String toString() {
        return String.format("buf=%s, readIndex=%d prefetched=%d, " + "contents:%n\t%s", this.buf, this.readIndex, this.getNumBytesPrefetched(),
                Text.byteBufferToString(this.buf, this.readIndex, this.writeIndex()));
    }
    
    private int writeIndex() {
        return this.buf.position();
    }
}
