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
package com.github.java.rsync.internal.channels;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.util.Consts;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.Util;

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
            buf = ByteBuffer.allocateDirect(bufferSize);
        } else {
            buf = ByteBuffer.allocate(bufferSize);
        }
        buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    private void ensureMinimumPrefetched(int numBytes) throws ChannelException {
        assert numBytes <= buf.limit();
        ensureSpaceFor(numBytes);
        while (getNumBytesPrefetched() < numBytes) {
            readNextAvailable(buf);
        }
        assert getNumBytesPrefetched() >= numBytes;
    }

    private void ensureSpaceFor(int numBytes) {
        assert numBytes >= 0;
        assert numBytes <= buf.limit();
        if (readIndex + numBytes > buf.limit()) {
            ByteBuffer prefetched = getReadableSlice();
            assert readIndex == writeIndex();
            prefetched.compact();
            readIndex = 0;
            buf.position(prefetched.position());
        }
    }

    @Override
    public void get(ByteBuffer dst) throws ChannelException {
        ByteBuffer prefetched = nextReadableSlice(Math.min(getNumBytesPrefetched(), dst.remaining()));
        dst.put(prefetched);
        super.get(dst);
    }

    @Override
    public ByteBuffer get(int numBytes) throws ChannelException {
        assert numBytes >= 0;
        assert numBytes <= buf.capacity();
        ensureMinimumPrefetched(numBytes);
        ByteBuffer slice = nextReadableSlice(numBytes);
        assert slice.remaining() == numBytes;
        return slice;
    }

    @Override
    public byte getByte() throws ChannelException {
        ensureMinimumPrefetched(Consts.SIZE_BYTE);
        byte result = buf.get(readIndex);
        readIndex += Consts.SIZE_BYTE;
        return result;
    }

    @Override
    public char getChar() throws ChannelException {
        ensureMinimumPrefetched(Consts.SIZE_CHAR);
        char result = buf.getChar(readIndex);
        readIndex += Consts.SIZE_CHAR;
        return result;
    }

    @Override
    public int getInt() throws ChannelException {
        ensureMinimumPrefetched(Consts.SIZE_INT);
        int result = buf.getInt(readIndex);
        readIndex += Consts.SIZE_INT;
        return result;
    }

    @Override
    public int getNumBytesAvailable() {
        return super.getNumBytesAvailable() + getNumBytesPrefetched();
    }

    public int getNumBytesPrefetched() {
        assert readIndex <= writeIndex();
        return writeIndex() - readIndex;
    }

    private ByteBuffer getReadableSlice() {
        return nextReadableSlice(getNumBytesPrefetched());
    }

    private ByteBuffer nextReadableSlice(int length) {
        assert length >= 0;
        assert length <= getNumBytesPrefetched();
        ByteBuffer slice = Util.slice(buf, readIndex, readIndex + length);
        readIndex += length;
        assert readIndex <= buf.limit();
        assert readIndex <= writeIndex();
        assert slice.remaining() == length;
        return slice;
    }

    @Override
    public String toString() {
        return String.format("buf=%s, readIndex=%d prefetched=%d, " + "contents:%n\t%s", buf, readIndex, getNumBytesPrefetched(), Text.byteBufferToString(buf, readIndex, writeIndex()));
    }

    private int writeIndex() {
        return buf.position();
    }
}
