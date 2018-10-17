/*
 * Aggregation of one input channel and one buffered output channel
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

import java.nio.ByteBuffer;

public class BufferedDuplexChannel implements Readable, Bufferable {
    private final Readable inputChannel;
    private final Bufferable outputChannel;
    
    public BufferedDuplexChannel(Readable readable, Bufferable writable) {
        this.inputChannel = readable;
        this.outputChannel = writable;
    }
    
    @Override
    public void flush() throws ChannelException {
        this.outputChannel.flush();
    }
    
    @Override
    public void get(byte[] dst, int offset, int length) throws ChannelException {
        this.inputChannel.get(dst, offset, length);
    }
    
    @Override
    public ByteBuffer get(int numBytes) throws ChannelException {
        return this.inputChannel.get(numBytes);
    }
    
    @Override
    public byte getByte() throws ChannelException {
        return this.inputChannel.getByte();
    }
    
    @Override
    public char getChar() throws ChannelException {
        return this.inputChannel.getChar();
    }
    
    @Override
    public int getInt() throws ChannelException {
        return this.inputChannel.getInt();
    }
    
    @Override
    public int getNumBytesBuffered() {
        return this.outputChannel.getNumBytesBuffered();
    }
    
    @Override
    public void put(byte[] src, int offset, int length) throws ChannelException {
        this.outputChannel.put(src, offset, length);
    }
    
    @Override
    public void put(ByteBuffer src) throws ChannelException {
        this.outputChannel.put(src);
    }
    
    @Override
    public void putByte(byte b) throws ChannelException {
        this.outputChannel.putByte(b);
    }
    
    @Override
    public void putChar(char c) throws ChannelException {
        this.outputChannel.putChar(c);
    }
    
    @Override
    public void putInt(int i) throws ChannelException {
        this.outputChannel.putInt(i);
    }
    
    @Override
    public void skip(int numBytes) throws ChannelException {
        this.inputChannel.skip(numBytes);
    }
    
    @Override
    public String toString() {
        return String.format("%s %s %s", this.getClass().getSimpleName(), this.inputChannel, this.outputChannel);
    }
}
