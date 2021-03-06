/*
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.java.rsync.test;

import java.nio.ByteBuffer;

import com.github.java.rsync.internal.channels.Readable;
import com.github.java.rsync.internal.util.Util;

public class ReadableByteBuffer implements Readable {
    private final ByteBuffer buf;
    
    public ReadableByteBuffer(ByteBuffer buf) {
        this.buf = buf;
    }
    
    @Override
    public void get(byte[] dst, int offset, int length) {
        this.buf.get(dst, offset, length);
    }
    
    @Override
    public ByteBuffer get(int numBytes) {
        return Util.slice(this.buf, 0, numBytes);
    }
    
    @Override
    public byte getByte() {
        return this.buf.get();
    }
    
    @Override
    public char getChar() {
        return this.buf.getChar();
    }
    
    @Override
    public int getInt() {
        return this.buf.getInt();
    }
    
    @Override
    public void skip(int numBytes) {
        this.buf.position(this.buf.position() + numBytes);
    }
}