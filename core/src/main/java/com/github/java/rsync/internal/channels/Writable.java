/*
 * Interface for types support sending rsync basic data to peer
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
package com.github.java.rsync.internal.channels;

import java.nio.ByteBuffer;

public interface Writable {
    void put(byte[] src, int offset, int length) throws ChannelException;

    void put(ByteBuffer src) throws ChannelException;

    void putByte(byte b) throws ChannelException;

    void putChar(char c) throws ChannelException;

    void putInt(int i) throws ChannelException;
}