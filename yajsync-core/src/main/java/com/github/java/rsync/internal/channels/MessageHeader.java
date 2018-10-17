/*
 * Rsync channel tagged messages header information (see
 * TaggedInputChannel and TaggedOutputChannel)
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014, 2016 Per Lundqvist
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

import java.util.Objects;

public class MessageHeader {
    private static final int MSG_MAX_LENGTH = 0xFFFFFF;
    private static final int MSG_TYPE_OFFSET = 7;
    
    /**
     * @throws IllegalArgumentException if tag value is invalid and has no matching
     *                                  MessageCode
     */
    public static MessageHeader fromTag(int tag) {
        int length = tag & MSG_MAX_LENGTH;
        MessageCode code = MessageCode.fromInt((tag >> 24) - MSG_TYPE_OFFSET); // throws IllegalArgumentException
        return new MessageHeader(code, length); // throws IllegalArgumentException
    }
    
    private final MessageCode code;
    
    private final int length;
    
    /**
     * @throws IllegalArgumentException if length < 0 or length > 0xFFFFFF)
     */
    public MessageHeader(MessageCode code, int length) {
        assert code != null;
        if (length < 0 || length > MSG_MAX_LENGTH) { // NOTE: 0-length messages are valid
            throw new IllegalArgumentException(String.format("Error: length %d out of range (0 <= length <= %d)", length, MSG_MAX_LENGTH));
        }
        this.code = code;
        this.length = length;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj != null && this.getClass() == obj.getClass()) {
            MessageHeader other = (MessageHeader) obj;
            return this.code == other.code && this.length == other.length;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.code, this.length);
    }
    
    public int getLength() {
        return this.length;
    }
    
    public MessageCode getMessageType() {
        return this.code;
    }
    
    @Override
    public String toString() {
        return String.format("%s %s length=%d", this.getClass().getSimpleName(), this.code, this.length);
    }
    
    public int toTag() {
        return MSG_TYPE_OFFSET + this.code.getValue() << 24 | this.length;
    }
}
