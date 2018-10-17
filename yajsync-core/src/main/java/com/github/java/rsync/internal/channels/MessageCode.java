/*
 * Rsync channel tagged message information flag
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

import java.util.HashMap;
import java.util.Map;

public enum MessageCode {
    CLIENT(7), DATA(0), DELETED(101), /* remote logging */
    DONE(86), /* remote logging */
    ERROR(3), /* remote logging */
    ERROR_SOCKET(5), /* sibling logging */
    ERROR_UTF8(8), ERROR_XFER(1), /* sibling logging */
    FLIST(20), /* sibling logging */
    FLIST_EOF(21), /* reprocess indicated flist index */
    INFO(2), /* extra file list over sibling socket */
    IO_ERROR(22), /* we've transmitted all the file lists */
    LOG(6), /* the sending side had an I/O error */
    NO_SEND(102), /* a do-nothing message */
    NOOP(42), /* current phase is done */
    REDO(9), /* successfully updated indicated flist index */
    SUCCESS(100), /* successfully deleted a file on receiving side */
    WARNING(4); /* sender failed to open a file we wanted */
    
    private static final Map<Integer, MessageCode> map = new HashMap<>();
    static {
        for (MessageCode message : MessageCode.values()) {
            map.put(message.getValue(), message);
        }
    }
    
    /**
     * @throws IllegalArgumentException
     */
    public static MessageCode fromInt(int value) {
        MessageCode message = map.get(value);
        if (message == null) {
            throw new IllegalArgumentException(String.format("Error: unknown tag for %d", value));
        }
        return message;
    }
    
    private final int value;
    
    MessageCode(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return this.value;
    }
}
