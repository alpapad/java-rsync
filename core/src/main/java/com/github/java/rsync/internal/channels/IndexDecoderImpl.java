/*
 * Rsync file list index decoding routine
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

import com.github.java.rsync.internal.session.Filelist;
import com.github.java.rsync.internal.util.BitOps;

public class IndexDecoderImpl implements IndexDecoder {
    private int prevNegativeReadIndex = 1;
    private int prevPositiveReadIndex = -1;
    private final byte[] readBuf = new byte[4];
    private final Readable src;

    public IndexDecoderImpl(Readable src) {
        this.src = src;
    }

    @Override
    public int decodeIndex() throws ChannelException {
        readBuf[0] = src.getByte();
        if (readBuf[0] == 0) {
            return Filelist.DONE;
        }

        int prevVal;
        boolean setNegative = false;
        if ((0xFF & readBuf[0]) == 0xFF) {
            readBuf[0] = src.getByte();
            prevVal = prevNegativeReadIndex;
            setNegative = true;
        } else {
            prevVal = prevPositiveReadIndex;
        }

        int value;
        if ((0xFF & readBuf[0]) == 0xFE) {
            src.get(readBuf, 0, 2);
            if ((0x80 & readBuf[0]) != 0) {
                readBuf[3] = (byte) (~0x80 & readBuf[0]);
                readBuf[0] = readBuf[1];
                src.get(readBuf, 1, 2);
                value = BitOps.toBigEndianInt(readBuf, 0);
            } else {
                value = ((0xFF & readBuf[0]) << 8) + (0xFF & readBuf[1]) + prevVal;
            }
        } else {
            value = (0xFF & readBuf[0]) + prevVal;
        }

        if (setNegative) {
            prevNegativeReadIndex = value;
            return -value;
        } else {
            prevPositiveReadIndex = value;
            return value;
        }
    }
}