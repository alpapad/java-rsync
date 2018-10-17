/*
 * Rsync file list index encoding routine
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

import com.github.java.rsync.internal.session.Filelist;

public class IndexEncoderImpl implements IndexEncoder {
    private final Writable dst;
    private int prevNegativeWriteIndex = 1;
    private int prevPositiveWriteIndex = -1;

    public IndexEncoderImpl(Writable dst) {
        this.dst = dst;
    }

    // A diff of 1 - 253 is sent as a one-byte diff; a diff of 254 - 32767
    // or 0 is sent as a 0xFE + a two-byte diff; otherwise send 0xFE
    // and all 4 bytes of the (non-negative) num with the high-bit set.
    @Override
    public void encodeIndex(int index) throws ChannelException {
        if (index == Filelist.DONE) {
            dst.putByte((byte) 0);
            return;
        }

        int indexPositive;
        int diff;
        if (index >= 0) {
            indexPositive = index;
            diff = indexPositive - prevPositiveWriteIndex;
            prevPositiveWriteIndex = indexPositive;
        } else {
            indexPositive = -index;
            diff = indexPositive - prevNegativeWriteIndex;
            prevNegativeWriteIndex = indexPositive;
            dst.putByte((byte) 0xFF);
        }

        if (diff < 0xFE && diff > 0) {
            dst.putByte((byte) diff);
        } else if (diff < 0 || diff > 0x7FFF) {
            dst.putByte((byte) 0xFE);
            dst.putByte((byte) (indexPositive >> 24 | 0x80));
            dst.putByte((byte) indexPositive);
            dst.putByte((byte) (indexPositive >> 8));
            dst.putByte((byte) (indexPositive >> 16));
        } else {
            dst.putByte((byte) 0xFE);
            dst.putByte((byte) (diff >> 8));
            dst.putByte((byte) diff);
        }
    }
}
