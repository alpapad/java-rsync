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
package com.github.perlundq.yajsync.internal.channels;

import com.github.perlundq.yajsync.internal.session.Filelist;
import com.github.perlundq.yajsync.internal.util.BitOps;

public class IndexDecoderImpl implements IndexDecoder {
    private int _prevNegativeReadIndex = 1;
    private int _prevPositiveReadIndex = -1;
    private final byte[] _readBuf = new byte[4];
    private final Readable _src;
    
    public IndexDecoderImpl(Readable src) {
        this._src = src;
    }
    
    @Override
    public int decodeIndex() throws ChannelException {
        this._readBuf[0] = this._src.getByte();
        if (this._readBuf[0] == 0) {
            return Filelist.DONE;
        }
        
        int prevVal;
        boolean setNegative = false;
        if ((0xFF & this._readBuf[0]) == 0xFF) {
            this._readBuf[0] = this._src.getByte();
            prevVal = this._prevNegativeReadIndex;
            setNegative = true;
        } else {
            prevVal = this._prevPositiveReadIndex;
        }
        
        int value;
        if ((0xFF & this._readBuf[0]) == 0xFE) {
            this._src.get(this._readBuf, 0, 2);
            if ((0x80 & this._readBuf[0]) != 0) {
                this._readBuf[3] = (byte) (~0x80 & this._readBuf[0]);
                this._readBuf[0] = this._readBuf[1];
                this._src.get(this._readBuf, 1, 2);
                value = BitOps.toBigEndianInt(this._readBuf, 0);
            } else {
                value = ((0xFF & this._readBuf[0]) << 8) + (0xFF & this._readBuf[1]) + prevVal;
            }
        } else {
            value = (0xFF & this._readBuf[0]) + prevVal;
        }
        
        if (setNegative) {
            this._prevNegativeReadIndex = value;
            return -value;
        } else {
            this._prevPositiveReadIndex = value;
            return value;
        }
    }
}