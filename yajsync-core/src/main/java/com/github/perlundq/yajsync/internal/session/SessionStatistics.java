/*
 * rsync network protocol statistics
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
package com.github.perlundq.yajsync.internal.session;

import com.github.perlundq.yajsync.Statistics;

public class SessionStatistics implements Statistics {
    long _fileListBuildTime;
    long _fileListTransferTime;
    // NOTE: package private fields
    int _numFiles;
    int _numTransferredFiles;
    long _totalBytesRead;
    long _totalBytesWritten;
    long _totalFileListSize;
    long _totalFileSize;
    long _totalLiteralSize;
    long _totalMatchedSize;
    long _totalTransferredSize;
    
    @Override
    public long fileListBuildTime() {
        return this._fileListBuildTime;
    }
    
    @Override
    public long fileListTransferTime() {
        return this._fileListTransferTime;
    }
    
    @Override
    public int numFiles() {
        return this._numFiles;
    }
    
    @Override
    public int numTransferredFiles() {
        return this._numTransferredFiles;
    }
    
    @Override
    public long totalBytesRead() {
        return this._totalBytesRead;
    }
    
    @Override
    public long totalBytesWritten() {
        return this._totalBytesWritten;
    }
    
    @Override
    public long totalFileListSize() {
        return this._totalFileListSize;
    }
    
    @Override
    public long totalFileSize() {
        return this._totalFileSize;
    }
    
    @Override
    public long totalLiteralSize() {
        return this._totalLiteralSize;
    }
    
    @Override
    public long totalMatchedSize() {
        return this._totalMatchedSize;
    }
    
    @Override
    public long totalTransferredSize() {
        return this._totalTransferredSize;
    }
}
