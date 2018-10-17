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
package com.github.java.rsync.internal.session;

import com.github.java.rsync.Statistics;

public class SessionStatistics implements Statistics {
    long fileListBuildTime;
    long fileListTransferTime;
    // NOTE: package private fields
    int numFiles;
    int numTransferredFiles;
    long totalBytesRead;
    long totalBytesWritten;
    long totalFileListSize;
    long totalFileSize;
    long totalLiteralSize;
    long totalMatchedSize;
    long totalTransferredSize;

    @Override
    public long getFileListBuildTime() {
        return fileListBuildTime;
    }

    @Override
    public long getFileListTransferTime() {
        return fileListTransferTime;
    }

    @Override
    public int getNumFiles() {
        return numFiles;
    }

    @Override
    public int getNumTransferredFiles() {
        return numTransferredFiles;
    }

    @Override
    public long getTotalBytesRead() {
        return totalBytesRead;
    }

    @Override
    public long getTotalBytesWritten() {
        return totalBytesWritten;
    }

    @Override
    public long getTotalFileListSize() {
        return totalFileListSize;
    }

    @Override
    public long getTotalFileSize() {
        return totalFileSize;
    }

    @Override
    public long getTotalLiteralSize() {
        return totalLiteralSize;
    }

    @Override
    public long getTotalMatchedSize() {
        return totalMatchedSize;
    }

    @Override
    public long getTotalTransferredSize() {
        return totalTransferredSize;
    }
}
