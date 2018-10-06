/*
 * Caching of previous sent file meta data for minimising
 * communication between Sender and Receiver
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
package com.github.perlundq.yajsync.internal.session;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.User;

class FileInfoCache {
    private byte[] _prevFileName = {};
    private Group _prevGroup;
    private long _prevLastModified = 0;
    private int _prevMajor = -1;
    private int _prevMode = -1;
    private User _prevUser;
    
    public FileInfoCache() {
    }
    
    public byte[] getPrevFileNameBytes() {
        return this._prevFileName;
    }
    
    public Group getPrevGroupOrNull() {
        return this._prevGroup;
    }
    
    public long getPrevLastModified() {
        return this._prevLastModified;
    }
    
    public int getPrevMajor() {
        return this._prevMajor;
    }
    
    public int getPrevMode() {
        return this._prevMode;
    }
    
    public User getPrevUserOrNull() {
        return this._prevUser;
    }
    
    public void setPrevFileNameBytes(byte[] prevFileName) {
        this._prevFileName = prevFileName;
    }
    
    public void setPrevGroup(Group group) {
        this._prevGroup = group;
    }
    
    public void setPrevLastModified(long prevLastModified) {
        this._prevLastModified = prevLastModified;
    }
    
    public void setPrevMajor(int major) {
        this._prevMajor = major;
    }
    
    public void setPrevMode(int prevMode) {
        this._prevMode = prevMode;
    }
    
    public void setPrevUser(User user) {
        this._prevUser = user;
    }
}
