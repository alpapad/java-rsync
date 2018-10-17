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
package com.github.java.rsync.internal.session;

import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.User;

class FileInfoCache {
    private byte[] prevFileName = {};
    private Group prevGroup;
    private long prevLastModified = 0;
    private int prevMajor = -1;
    private int prevMode = -1;
    private User prevUser;
    
    public FileInfoCache() {
    }
    
    public byte[] getPrevFileNameBytes() {
        return this.prevFileName;
    }
    
    public Group getPrevGroup() {
        return this.prevGroup;
    }
    
    public long getPrevLastModified() {
        return this.prevLastModified;
    }
    
    public int getPrevMajor() {
        return this.prevMajor;
    }
    
    public int getPrevMode() {
        return this.prevMode;
    }
    
    public User getPrevUser() {
        return this.prevUser;
    }
    
    public void setPrevFileNameBytes(byte[] prevFileName) {
        this.prevFileName = prevFileName;
    }
    
    public void setPrevGroup(Group group) {
        this.prevGroup = group;
    }
    
    public void setPrevLastModified(long prevLastModified) {
        this.prevLastModified = prevLastModified;
    }
    
    public void setPrevMajor(int major) {
        this.prevMajor = major;
    }
    
    public void setPrevMode(int prevMode) {
        this.prevMode = prevMode;
    }
    
    public void setPrevUser(User user) {
        this.prevUser = user;
    }
}
