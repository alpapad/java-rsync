/*
 * Rsync network protocol version type
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

import java.util.Objects;

public final class ProtocolVersion implements Comparable<ProtocolVersion> {
    private final int major;
    private final int minor;
    
    public ProtocolVersion(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }
    
    @Override
    public int compareTo(ProtocolVersion other) {
        int res = this.major - other.major;
        if (res == 0) {
            return this.minor - other.minor;
        }
        return res;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o != null && this.getClass() == o.getClass()) {
            return this.compareTo((ProtocolVersion) o) == 0;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.major, this.minor);
    }
    
    public int getMajor() {
        return this.major;
    }
    
    public int getMinor() {
        return this.minor;
    }
    
    @Override
    public String toString() {
        return String.format("%d.%d", this.major, this.minor);
    }
}
