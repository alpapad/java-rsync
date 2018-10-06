/*
 * Copyright (C) 2014-2016 Per Lundqvist
 * Copyright (C) 2015-2016 Florian Sager
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
package com.github.perlundq.yajsync.attr;

import java.util.Objects;

import com.github.perlundq.yajsync.internal.util.Environment;

public final class Group {
    public static final int ID_MAX = 65535;
    private static final int ID_NOBODY = ID_MAX - 1;
    
    private static final int MAX_NAME_LENGTH = 255;
    public static final Group NOBODY = new Group("nobody", ID_NOBODY);
    public static final Group ROOT = new Group("root", 0);
    public static final Group JVM_GROUP = new Group(Environment.getGroupName(), Environment.getGroupId());
    
    private final int _id;
    private final String _name;
    
    public Group(String name, int id) {
        if (name == null) {
            throw new IllegalArgumentException();
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException();
        }
        if (id < 0 || id > ID_MAX) {
            throw new IllegalArgumentException();
        }
        this._name = name;
        this._id = id;
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other != null && this.getClass() == other.getClass()) {
            Group otherGroup = (Group) other;
            return this._id == otherGroup._id && this._name.equals(otherGroup._name);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this._name, this._id);
    }
    
    public int id() {
        return this._id;
    }
    
    public String name() {
        return this._name;
    }
    
    @Override
    public String toString() {
        return String.format("%s (%s, %d)", this.getClass().getSimpleName(), this._name, this._id);
    }
}
