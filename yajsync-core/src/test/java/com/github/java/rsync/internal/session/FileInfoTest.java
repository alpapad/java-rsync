/*
 * Copyright (C) 2016 Per Lundqvist
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.util.FileOps;

public class FileInfoTest {
    
    RsyncFileAttributes dirAttrs = new RsyncFileAttributes(FileOps.S_IFDIR | 0755, 0, 0, User.JVM_USER, Group.JVM_GROUP);
    FileInfoImpl dotDir = new FileInfoImpl(".", ".".getBytes(), this.dirAttrs);
    String dotDirPathName = ".";
    RsyncFileAttributes _fileAttrs = new RsyncFileAttributes(FileOps.S_IFREG | 0644, 0, 0, User.JVM_USER, Group.JVM_GROUP);
    
    @Test
    public void testSortDotDirBeforeNonDotDir() {
        String str = ".a";
        FileInfo f = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        assertFalse(this.dotDir.equals(f));
        assertTrue(this.dotDir.compareTo(f) <= -1);
    }
    
    @Test
    public void testSortDotDirBeforeNonDotDir2() {
        String str = "...";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        assertFalse(this.dotDir.equals(f));
        assertTrue(this.dotDir.compareTo(f) <= -1);
    }
    
    @Test
    public void testSortDotDirBeforeNonDotDir3() {
        String str = "...";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        assertFalse(this.dotDir.equals(f));
        assertTrue(this.dotDir.compareTo(f) <= -1);
    }
    
    @Test
    public void testSortDotDirBeforeNonDotDir4() {
        String str = "a.";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        assertFalse(this.dotDir.equals(f));
        assertTrue(this.dotDir.compareTo(f) <= -1);
    }
    
    // test empty throws illegalargumentexception
    // test dot is always a directory
    
    /*
     * ./ ... ..../ .a a. a.. ..a .a.
     *
     * a. a../ .a/ .a/b b/ b/. c cc c.c
     */
    
    @Test
    public void testSortDotDirWithDotDirEqual() {
        assertTrue(this.dotDir.equals(this.dotDir));
        assertEquals(this.dotDir.compareTo(this.dotDir), 0);
    }
    
    @Test
    public void testSortSubstringDirs1() {
        String str = "a"; // i.e. a/
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        str = "a.";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) >= 1);
    }
    
    @Test
    public void testSortSubstringDirs2() {
        String str = "a"; // i.e. a/
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        str = "a0";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) <= -1);
    }
    
    @Test
    public void testSortSubstringFileDir1() {
        String str = "a";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        str = "a.";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) <= -1);
    }
    
    @Test
    public void testSortSubstringFileDir2() {
        String str = "a";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        str = "a.";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) >= 1);
    }
    
    @Test
    public void testSortSubstringFileDir3() {
        String str = "a";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        str = "a0";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) <= -1);
    }
    
    @Test
    public void testSortSubstringFileDir4() {
        String str = "a";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this.dirAttrs);
        str = "a0";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) >= 1);
    }
    
    @Test
    public void testSortSubstringFiles1() {
        String str = "a";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        str = "a.";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) <= -1);
    }
    
    @Test
    public void testSortSubstringFiles2() {
        String str = "a";
        FileInfoImpl f = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        str = "a0";
        FileInfoImpl g = new FileInfoImpl(str, str.getBytes(), this._fileAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) <= -1);
    }
    
    @Test
    public void testSortUtfString() {
        String str1 = "Tuabc";
        String str2 = "Tüabc";
        FileInfoImpl f = new FileInfoImpl(str1, str1.getBytes(), this._fileAttrs);
        FileInfoImpl g = new FileInfoImpl(str2, str2.getBytes(), this._fileAttrs);
        assertFalse(f.equals(g));
        assertTrue(f.compareTo(g) <= -1);
    }
}