/*
 * Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package moe.lymia.zipfs;

import static moe.lymia.zipfs.ZipConstants.CENATT;
import static moe.lymia.zipfs.ZipConstants.CENATX;
import static moe.lymia.zipfs.ZipConstants.CENCOM;
import static moe.lymia.zipfs.ZipConstants.CENCRC;
import static moe.lymia.zipfs.ZipConstants.CENDSK;
import static moe.lymia.zipfs.ZipConstants.CENEXT;
import static moe.lymia.zipfs.ZipConstants.CENFLG;
import static moe.lymia.zipfs.ZipConstants.CENHDR;
import static moe.lymia.zipfs.ZipConstants.CENHOW;
import static moe.lymia.zipfs.ZipConstants.CENLEN;
import static moe.lymia.zipfs.ZipConstants.CENNAM;
import static moe.lymia.zipfs.ZipConstants.CENOFF;
import static moe.lymia.zipfs.ZipConstants.CENSIG;
import static moe.lymia.zipfs.ZipConstants.CENSIZ;
import static moe.lymia.zipfs.ZipConstants.CENTIM;
import static moe.lymia.zipfs.ZipConstants.CENVEM;
import static moe.lymia.zipfs.ZipConstants.CENVER;
import static moe.lymia.zipfs.ZipConstants.EXTID_EXTT;
import static moe.lymia.zipfs.ZipConstants.EXTID_NTFS;
import static moe.lymia.zipfs.ZipConstants.EXTID_ZIP64;
import static moe.lymia.zipfs.ZipConstants.LG;
import static moe.lymia.zipfs.ZipConstants.LL;
import static moe.lymia.zipfs.ZipConstants.LOCCRC;
import static moe.lymia.zipfs.ZipConstants.LOCEXT;
import static moe.lymia.zipfs.ZipConstants.LOCFLG;
import static moe.lymia.zipfs.ZipConstants.LOCHDR;
import static moe.lymia.zipfs.ZipConstants.LOCHOW;
import static moe.lymia.zipfs.ZipConstants.LOCLEN;
import static moe.lymia.zipfs.ZipConstants.LOCNAM;
import static moe.lymia.zipfs.ZipConstants.LOCSIG;
import static moe.lymia.zipfs.ZipConstants.LOCSIZ;
import static moe.lymia.zipfs.ZipConstants.LOCTIM;
import static moe.lymia.zipfs.ZipConstants.LOCVER;
import static moe.lymia.zipfs.ZipConstants.SH;
import static moe.lymia.zipfs.ZipConstants.ZIP64_MINVAL;
import static moe.lymia.zipfs.ZipUtils.dosToJavaTime;
import static moe.lymia.zipfs.ZipUtils.unixToJavaTime;
import static moe.lymia.zipfs.ZipUtils.winToJavaTime;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

/**
 * Print all loc and cen headers of the ZIP file
 *
 * @author Xueming Shen
 */

public class ZipInfo {
    
    static long locoff(byte[] cen, int pos) {
        long locoff = CENOFF(cen, pos);
        if (locoff == ZIP64_MINVAL) { // ZIP64
            int off = pos + CENHDR + CENNAM(cen, pos);
            int end = off + CENEXT(cen, pos);
            while (off + 4 < end) {
                int tag = SH(cen, off);
                int sz = SH(cen, off + 2);
                if (tag != EXTID_ZIP64) {
                    off += 4 + sz;
                    continue;
                }
                off += 4;
                if (CENLEN(cen, pos) == ZIP64_MINVAL) {
                    off += 8;
                }
                if (CENSIZ(cen, pos) == ZIP64_MINVAL) {
                    off += 8;
                }
                return LL(cen, off);
            }
            // should never be here
        }
        return locoff;
    }
    
    public static void main(String[] args) throws Throwable {
        if (args.length < 1) {
            print("Usage: java ZipInfo zfname");
        } else {
            Map<String, ?> env = Collections.emptyMap();
            ZipFileSystem zfs = new ZipFileSystemProvider().newFileSystem(Paths.get(args[0]), env);
            byte[] cen = zfs.cen;
            if (cen == null) {
                print("zip file is empty%n");
                return;
            }
            int pos = 0;
            byte[] buf = new byte[1024];
            int no = 1;
            while (pos + CENHDR < cen.length) {
                print("----------------#%d--------------------%n", no++);
                printCEN(cen, pos);
                
                // use size CENHDR as the extra bytes to read, just in case the
                // loc.extra is bigger than the cen.extra, try to avoid to read
                // twice
                long len = LOCHDR + CENNAM(cen, pos) + CENEXT(cen, pos) + CENHDR;
                if (zfs.readFullyAt(buf, 0, len, locoff(cen, pos)) != len) {
                    ZipFileSystem.zerror("read loc header failed");
                }
                if (LOCEXT(buf) > CENEXT(cen, pos) + CENHDR) {
                    // have to read the second time;
                    len = LOCHDR + LOCNAM(buf) + LOCEXT(buf);
                    if (zfs.readFullyAt(buf, 0, len, locoff(cen, pos)) != len) {
                        ZipFileSystem.zerror("read loc header failed");
                    }
                }
                printLOC(buf);
                pos += CENHDR + CENNAM(cen, pos) + CENEXT(cen, pos) + CENCOM(cen, pos);
            }
            zfs.close();
        }
    }
    
    static void print(String fmt, Object... objs) {
        System.out.printf(fmt, objs);
    }
    
    static void printCEN(byte[] cen, int off) {
        print("[Central Directory Header]%n");
        print("    Signature   :   %#010x%n", CENSIG(cen, off));
        if (CENSIG(cen, off) != CENSIG) {
            print("    Wrong signature!");
            return;
        }
        print("    VerMadeby   :       %#6x    [%d, %d.%d]%n", CENVEM(cen, off), CENVEM(cen, off) >> 8, (CENVEM(cen, off) & 0xff) / 10, (CENVEM(cen, off) & 0xff) % 10);
        print("    VerExtract  :       %#6x    [%d.%d]%n", CENVER(cen, off), CENVER(cen, off) / 10, CENVER(cen, off) % 10);
        print("    Flag        :       %#6x%n", CENFLG(cen, off));
        print("    Method      :       %#6x%n", CENHOW(cen, off));
        print("    LastMTime   :   %#10x    [%tc]%n", CENTIM(cen, off), dosToJavaTime(CENTIM(cen, off)));
        print("    CRC         :   %#10x%n", CENCRC(cen, off));
        print("    CSize       :   %#10x%n", CENSIZ(cen, off));
        print("    Size        :   %#10x%n", CENLEN(cen, off));
        print("    NameLen     :       %#6x    [%s]%n", CENNAM(cen, off), new String(cen, off + CENHDR, CENNAM(cen, off)));
        print("    ExtraLen    :       %#6x%n", CENEXT(cen, off));
        if (CENEXT(cen, off) != 0) {
            printExtra(cen, off + CENHDR + CENNAM(cen, off), CENEXT(cen, off));
        }
        print("    CommentLen  :       %#6x%n", CENCOM(cen, off));
        print("    DiskStart   :       %#6x%n", CENDSK(cen, off));
        print("    Attrs       :       %#6x%n", CENATT(cen, off));
        print("    AttrsEx     :   %#10x%n", CENATX(cen, off));
        print("    LocOff      :   %#10x%n", CENOFF(cen, off));
        
    }
    
    static void printExtra(byte[] extra, int off, int len) {
        int end = off + len;
        while (off + 4 <= end) {
            int tag = SH(extra, off);
            int sz = SH(extra, off + 2);
            print("        [tag=0x%04x, sz=%d, data= ", tag, sz);
            if (off + sz > end) {
                print("    Error: Invalid extra data, beyond extra length");
                break;
            }
            off += 4;
            for (int i = 0; i < sz; i++) {
                print("%02x ", extra[off + i]);
            }
            print("]%n");
            switch (tag) {
                case EXTID_ZIP64:
                    print("         ->ZIP64: ");
                    int pos = off;
                    while (pos + 8 <= off + sz) {
                        print(" *0x%x ", LL(extra, pos));
                        pos += 8;
                    }
                    print("%n");
                    break;
                case EXTID_NTFS:
                    print("         ->PKWare NTFS%n");
                    // 4 bytes reserved
                    if (SH(extra, off + 4) != 0x0001 || SH(extra, off + 6) != 24) {
                        print("    Error: Invalid NTFS sub-tag or subsz");
                    }
                    print("            mtime:%tc%n", winToJavaTime(LL(extra, off + 8)));
                    print("            atime:%tc%n", winToJavaTime(LL(extra, off + 16)));
                    print("            ctime:%tc%n", winToJavaTime(LL(extra, off + 24)));
                    break;
                case EXTID_EXTT:
                    print("         ->Info-ZIP Extended Timestamp: flag=%x%n", extra[off]);
                    pos = off + 1;
                    while (pos + 4 <= off + sz) {
                        print("            *%tc%n", unixToJavaTime(LG(extra, pos)));
                        pos += 4;
                    }
                    break;
                default:
                    print("         ->[tag=%x, size=%d]%n", tag, sz);
            }
            off += sz;
        }
    }
    
    static void printLOC(byte[] loc) {
        print("%n");
        print("[Local File Header]%n");
        print("    Signature   :   %#010x%n", LOCSIG(loc));
        if (LOCSIG(loc) != LOCSIG) {
            print("    Wrong signature!");
            return;
        }
        print("    Version     :       %#6x    [%d.%d]%n", LOCVER(loc), LOCVER(loc) / 10, LOCVER(loc) % 10);
        print("    Flag        :       %#6x%n", LOCFLG(loc));
        print("    Method      :       %#6x%n", LOCHOW(loc));
        print("    LastMTime   :   %#10x    [%tc]%n", LOCTIM(loc), dosToJavaTime(LOCTIM(loc)));
        print("    CRC         :   %#10x%n", LOCCRC(loc));
        print("    CSize       :   %#10x%n", LOCSIZ(loc));
        print("    Size        :   %#10x%n", LOCLEN(loc));
        print("    NameLength  :       %#6x    [%s]%n", LOCNAM(loc), new String(loc, LOCHDR, LOCNAM(loc)));
        print("    ExtraLength :       %#6x%n", LOCEXT(loc));
        if (LOCEXT(loc) != 0) {
            printExtra(loc, LOCHDR + LOCNAM(loc), LOCEXT(loc));
        }
    }
}
