/*
 * Copyright (c) 2009, Luis Hector Chavez <lhchavez@lhchavez.com>
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package mx.lhchavez.paradis.client;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Permission;

/**
 *
 * @author lhchavez
 */
public class ParadisSecurityManager extends SecurityManager {
    private InetAddress[] ips;
    private String root;

    public ParadisSecurityManager(String host, int port, String root) {
        this.root = root;
        try {
            ips = InetAddress.getAllByName(host);
        } catch (UnknownHostException ex) {
            ips = new InetAddress[0];
        }
    }

    @Override
    public void checkAccept(String host, int port) {
        throw new SecurityException();
    }

    @Override
    public void checkAccess(Thread t) {
        super.checkAccess(t);
    }

    @Override
    public void checkAccess(ThreadGroup g) {
        super.checkAccess(g);
    }

    @Override
    public void checkAwtEventQueueAccess() {
        throw new SecurityException();
    }

    @Override
    public void checkConnect(String host, int port) {
        if(port == 53 || port == -1) return;
        
        InetAddress hostIp;
        
        try {
            hostIp = InetAddress.getByName(host);
        } catch (UnknownHostException ex) {
            throw new SecurityException(host + ":" + port);
        }

        for(InetAddress ip : ips) {
            if(ip.equals(hostIp))
                return;
        }

        throw new SecurityException(host + ":" + port);
    }

    @Override
    public void checkConnect(String host, int port, Object context) {
        checkConnect(host, port);
    }

    @Override
    public void checkCreateClassLoader() {
        super.checkCreateClassLoader();
    }

    @Override
    public void checkDelete(String file) {
        try {
            file = new File(file).getCanonicalPath();
        } catch(IOException e) {
            throw new SecurityException(file);
        }

        if(file.startsWith(this.root))
            super.checkRead(file);
        else
            throw new SecurityException();
    }

    @Override
    public void checkExec(String cmd) {
        throw new SecurityException();
    }

    @Override
    public void checkExit(int status) {
        throw new SecurityException();
    }

    @Override
    public void checkLink(String lib) {
        super.checkLink(lib);
    }

    @Override
    public void checkListen(int port) {
        throw new SecurityException();
    }

    @Override
    public void checkMemberAccess(Class<?> clazz, int which) {
        super.checkMemberAccess(clazz, which);
    }

    @Override
    public void checkMulticast(InetAddress maddr) {
        throw new SecurityException();
    }

    @Override
    public void checkMulticast(InetAddress maddr, byte ttl) {
        throw new SecurityException();
    }

    @Override
    public void checkPackageAccess(String pkg) {
        super.checkPackageAccess(pkg);
    }

    @Override
    public void checkPackageDefinition(String pkg) {
        super.checkPackageDefinition(pkg);
    }

    @Override
    public void checkPermission(Permission perm) {
        if(perm.getName().equals("setSecurityManager"))
            throw new SecurityException(perm.getName());
        //super.checkPermission(perm);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        checkPermission(perm);
    }

    @Override
    public void checkPrintJobAccess() {
        throw new SecurityException();
    }

    @Override
    public void checkPropertiesAccess() {
        super.checkPropertiesAccess();
    }

    @Override
    public void checkPropertyAccess(String key) {
        super.checkPropertyAccess(key);
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        super.checkRead(fd);
    }

    @Override
    public void checkRead(String file) {
        try {
            file = new File(file).getCanonicalPath();
        } catch(IOException e) {
            throw new SecurityException(file);
        }

        if(file.startsWith(this.root))
            super.checkRead(file);
        else if(file.startsWith("/usr/lib") || file.startsWith("C:\\Archivos de programa\\Java\\jre6\\bin\\") || file.startsWith("/System/Library/Frameworks/JavaVM.framework/"))
            super.checkRead(file);
        else if(file.equals("/dev/random") || file.equals("/dev/urandom"))
            super.checkRead(file);
        else
            throw new SecurityException(file);
    }

    @Override
    public void checkRead(String file, Object context) {
        try {
            file = new File(file).getCanonicalPath();
        } catch(IOException e) {
            throw new SecurityException(file);
        }

        if(file.startsWith(this.root))
            super.checkRead(file, context);
        else if(file.startsWith("/usr/lib") || file.startsWith("C:\\Archivos de programa\\Java\\jre6\\bin\\") || file.startsWith("/System/Library/Frameworks/JavaVM.framework/"))
            super.checkRead(file, context);
        else if(file.equals("/dev/random") || file.equals("/dev/urandom"))
            super.checkRead(file, context);
        else
            throw new SecurityException(file);
    }

    @Override
    public void checkSecurityAccess(String target) {
        super.checkSecurityAccess(target);
    }

    @Override
    public void checkSetFactory() {
        super.checkSetFactory();
    }

    @Override
    public void checkSystemClipboardAccess() {
        throw new SecurityException();
    }

    @Override
    public boolean checkTopLevelWindow(Object window) {
        throw new SecurityException();
    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        super.checkWrite(fd);
    }

    @Override
    public void checkWrite(String file) {
        try {
            file = new File(file).getCanonicalPath();
        } catch(IOException e) {
            throw new SecurityException(file);
        }

        if(file.startsWith(this.root))
            super.checkWrite(file);
        else if(file.startsWith("/usr/lib"))
            super.checkWrite(file);
        else
            throw new SecurityException(file);
    }
}
