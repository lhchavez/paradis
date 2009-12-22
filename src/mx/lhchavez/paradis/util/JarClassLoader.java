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

package mx.lhchavez.paradis.util;

import java.net.*;
import java.lang.reflect.*;

/**
 * @author lhchavez
 */
public class JarClassLoader extends URLClassLoader {
    public JarClassLoader(URL[] urls) throws MalformedURLException {
        super(new URL[] { new URL("jar", "", urls[0] + "!/") });
        
        for(int i = 1; i < urls.length; i++) {
            addURL(new URL("jar", "", urls[i] + "!/"));
        }
    }
    
    public Object getInstance(String classname) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class c = loadClass(classname);
        Constructor cons = c.getConstructor();

        return cons.newInstance();
    }
}
