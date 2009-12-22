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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import mx.lhchavez.paradis.io.Writable;
import mx.lhchavez.paradis.io.WritableComparable;
import mx.lhchavez.paradis.mapreduce.Mapper;
import mx.lhchavez.paradis.mapreduce.Reducer;
import mx.lhchavez.paradis.mapreduce.InputFormat;
import mx.lhchavez.paradis.mapreduce.OutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * @author lhchavez
 */
public class Configuration implements Iterable<Map.Entry<String,Object>>, Writable {
    private Map<String, Object> values;
    private Document doc;
    private File jobDirectory;

    private Class<? extends WritableComparable> keyInClass;
    private Class<? extends Writable> valueInClass;
    private Class<? extends WritableComparable> keyOutClass;
    private Class<? extends Writable> valueOutClass;

    private Class<? extends Mapper> mapperClass;
    private Class<? extends Reducer> reducerClass;
    private Class<? extends InputFormat> inputFormatClass;
    private Class<? extends OutputFormat> outputFormatClass;

    private URLClassLoader loader;

    public Configuration(InputStream is) throws IOException {
        this(is, null);
    }

    public Configuration(InputStream is, File jobDirectory) throws IOException {
        values = new TreeMap<String, Object>();
        this.jobDirectory = jobDirectory;

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db;

        try {
            db = dbf.newDocumentBuilder();
            doc = db.parse(is);
        } catch(Exception ex) {
            throw new IOException(ex);
        }

        doc.getDocumentElement().normalize();

        ArrayDeque<Element> q = new ArrayDeque<Element>();
        q.add(doc.getDocumentElement());

        while(q.size() > 0) {
            Element elm = q.removeFirst();
            NodeList it = elm.getChildNodes();

            if(elm != doc.getDocumentElement() && elm.getFirstChild() != null && elm.getFirstChild().getNodeType() == Element.TEXT_NODE) {
                String path = "";

                Element curr = elm;
                while(curr != doc.getDocumentElement()) {
                    path = "." + curr.getTagName() + path;
                    curr = (Element)curr.getParentNode();
                }

                String key = path.substring(1);
                if(values.containsKey(key)) {
                    Object o = values.get(key);
                    if(o instanceof String) {
                        values.put(key, new String[] { (String)o, elm.getFirstChild().getTextContent() });
                    } else if(o instanceof String[]) {
                        String[] newArray = Arrays.copyOf((String[])o, ((String[])o).length + 1);
                        newArray[newArray.length - 1] = elm.getFirstChild().getTextContent();
                        values.put(key, newArray);
                    }
                } else {
                    set(key, elm.getFirstChild().getTextContent());
                }
            }

            for(int i = 0; i < it.getLength(); i++) {
                if(it.item(i).getNodeType() == Element.ELEMENT_NODE) {
                    q.add((Element)it.item(i));
                }
            }
        }
    }

    public void setJobDirectory(File jobDirectory) {
        this.jobDirectory = jobDirectory;
    }

    public File getJobDirectory() {
        return jobDirectory;
    }

    public File getFile(String fileName) {
        return new File(this.jobDirectory + File.separator + fileName);
    }

    public Iterator<Entry<String, Object>> iterator() {
        return values.entrySet().iterator();
    }

    public String getString(String name) {
        return getString(name, null);
    }

    public String getString(String name, String defaultValue) {
        if(values.containsKey(name))
            return values.get(name).toString();
        else
            return defaultValue;
    }

    public double getDouble(String name) {
        return getDouble(name, Double.NaN);
    }

    public double getDouble(String name, double defaultValue) {
        if(values.containsKey(name)) {
            Object o = values.get(name);
            if(o instanceof Double) {
                return ((Double)o).doubleValue();
            } else if(o instanceof Float) {
                return ((Float)o).doubleValue();
            } else if(o instanceof Integer) {
                return ((Integer)o).doubleValue();
            } else {
                return Double.parseDouble(o.toString());
            }
        } else
            return defaultValue;
    }

    public int getInt(String name) {
        return getInt(name, 0);
    }

    public int getInt(String name, int defaultValue) {
        if(values.containsKey(name)) {
            Object o = values.get(name);
            if(o instanceof Double) {
                return ((Double)o).intValue();
            } else if(o instanceof Float) {
                return ((Float)o).intValue();
            } else if(o instanceof Integer) {
                return ((Integer)o).intValue();
            } else {
                return Integer.parseInt(o.toString());
            }
        } else
            return defaultValue;
    }

    public byte[] getByteArray(String name) {
        if(values.containsKey(name)) {
            Object o = values.get(name);
            if(o instanceof byte[])
                return (byte[])o;
            else if(o instanceof String)
                return ((String)o).getBytes();
            else
                return null;
        } else
            return null;
    }

    public String[] getStringArray(String name) {
        if(values.containsKey(name)) {
            Object o = values.get(name);
            if(o instanceof String[])
                return (String[])o;
            else
                return new String[] {o.toString()};
        } else
            return new String[0];
    }

    public void set(String name, Object value) {
        values.put(name, value);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for(Entry<String,Object> entry : values.entrySet()) {
            builder.append(String.format("%s: %s\n", entry.getKey(), entry.getValue().toString()));
        }

        return builder.toString();
    }

    public Document getDocument() {
        return doc;
    }

    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void validate(File jobRoot) throws InstantiationException, IllegalAccessException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, MalformedURLException, IOException {
        if(
                getString("jar.file") == null || getString("mapper.class") == null ||
                getString("mapper.key.class") == null || getString("mapper.value.class") == null ||
                getString("reducer.key.class") == null || getString("reducer.value.class") == null ||
                getString("reducer.class") == null || getString("inputFormat.class") == null ||
                getString("outputFormat.class") == null
        ) {
            throw new IllegalArgumentException("Configuration file is missing one of jar.file, mapper.class, reducer.class, partitioner.class, outputCommitter.class");
        }

        ArrayList<URL> jarUrls = new ArrayList<URL>();

        for(String jarFile : getStringArray("jar.file")) {
            jarUrls.add(new URL("file", "", jobRoot.getCanonicalPath() + File.separator + jarFile));
        }

        for(String jarFile : getStringArray("jar.libraries")) {
            jarUrls.add(new URL("file", "", new File("libraries" + File.separator + jarFile + ".jar").getCanonicalPath()));
        }

        URL[] jarUrlArray = new URL[jarUrls.size()];
        jarUrls.toArray(jarUrlArray);
        loader = new URLClassLoader(jarUrlArray);
        
        keyInClass = (Class<? extends WritableComparable>) loader.loadClass(getString("mapper.key.class"));
        valueInClass = (Class<? extends Writable>) loader.loadClass(getString("mapper.value.class"));
        keyOutClass = (Class<? extends WritableComparable>) loader.loadClass(getString("reducer.key.class"));
        valueOutClass = (Class<? extends Writable>) loader.loadClass(getString("reducer.value.class"));

        // if paradis throws up, it WILL throw up here, and not later in the process :P
        getKeyInClass().newInstance();
        getValueInClass().newInstance();
        getKeyOutClass().newInstance();
        getValueOutClass().newInstance();
        mapperClass = (Class<? extends Mapper>) loader.loadClass(getString("mapper.class"));
        reducerClass = (Class<? extends Reducer>) loader.loadClass(getString("reducer.class"));
        inputFormatClass = (Class<? extends InputFormat>) loader.loadClass(getString("inputFormat.class"));
        outputFormatClass = (Class<? extends OutputFormat>) loader.loadClass(getString("outputFormat.class"));
    }

    /**
     * @return the loader
     */
    public ClassLoader getClassLoader() {
        return loader;
    }

    /**
     * @return the keyInClass
     */
    public Class<? extends WritableComparable> getKeyInClass() {
        return keyInClass;
    }

    /**
     * @return the valueInClass
     */
    public Class<? extends Writable> getValueInClass() {
        return valueInClass;
    }

    /**
     * @return the keyOutClass
     */
    public Class<? extends WritableComparable> getKeyOutClass() {
        return keyOutClass;
    }

    /**
     * @return the valueOutClass
     */
    public Class<? extends Writable> getValueOutClass() {
        return valueOutClass;
    }

    /**
     * @return the mapperClass
     */
    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    /**
     * @return the reducerClass
     */
    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    /**
     * @return the inputFormatClass
     */
    public Class<? extends InputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    /**
     * @return the outputFormatClass
     */
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }
}
