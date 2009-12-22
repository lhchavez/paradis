/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mx.lhchavez.paradis.mapreduce;

import mx.lhchavez.paradis.io.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author lhchavez
 */
public abstract class OutputFormat<K, V> implements RecordWriter<K, V> {
    private File outputDir;
    private long outputCount;

    public void setOutputDirectory(File output) {
        this.outputDir = output;
        outputCount = 0;
    }

    public final void write(K k, V v) throws IOException {
        FileOutputStream output = new FileOutputStream(String.format("%s%s%08d.out", outputDir.getAbsolutePath(), File.separator, outputCount));
        ++outputCount;

        write(k, v, output);
        
        output.close();
    }

    public abstract void write(K k, V v, OutputStream output) throws IOException;
}
