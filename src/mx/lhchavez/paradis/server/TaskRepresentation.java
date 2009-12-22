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

package mx.lhchavez.paradis.server;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import mx.lhchavez.paradis.mapreduce.TaskAttemptID;
import org.restlet.data.MediaType;
import org.restlet.representation.OutputRepresentation;

/**
 *
 * @author lhchavez
 */
public class TaskRepresentation extends OutputRepresentation {
    private TaskAttemptID taid;
    byte[] taidBytes;
    File inputSplitFile;
    
    public TaskRepresentation(TaskAttemptID taid) throws IOException {
        super(new MediaType("application/paradis-task"));

        this.taid = taid;
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        taid.write(out);
        out.close();

        taidBytes = baos.toByteArray();

        inputSplitFile = new File("jobs" + File.separator + taid.getJobID() + File.separator + "in" + File.separator + taid.getTaskID());

        if(!inputSplitFile.exists())
            throw new FileNotFoundException(inputSplitFile.getCanonicalPath());
    }

    @Override
    public long getSize() {
        return 4L + taidBytes.length + inputSplitFile.length();
    }
    
    @Override
    public void write(OutputStream arg0) throws IOException {
        DataOutputStream output = new DataOutputStream(arg0);

        output.writeInt(taidBytes.length);
        output.write(taidBytes);

        byte[] buffer = new byte[4096];
        int read = 0;

        FileInputStream fis = new FileInputStream(inputSplitFile);

        while((read = fis.read(buffer)) > 0) {
            output.write(buffer, 0, read);
        }

        output.close();
    }
}
