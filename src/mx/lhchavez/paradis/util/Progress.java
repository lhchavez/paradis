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

import java.util.ArrayList;

/**
 *
 * @author lhchavez
 */
public class Progress {
    private ArrayList<Progress> childNodes;
    private String status;
    private float progress;
    private Progress current;

    public Progress() {
        this(null);
    }

    private Progress(String status) {
        childNodes = new ArrayList<Progress>();
        this.status = status;
        this.progress = 0;
        this.current = this;
    }

    public Progress addPhase(String status) {
        Progress child = new Progress(status);

        childNodes.add(child);

        if(current == this)
            current = child;
        
        return child;
    }

    public Progress addPhase() {
        return addPhase(null);
    }

    public void startNextPhase() {

    }

    public Progress phase() {
        if(current == this)
            return this;
        else
            return current.phase();
    }

    public void complete() {
        this.progress = 1.0f;
    }

    public void set(float progress) {
        if(childNodes.isEmpty()) {
            this.progress = progress;
        }
    }

    public float get() {
        if(childNodes.isEmpty())
            return progress;

        float total = 0;
        for(Progress child : childNodes) {
            total += child.get();
        }
        return total / childNodes.size();
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return String.valueOf(status);
    }
}
