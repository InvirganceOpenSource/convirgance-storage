/*
 * The MIT License
 *
 * Copyright 2025 jbanes.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.invirgance.convirgance.storage;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.source.FileSource;
import com.invirgance.convirgance.source.Source;
import com.invirgance.convirgance.target.Target;
import java.io.*;

/**
 *
 * @author jbanes
 */
public class AtomicFile
{
    private final File file;
    private final File temp;
    private final File old;

    public AtomicFile(File parent, String filename)
    {
        this(new File(parent, filename));
    }
    
    public AtomicFile(File file)
    {
        String root = file.getName();
        int dot = root.indexOf('.');
        
        if(dot > 0) root = file.getName().substring(0, dot);
        
        this.file = file;
        this.temp = new File(file.getParentFile(), root + ".tmp");
        this.old = new File(file.getParentFile(), root + ".old");
        
        // Restore the previous version if something went wrong
        if(!file.exists() && old.exists())
        {
            old.renameTo(file);
        }
    }

    public File getFile()
    {
        return file;
    }
    public boolean exists()
    {
        return file.exists();
    }
    
    public Source getSource()
    {
        return new FileSource(file);
    }
    
    public Target getTarget()
    {
        return new Target() {
            
            @Override
            public OutputStream getOutputStream()
            {
                try
                {
                    return getOutput();
                }
                catch(IOException e)
                {
                    throw new ConvirganceException(e);
                }
            }
        };
    }
    
    public DataInputStream getInput() throws IOException
    {
        return new DataInputStream(new FileInputStream(this.file));
    }
    
    public DataOutputStream getOutput() throws IOException
    {
        return new SwappableDataOutputStream();
    }
    
    private class SwappableDataOutputStream extends DataOutputStream
    {
        public SwappableDataOutputStream() throws IOException
        {
            super(new FileOutputStream(temp));
        }

        @Override
        public void close() throws IOException
        {
            super.close();
            
            old.delete();
            file.renameTo(old);
            temp.renameTo(file);
        }
    }
}
