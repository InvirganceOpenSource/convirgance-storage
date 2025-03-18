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
 * Provides access to a local filesystem file for reading and atomic writes. When
 * the file is updated, data is first written to a temporary location. Once the
 * data is successfully written and the stream is closed, the temporary file is
 * swapped with the previous file version.
 * 
 * A backup is kept of the previous version in case the swap fails. In such a
 * case the older revision will be detected and restored when the AtomicFile 
 * object is constructed.
 * 
 * Note that the atomic nature of the file makes simultaneous reads and writes
 * possible, making AtomicFile ideal for updates where data is streamed and 
 * manipulated while being written.
 * 
 * @author jbanes
 */
public class AtomicFile
{
    private final File file;
    private final File temp;
    private final File old;

    /**
     * Access the specified filename in the given directory.
     * 
     * @param parent The parent directory of the file
     * @param filename The name of the file to operate on atomically
     */
    public AtomicFile(File parent, String filename)
    {
        this(new File(parent, filename));
    }
    
    /**
     * Access the specified file. The file must be a regular file and not a
     * directory, link, symlink, or other special file.
     * 
     * @param file The underlying file to operate on atomically
     */
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

    /**
     * Return a reference to the underlying file
     * 
     * @return Reference to the underlying file 
     */
    public File getFile()
    {
        return file;
    }
    
    /**
     * Checks if the underlying file exists
     * 
     * @return true if the underlying file exists, false otherwise
     */
    public boolean exists()
    {
        return file.exists();
    }
    
    /**
     * Obtains a Convirgance Source for accessing the AtomicFile data
     * 
     * @return a Convirgance Source for reading the AtomicFile data
     */
    public Source getSource()
    {
        return new FileSource(file);
    }
    
    /**
     * Obtains a Convirgance Target for writing to the AtomicFile. The Target
     * writes to a temporary location until the underlying stream is closed. As
     * soon as the stream is closed, the temporary data is swapped in.
     * 
     * @return a Convirgance Target for writing to the AtomicFile
     */
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
    
    /**
     * Obtains a DataInputStream for accessing the AtomicFile data
     * 
     * @return a DataInputStream for reading the AtomicFile data
     */
    public DataInputStream getInput() throws IOException
    {
        return new DataInputStream(new FileInputStream(this.file));
    }
    
    /**
     * Obtains a DataOutputStream for writing to the AtomicFile. The Target
     * writes to a temporary location until the stream is closed. As
     * soon as the stream is closed, the temporary data is swapped in.
     * 
     * @return a DataOutputStream for writing to the AtomicFile
     */
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
            System.gc();

            old.delete();
            file.renameTo(old);
            temp.renameTo(file);
        }
    }
}
