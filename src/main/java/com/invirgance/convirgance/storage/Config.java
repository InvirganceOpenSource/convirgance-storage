/*
 * Copyright 2024 INVIRGANCE LLC

Permission is hereby granted, free of charge, to any person obtaining a copy 
of this software and associated documentation files (the “Software”), to deal 
in the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE.
 */
package com.invirgance.convirgance.storage;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.bson.BinaryDecoder;
import com.invirgance.convirgance.bson.BinaryEncoder;
import com.invirgance.convirgance.input.InputCursor;
import com.invirgance.convirgance.input.JSONInput;
import com.invirgance.convirgance.json.JSONArray;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.output.JSONOutput;
import com.invirgance.convirgance.output.OutputCursor;
import com.invirgance.convirgance.source.Source;
import com.invirgance.convirgance.transform.filter.Filter;
import com.invirgance.convirgance.transform.sets.UnionIterable;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Sophisticated database of configuration records that can be easily inserted,
 * updated, and read. A default file can be used to seed the configuration data.
 * This default data can be deleted, overwritten, and updated without affecting
 * the underlying source of the default data.
 * 
 * Note that the data is stored as a file called "config.json" in the chosen
 * directory. The data is in JSON format to make it easy to read and modify if so 
 * desired.
 * 
 * @author jbanes
 */
public class Config implements Iterable<JSONObject>
{
    private Source source;
    private File directory;
    private String primaryKey;

    private List defaultIndex;
    private List deletedIndex;
    
    private AtomicFile file;
    private AtomicFile deleted;
    
    /**
     * Access a configuration database without any default data. Records are
     * determined to be unique based on the primaryKey. The primary key does
     * not have to be a specific data type, only unique across records.
     * 
     * @param directory where to store the configuration data
     * @param primaryKey the key name of the primary key (e.g. id)
     */
    public Config(File directory, String primaryKey)
    {
        this(null, directory, primaryKey);
    }
    
    /**
     * Access a configuration database, pulling default data from the specified
     * source. Records are determined to be unique based on the primaryKey. The 
     * primary key does not have to be a specific data type, only unique across 
     * records. The default data source must have the primary key in its data.
     * 
     * A common use case would be to store the default configuration data in the
     * project's classpath and use a ClasspathSource to access the data. This 
     * allows for intelligent "defaults" that can then be overridden once the 
     * app is in use.
     * 
     * @param source file containing default configuration data
     * @param directory where to store the configuration data
     * @param primaryKey the key name of the primary key (e.g. id)
     */
    public Config(Source source, File directory, String primaryKey)
    {
        this.source = source;
        this.directory = directory;
        this.primaryKey = primaryKey;
        
        if(!directory.exists()) directory.mkdirs();
        
        this.file = new AtomicFile(directory, "config.json");
        this.deleted = new AtomicFile(directory, "deleted.idx");
    }

    /**
     * Obtain the key name used to determine record uniqueness
     * 
     * @return the primary key name
     */
    public String getPrimaryKey()
    {
        return primaryKey;
    }

    /**
     * Obtain the default data source provided when this object was created
     * 
     * @return the default data source if set, otherwise null
     */
    public Source getSource()
    {
        return source;
    }

    /**
     * Obtain the directory in which the configuration data will be stored
     * 
     * @return the configuration database directory
     */
    public File getDirectory()
    {
        return directory;
    }
    
    private void init()
    {
        if(this.defaultIndex == null) generateDefaultIndex();
        if(this.deletedIndex == null) loadDeletedIndex();
    }
    
    private void loadDeletedIndex()
    {
        BinaryDecoder decoder = new BinaryDecoder();
        int count;
        
        this.deletedIndex = new ArrayList();
        
        if(!deleted.exists()) return;
        
        try(DataInputStream in = deleted.getInput())
        {
            count = (int)decoder.read(in);
            
            for(int i=0; i<count; i++)
            {
                this.deletedIndex.add(decoder.read(in));
            }
        }
        catch(IOException e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    private void generateDefaultIndex()
    {
        Object value;
        
        defaultIndex = new ArrayList();
        
        if(source == null) return;
        
        for(JSONObject record : getDefault())
        {
            value = record.get(primaryKey);
            
            if(value == null) throw new ConvirganceException("Default source contains null primary key: \n" + record.toString(4));
            
            defaultIndex.add(value);
        }
    }
    
    private InputCursor<JSONObject> getDefault()
    {
        return new JSONInput().read(this.source);
    }
    
    private Iterable<JSONObject> getFilteredDefault()
    {
        Iterable<JSONObject> iterable = getDefault();

        return new Filter() {

            @Override
            public boolean test(JSONObject record)
            {
                Object key = record.get(primaryKey);

                return !deletedIndex.contains(key);
            }
        }.transform(iterable);
    }
    
    private Iterable<JSONObject> getData()
    {
        if(!file.exists()) return new JSONArray<>();
        
        return new JSONInput().read(file.getSource());
    }
    
    @Override
    public Iterator<JSONObject> iterator()
    {   
        init();
        
        if(source != null)
        {
            return new UnionIterable(getFilteredDefault(), getData()).iterator();
        }
        
        return getData().iterator();
    }
    
    /**
     * Delete the record that has the specified primary key. If the key is not
     * found in any of the records, this call will be a no-op.
     * 
     * @param primaryKey the value identifying the record to be deleted
     */
    public void delete(Object primaryKey)
    {
        BinaryEncoder encoder = new BinaryEncoder();
        
        init();
        
        // Mark deleted from default config
        if(!deletedIndex.contains(primaryKey) && defaultIndex.contains(primaryKey))
        {
            deletedIndex.add(primaryKey);
            
            try(DataOutputStream out = deleted.getOutput())
            {
                encoder.write(deletedIndex.size(), out);
                
                for(Object key : this.deletedIndex) encoder.write(key, out);
            }
            catch(IOException e)
            {
                throw new ConvirganceException(e);
            }
        }
        
        // File is not yet initialized
        if(!file.exists()) return;
        
        // Remove from stored data
        try(OutputCursor output = new JSONOutput().write(file.getTarget()))
        {
            for(JSONObject item : getData())
            {
                if(item.get(this.primaryKey).equals(primaryKey)) continue;

                output.write(item);
            }
        }
        catch(Exception e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    /**
     * Delete the specified record from the configuration database. Note that
     * the record is identified by the primary key. The underlying record will
     * still be deleted even if parts of the record's data are different. 
     * 
     * If the record is not found, this call will be a no-op.
     * 
     * @param record the record to delete
     */
    public void delete(JSONObject record)
    {
        delete(record.get(this.primaryKey));
    }
    
    /**
     * Inserts or updates a record in the configuration database. The record is
     * updated if the primary key matches a record already in the underlying 
     * data set.
     * 
     * @param record the record to insert or update
     */
    public void insert(JSONObject record)
    {
        Object key = record.get(this.primaryKey);
        
        init();
        
        // Mark the default version as deleted
        if(defaultIndex.contains(key) && !deletedIndex.contains(key)) 
        {
            delete(key);
        }
        
        try(OutputCursor output = new JSONOutput().write(file.getTarget()))
        {
            for(JSONObject item : getData())
            {
                if(item.get(this.primaryKey).equals(key)) continue;

                output.write(item);
            }
            
            // Write the new record at the end
            output.write(record);
        }
        catch(Exception e)
        {
            throw new ConvirganceException(e);
        }
    }
}
