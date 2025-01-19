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

import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.source.ClasspathSource;
import java.io.File;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jbanes
 */
public class ConfigTest
{
    private void delete(File directory)
    {
        if(!directory.exists()) return;
        
        for(File file : directory.listFiles())
        {
            if(file.isDirectory()) delete(file);
            else file.delete();
        }
        
        directory.delete();
    }
    
    @Test
    public void testEmpty()
    {
        File file = new File("target/tests/config/empty/");
        
        Config config = new Config(file, "name");
        
        assertFalse(config.iterator().hasNext());
    }
    
    @Test
    public void testDefaultConfig()
    {
        Object[] expected = new Object[] {
            "Oracle Thin Driver",
            "Derby Network Client",
            "SQL Server (jTDS)",
            "HSQLDB"
        };
        
        File file = new File("target/tests/config/default/");
        
        ClasspathSource source = new ClasspathSource("/database/drivers.json");
        Config config = new Config(source, file, "name");
        int index = 0;
        
        delete(file);
        
        for(JSONObject record : config)
        {
            assertEquals(expected[index++], record.getString(config.getPrimaryKey()));
        }
        
        assertEquals(expected.length, index); // Make sure we got the expected number of records
    }
    
    @Test
    public void testDeleteDefault()
    {
        Object[] deletes = new Object[] {
            "Derby Network Client",
            "Oracle Thin Driver",
            "HSQLDB",
            "SQL Server (jTDS)"
        };
        
        Object[][] expected = new Object[][] {
            {
                "Oracle Thin Driver",
                "Derby Network Client",
                "SQL Server (jTDS)",
                "HSQLDB"
            },
            {
                "Oracle Thin Driver",
                "SQL Server (jTDS)",
                "HSQLDB"
            },
            {
                "SQL Server (jTDS)",
                "HSQLDB"
            },
            {
                "SQL Server (jTDS)"
            }
        };
        
        File directory = new File("target/tests/config/default/");
        ClasspathSource source = new ClasspathSource("/database/drivers.json");
        Config config;
        
        int pass = 0;
        int index = 0;
        
        delete(directory);
        
        config = new Config(source, directory, "name");
        
        for(Object deleted : deletes)
        {
            index = 0;
            
            for(JSONObject record : config)
            {
                assertEquals(expected[pass][index++], record.getString(config.getPrimaryKey()));
            }

            assertEquals(expected[pass].length, index); // Make sure we got the expected number of records
            
            config.delete(deleted);
            
            pass++;
        }
        
        assertFalse(config.iterator().hasNext());
        
        // Make sure the deletedIndex is successfully reloaded
        assertFalse(new Config(source, directory, "name").iterator().hasNext());
    }
    
    @Test
    public void testInsertNoDefault()
    {
        File directory = new File("target/tests/config/insert-no-default/");
        Config config;
        
        int index = 0;
        
        delete(directory);
        
        config = new Config(directory, "name");
        
        // Ensure no records yet
        assertFalse(config.iterator().hasNext());
        
        
        // Insert first record
        config.insert(new JSONObject("{\"name\": \"Record1\"}"));
        
        for(JSONObject record : config)
        {
            assertEquals("Record1", record.get("name"));
            
            index++;
        }
        
        assertEquals(1, index);
        
        
        // Update existing record
        index = 0;
        
        config.insert(new JSONObject("{\"name\": \"Record1\"}"));
        
        for(JSONObject record : config)
        {
            assertEquals("Record1", record.get("name"));
            
            index++;
        }
        
        assertEquals(1, index);
        
        
        // Insert second record
        index = 0;
        
        config.insert(new JSONObject("{\"name\": \"Record2\"}"));
        
        for(JSONObject record : config)
        {
            assertEquals("Record" + (index+1), record.get("name"));
            
            index++;
        }
        
        assertEquals(2, index);
    }
    
    @Test
    public void testUpdate()
    {
        Object[] expected = new Object[] {
            "Oracle Thin Driver",
            "Derby Network Client",
            "SQL Server (jTDS)",
            "HSQLDB"
        };
        
        File directory = new File("target/tests/config/update/");
        ClasspathSource source = new ClasspathSource("/database/drivers.json");
        Config config;
        
        int index = 0;
        
        delete(directory);
        
        config = new Config(source, directory, "name");
        
        config.insert(new JSONObject("{\"name\": \"HSQLDB\"}"));
        
        for(JSONObject record : config)
        {
            if(record.get("name").equals("HSQLDB"))
            {
                assertEquals(1, record.size());
            }
            
            assertEquals(expected[index++], record.get("name"));
        }
        
        assertEquals(expected.length, index);
    }
    
    @Test
    public void testInsert()
    {
        Object[] expected = new Object[] {
            "Oracle Thin Driver",
            "Derby Network Client",
            "SQL Server (jTDS)",
            "HSQLDB",
            "TestSQL"
        };
        
        File directory = new File("target/tests/config/insert/");
        ClasspathSource source = new ClasspathSource("/database/drivers.json");
        Config config;
        
        int index = 0;
        
        delete(directory);
        
        config = new Config(source, directory, "name");
        
        config.insert(new JSONObject("{\"name\": \"TestSQL\"}"));
        
        for(JSONObject record : config)
        {
            if(record.get("name").equals("TestSQL"))
            {
                assertEquals(1, record.size());
            }
            
            assertEquals(expected[index++], record.get("name"));
        }
        
        assertEquals(expected.length, index);
    }
}
