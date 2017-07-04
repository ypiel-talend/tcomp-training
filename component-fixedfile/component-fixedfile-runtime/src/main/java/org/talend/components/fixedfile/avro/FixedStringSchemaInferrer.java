// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.fixedfile.avro;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.talend.components.fixedfile.utils.FixedFileUtils;
import org.talend.daikon.avro.AvroUtils;

/**
 * Creates (infers) {@link Schema} from data, which is read from data storage
 * This is used in case user specifies dynamic field in Design schema
 */
public class FixedStringSchemaInferrer {

    /**
     * Default schema for dynamic fields are of type String
     */
    private static final Schema STRING_SCHEMA = AvroUtils._string();

    /**
     * Field length which is used in string line
     */
    private final Integer length;

    /**
     * Constructors sets delimiter
     */
    public FixedStringSchemaInferrer(Integer length) {
        this.length = length;
    }

    /**
     * Creates Runtime schema from incoming data. <br>
     * Schema is created in following way: <br>
     * 1. Delimited string is splitted using <code>delimiter</code> to count
     * number of fields in delimited string <br>
     * 2. The same number of fields are created for Runtime schema <br>
     * 3. Field names are {@code "column<Index>"} <br>
     * 4. Field types are String
     * 
     * @param delimitedString a line, which was read from file source
     * @return Runtime avro schema
     */
    public Schema inferSchema(String delimitedString) {        
        List<Field> schemaFields = new ArrayList<>();
        
        List<String> fields = FixedFileUtils.fixedSplit(delimitedString, this.length);
        Iterator<String> i = fields.iterator();
        int n = 1;
        while (i.hasNext()) {
            String f = i.next();
            Field designField = new Field("column" + n, STRING_SCHEMA, null, (Object) null);
            schemaFields.add(designField);
            
            n++;
        }
        
        Schema schema = Schema.createRecord("Runtime", null, null, false, schemaFields);
        return schema;
    }

}
