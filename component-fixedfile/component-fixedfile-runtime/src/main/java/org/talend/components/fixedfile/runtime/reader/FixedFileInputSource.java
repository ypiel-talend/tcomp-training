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
package org.talend.components.fixedfile.runtime.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.talend.components.api.component.runtime.BoundedSource;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.fixedfile.avro.DelimitedStringConverter;
import org.talend.components.fixedfile.avro.FixedStringSchemaInferrer;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties.TRIM;
import org.talend.daikon.NamedThing;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.converter.AvroConverter;
import org.talend.daikon.i18n.TranslatableImpl;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.ValidationResult.Result;
import org.talend.daikon.properties.presentation.Form;

/**
 * The FixedFileInputSource provides the mechanism to supply data to other
 * components at run-time.
 *
 * Based on the Apache Beam project, the Source mechanism is appropriate to
 * describe distributed and non-distributed data sources and can be adapted
 * to scalable big data execution engines on a cluster, or run locally.
 *
 * This example component describes an input source that is guaranteed to be
 * run in a single JVM (whether on a cluster or locally)
 */
public class FixedFileInputSource extends TranslatableImpl implements BoundedSource, SchemaDiscovery {

    /** Default serial version UID. */
    private static final long serialVersionUID = 1L;
    
    private FixedFileInputProperties inputProperties;

    /**
     * Sets {@link ComponentProperties} and checks whether file name is not empty.
     * Returns error {@link ValidationResult} if it is empty.
     * {@link ValidationResult} is returned to user, so its message should be i18n
     * 
     * @param container {@link RuntimeContainer}
     * @param properties {@link ComponentProperties}
     * @return {@link ValidationResult#OK} if file name is not empty, error otherwise
     */
    @Override
    public ValidationResult initialize(RuntimeContainer container, ComponentProperties properties) {
        ValidationResult result = ValidationResult.OK;
        
        this.inputProperties = (FixedFileInputProperties) properties;
        if (getFilePath().isEmpty()) {
            result = new ValidationResult().setStatus(Result.ERROR);
            result.setMessage(getI18nMessage("error.fileNameEmpty"));
        }
        
        if(result.getStatus() == Result.OK && !validateDefaultLength()){
            result = new ValidationResult().setStatus(Result.ERROR);
            result.setMessage(getI18nMessage("error.badDefaultLength"));
        }
        
        return result;
    }

    /**
     * Validates that component can connect to Data Store
     * Here, method checks that file exist
     * If it doesn't exist returns error {@link ValidationResult}
     * If it's ok that returns {@link ValidationResult#OK}
     * {@link ValidationResult} is returned to user, so its message should be i18n
     * 
     * @param container {@link RuntimeContainer}
     * @return {@link ValidationResult#OK} if file exists, error otherwise
     */
    @Override
    public ValidationResult validate(RuntimeContainer container) {
        File file = new File(getFilePath());
        ValidationResult result;
        if (file.exists()) {
            result = ValidationResult.OK;
        } else {
            result = new ValidationResult().setStatus(Result.ERROR);
            result.setMessage(getI18nMessage("error.fileDoesntExist", getFilePath()));
        }
        return result;
    }

    /**
     * Reads first line from file source and guesses its schema.
     * Before calling this method {@link this#initialize()} and {@link this#validate()}
     * should be called
     * 
     * @return avro runtime schema of file
     * @throws IOException in case of problems during file reading or {@link FileReader} closing
     */
    @Override
    public Schema guessSchema() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(getFilePath()))) {
            String line = reader.readLine();
            return createRuntimeSchema(line);
        }
    }

    @Override
    public Schema getEndpointSchema(RuntimeContainer container, String schemaName) throws IOException {
        return null;
    }

    @Override
    public List<NamedThing> getSchemaNames(RuntimeContainer container) throws IOException {
        return null;
    }

    @Override
    public List<? extends BoundedSource> splitIntoBundles(long desiredBundleSizeBytes, RuntimeContainer adaptor) throws Exception {
       // There can be only one.
       return Arrays.asList(this);
    }

    @Override
    public long getEstimatedSizeBytes(RuntimeContainer adaptor) {
       // This will be ignored since the source will never be split.
       return 0;
    }

    @Override
    public boolean producesSortedKeys(RuntimeContainer adaptor) {
       return false;
    }
    
    @Override
    public FixedFileInputReader createReader(RuntimeContainer container) {
        return new FixedFileInputReader(this);
    }
    
    /**
     * Creates converter, which converts delimited string to
     * {@link IndexedRecord} and vice versa. <code>delimitedString</code> is
     * used to infer Runtime schema in case Design schema contains dynamic field
     * 
     * @param runtimeSchema
     *            Schema of data record
     * @return {@link AvroConverter} from delimited string to
     *         {@link IndexedRecord}
     */
    AvroConverter<String, IndexedRecord> createConverter(Schema runtimeSchema) {
        AvroConverter<String, IndexedRecord> converter = new DelimitedStringConverter(runtimeSchema, getDefaultLength(), inputProperties.tableTrim.getTrimByField());
        return converter;
    }

    Schema getDesignSchema() {
        return inputProperties.schema.schema.getValue();
    }
    
    String getFilePath() {
        return inputProperties.filename.getValue();
    }
    
    Integer getDefaultLength() {
        if (inputProperties.hasDefaultLength.getValue()) {
            return inputProperties.defaultLength.getValue();
        }
        
        return 10;
    }
    
    boolean validateDefaultLength() {
        boolean ret = true;
        
        if (inputProperties.hasDefaultLength.getValue() && inputProperties.defaultLength.getValue() < 1) {
            ret = false;
        }
        
        return ret;
    }
    
    /**
     * Provides Runtime schema for {@link FileInputReader}
     * If Design schema contains dynamic field, than Runtime schema is created from incoming data
     * If Design schema doesn't contain dynamic field, then design schema is returned
     * 
     * @param delimitedString data line
     * @return avro Runtime schema
     */
    Schema provideRuntimeSchema(String delimitedString) {
        Schema designSchema = getDesignSchema();
        if (AvroUtils.isIncludeAllFields(designSchema)) {
            return createRuntimeSchema(delimitedString);
        } else {
            return designSchema;
        }
    }

    /**
     * Creates Runtime schema from incoming data
     * 
     * @param delimitedString data line
     * @return avro Runtime schema
     */
    private Schema createRuntimeSchema(String delimitedString) {
        return new FixedStringSchemaInferrer(getDefaultLength()).inferSchema(delimitedString);
    }
}
