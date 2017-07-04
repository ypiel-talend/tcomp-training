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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.service.common.DefinitionRegistry;
import org.talend.components.fixedfile.FixedFileInputFamilyDefinition;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties.TRIM;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;

@SuppressWarnings("nls")
public class FixedFileInputTest {

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    private ComponentService componentService;

    @Before
    public void initializeComponentRegistryAndService() {
        // reset the component service
        componentService = null;
    }

    // default implementation for pure java test. 
    public ComponentService getComponentService() {
        if (componentService == null) {
            DefinitionRegistry testComponentRegistry = new DefinitionRegistry();
            testComponentRegistry.registerComponentFamilyDefinition(new FixedFileInputFamilyDefinition());
            componentService = new ComponentServiceImpl(testComponentRegistry);
        }
        return componentService;
    }

    @Test
    public void testFixedFileInputRuntime() throws Exception {
        FixedFileInputProperties props = (FixedFileInputProperties) getComponentService().getComponentProperties("FixedFileInput");

        File tempFile = File.createTempFile("FixedFileInputTestFile", ".txt");
        try {
            PrintWriter writer = new PrintWriter(tempFile.getAbsolutePath(), "UTF-8");
            writer.println("Hello_____True      500       2017-01-2212.147       First        Last  BOTH    ");
            writer.println("H2llo_____True      200       2017-02-2212.222       F2rst        L2st  B2TH    ");
            writer.close();
            props.filename.setValue(tempFile.getAbsolutePath());

            Schema.Field col0 = new Schema.Field("stringCol", AvroUtils._string(), null, (Object) null);
            Schema.Field col1 = new Schema.Field("booleanCol", AvroUtils._boolean(), null, (Object) null);
            Schema.Field col2 = new Schema.Field("intCol", AvroUtils._int(), null, (Object) null);
            Schema.Field col3 = new Schema.Field("timestampCol", AvroUtils._logicalTimestamp(), null, (Object) null);
            col3.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd");
            Schema.Field col4 = new Schema.Field("doubleCol", AvroUtils._double(), null, (Object) null);
            Schema.Field col5 = new Schema.Field("RightCol", AvroUtils._string(), null, (Object) null);
            Schema.Field col6 = new Schema.Field("LeftCol", AvroUtils._string(), null, (Object) null);
            Schema.Field col7 = new Schema.Field("BothCol", AvroUtils._string(), null, (Object) null);

            List<Field> fields = Arrays.asList(col0, col1, col2, col3, col4, col5, col6, col7);
            Schema schema  = Schema.createRecord("file", null, null, false, fields);
            props.schema.schema.setValue(schema);
            
            List<String> cols = new ArrayList<>();
            List<TRIM> colTrims = new ArrayList<>();
            
            cols.add("stringCol");
            cols.add("booleanCol");
            cols.add("intCol");
            cols.add("timestampCol");
            cols.add("doubleCol");
            cols.add("RightCol");
            cols.add("LeftCol");
            cols.add("BothCol");
            
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            colTrims.add(TRIM.BOTH);
            
            props.tableTrim.field.setValue(cols);
            props.tableTrim.trim.setValue(colTrims);
            
            

            FixedFileInputSource source = new FixedFileInputSource();
            source.initialize(null, props);

            FixedFileInputReader reader = source.createReader(null);
            assertThat(reader.start(), is(true));
            
            IndexedRecord current = reader.getCurrent();
			IndexedRecord currentDataRecord = (IndexedRecord) current.get(0);
			IndexedRecord currentOutOfBandRecord = (IndexedRecord) current.get(1);
			
			/*assertThat(currentDataRecord.get(0), is((Object) "string value 1"));
			assertThat(currentDataRecord.get(1), is((Object) true));
			assertThat(currentDataRecord.get(2), is((Object) 100));
			assertThat(currentDataRecord.get(3), is((Object) 1483228800000l));
			assertThat(currentDataRecord.get(4), is((Object) 1.23));
			assertThat(currentOutOfBandRecord.get(0), is((Object) 0));*/

			// No auto advance when calling getCurrent more than once.
			current = reader.getCurrent();
			currentDataRecord = (IndexedRecord) current.get(0);
			currentOutOfBandRecord = (IndexedRecord) current.get(1);
			
			/*assertThat(currentDataRecord.get(0), is((Object) "string value 1"));
			assertThat(currentDataRecord.get(1), is((Object) true));
			assertThat(currentDataRecord.get(2), is((Object) 100));
			assertThat(currentDataRecord.get(3), is((Object) 1483228800000l));
			assertThat(currentDataRecord.get(4), is((Object) 1.23));
			assertThat(currentOutOfBandRecord.get(0), is((Object) 0));*/

			assertThat(reader.advance(), is(true));
			current = reader.getCurrent();
			currentDataRecord = (IndexedRecord) current.get(0);
			currentOutOfBandRecord = (IndexedRecord) current.get(1);
			
			/*assertThat(currentDataRecord.get(0), is((Object) "string value 2"));
			assertThat(currentDataRecord.get(1), is((Object) false));
			assertThat(currentDataRecord.get(2), is((Object) 200));
			assertThat(currentDataRecord.get(3), is((Object) 1485043200000l));
			assertThat(currentDataRecord.get(4), is((Object) 4.56));
			assertThat(currentOutOfBandRecord.get(0), is((Object) 1));*/

			// no more records
			assertThat(reader.advance(), is(false));
        } finally {
            tempFile.delete();
        }
    }

}
