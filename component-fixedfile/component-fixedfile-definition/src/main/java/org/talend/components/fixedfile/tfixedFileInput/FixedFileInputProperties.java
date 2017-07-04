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
package org.talend.components.fixedfile.tfixedFileInput;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.common.FixedConnectorsComponentProperties;
import org.talend.components.common.SchemaProperties;
import org.talend.components.common.avro.RootSchemaUtils;
import org.talend.components.fixedfile.RuntimeInfoProvider;
import org.talend.components.fixedfile.helper.FixedFileTrimTableProperties;
import org.talend.components.fixedfile.runtime.reader.SchemaDiscovery;
import org.talend.daikon.properties.PresentationItem;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.ValidationResult.Result;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.EnumProperty;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;
import org.talend.daikon.runtime.RuntimeInfo;
import org.talend.daikon.runtime.RuntimeUtil;
import org.talend.daikon.sandbox.SandboxedInstance;

/**
 * The ComponentProperties subclass provided by a component stores the 
 * configuration of a component and is used for:
 * 
 * <ol>
 * <li>Specifying the format and type of information (properties) that is 
 *     provided at design-time to configure a component for run-time,</li>
 * <li>Validating the properties of the component at design-time,</li>
 * <li>Containing all of the UI information for laying out and presenting the 
 *     properties to the user.</li>
 * </ol>
 * 
 * The FixedFileInputProperties has following properties:
 * <ol>
 * <li>{@code filename}, a simple property which is a String containing the 
 *     file path that this component will read.</li>
 * <li>{@code schema}, an embedded property referring to a Schema.</li>
 * <li>{@code delimiter}, a string property containing field delimiter, 
 *     which is used in a file that this component will read.</li>
 * </ol>
 */
public class FixedFileInputProperties extends FixedConnectorsComponentProperties {
    
    /**
     * The component can trim field in different way.
     *  
     * @author ypiel
     *
     */
    public enum TRIM {NONE, BEGIN, END, BOTH};
    
    /**
     * Out of band (a.k.a flow variables) data schema
     * 
     * It has one field: int currentLine
     */
    public static final Schema outOfBandSchema;
    
    

    /**
     * Stores path to file to be read <br>
     * Note: property <code>name</code>, which is
     * passed to factory should be exactly the same as Property field name Here,
     * field name is filename and property name is "filename"
     * 
     * Specify i18n messages for all {@link Property} defined in this class in
     * FixedFileInputProperties.properties file
     */
    public final Property<String> filename = PropertyFactory.newString("filename"); //$NON-NLS-1$
    
    
    /**
     * Active or not the default file field Length.
     * 
     */
    public final Property<Boolean> hasDefaultLength = PropertyFactory.newBoolean("hasDefaultLength", Boolean.FALSE);
    /**
     * The default length of a file's field.
     * If 0 not take into account.
     */
    public final Property<Integer> defaultLength = PropertyFactory.newInteger("defaultLength", Integer.valueOf(0));
    
    public final Property<TRIM> trim = PropertyFactory.newEnum("trim", TRIM.class);
    
    public final FixedFileTrimTableProperties tableTrim = new FixedFileTrimTableProperties("tableTrim");
    
    /**
     * Design schema of input component. Design schema defines data fields which
     * should be retrieved from Data Store. In this component example Data Store
     * is a single file on file system
     */
    public final SchemaProperties schema = new SchemaProperties("schema"); //$NON-NLS-1$
    

    /**
     * This {@link PresentationItem} field is used to display button on UI form. {@link Widget#BUTTON_WIDGET_TYPE} should be set
     * for this widget to specify that it is a button. This button will be used to guess schema of delimited file
     */
    public final transient PresentationItem guessSchema = new PresentationItem("guessSchema", "Guess schema");
    
    /**
     * This field specifies path {@link SchemaProperties} associated with some
     * connector. This is used to retrieve schema value from
     * {@link FixedConnectorsComponentProperties} class
     */
    protected final transient PropertyPathConnector mainConnector = new PropertyPathConnector(Connector.MAIN_NAME, "schema"); //$NON-NLS-1$
    
    /**
     * Sets Out of band schema. This schema is not supposed to be changed by user
     */
    static {       
        Field currentLineField = new Field("CURRENT_LINE", Schema.create(Schema.Type.INT), null, (Object) null);
        outOfBandSchema = Schema.createRecord("OutOfBand", null, null, false);
        outOfBandSchema.setFields(Collections.singletonList(currentLineField));
    }
 
    public FixedFileInputProperties(String name) {
        super(name);
    }

    /**
     * Default properties values are set in this method
     * 
     * Note: first line in this method should be
     * <code>super.setupProperties();</code>
     */
    @Override
    public void setupProperties() {
        super.setupProperties();
    }

    /**
     * Sets UI elements layout on the form {@link Form#addRow()} sets new
     * element under previous one {@link Form#addColumn()} sets new element to
     * the right of previous one in the same row
     * 
     * Note: first line in this method should be
     * <code>super.setupLayout();</code>
     */
    @Override
    public void setupLayout() {
        super.setupLayout();
        Form form = Form.create(this, Form.MAIN);
        form.addRow(schema.getForm(Form.REFERENCE));
        form.addRow(Widget.widget(filename).setWidgetType(Widget.FILE_WIDGET_TYPE));
        form.addRow(trim);
        form.addRow(Widget.widget(tableTrim).setWidgetType(Widget.TABLE_WIDGET_TYPE));
        form.addRow(hasDefaultLength)
                .addColumn(defaultLength);
        form.addRow(Widget.widget(guessSchema).setWidgetType(Widget.BUTTON_WIDGET_TYPE));
    }
    
    /**
     * Refreshes <code>form</code> layout after some changes. Often it is used
     * to show or hide some UI elements
     * 
     * Note: first line in this method should be
     * <code>super.refreshLayout(form);</code>
     */
    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);

        if (form.getName().equals(Form.MAIN)) {
            form.getWidget(defaultLength.getName())
                                    .setHidden(!hasDefaultLength.getValue());
        }
    }
    
    /**
     * Callback method. Runtime Platform calls it after changes with UI element
     * This method should have name if following format {@code after
     * <PropertyName>}
     */   
    public void afterHasDefaultLength() {
        refreshLayout(getForm(Form.MAIN));
    }

    /**
     * This is a callback, which is called, when user presses "Guess Schema" button.
     * It tries to read data sample and guess schema using specified properties.
     * If properties are not correctly set or error occurs during reading then
     * error {@link ValidationResult} is returned.
     * If everything is ok gueessed schema is set as value of SchemaProperties.
     * 
     * @return {@link ValidationResult}
     */
    public ValidationResult validateGuessSchema() {
        RuntimeInfo runtimeInfo = RuntimeInfoProvider.provideInputRuntimeInfo();
        try (SandboxedInstance sandboxedInstance = RuntimeUtil.createRuntimeClass(runtimeInfo, getClass().getClassLoader())) {
            SchemaDiscovery discovery = (SchemaDiscovery) sandboxedInstance.getInstance();
            ValidationResult result = discovery.initialize(null, this);
            if (result.getStatus() == Result.ERROR) {
                return result;
            }
            result = discovery.validate(null);
            if (result.getStatus() == Result.ERROR) {
                return result;
            }
            try {
                Schema runtimeSchema = discovery.guessSchema();
                schema.schema.setValue(runtimeSchema);
                result = ValidationResult.OK;
            } catch (IOException e) {
                result = new ValidationResult().setStatus(Result.ERROR);
                String errorMessage = getI18nMessage("error.fileReading", e.getMessage());
                result.setMessage(errorMessage);
            }
            return result;
        }
    }

    /**
     * Refreshes form after "Guess Schema" button was processed
     */
    public void afterGuessSchema() {
        refreshLayout(getForm(Form.MAIN));
    }

    /**
     * Returns input or output component connectors
     * 
     * @param isOutputConnectors
     *            specifies what connectors to return, true if output connectors
     *            are requires, false if input connectors are requires
     * @return component connectors
     */
    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnectors) {
        if (isOutputConnectors) {
            return Collections.singleton(mainConnector);
        }
        return Collections.emptySet();
    }
}
