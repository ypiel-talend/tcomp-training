package org.talend.components.fixedfile.helper;

import static org.talend.daikon.properties.property.PropertyFactory.newProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.TypeLiteral;
import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties.TRIM;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

public class FixedFileTrimTableProperties extends ComponentPropertiesImpl {
    private static final long serialVersionUID = 1L;

    public static final TypeLiteral<List<String>> LIST_STRING_TYPE = new TypeLiteral<List<String>>() {
                                                                                    };
    public static final TypeLiteral<List<FixedFileInputProperties.TRIM>> LIST_TRIM_TYPE = new TypeLiteral<List<FixedFileInputProperties.TRIM>>() {
                                                                                    };
        
    public Property<List<String>> field = newProperty(LIST_STRING_TYPE, "field");
    public Property<List<FixedFileInputProperties.TRIM>> trim = newProperty(LIST_TRIM_TYPE, "trim"); //PropertyFactory.newEnumList("trim", LIST_TRIM_TYPE); //newProperty(LIST_TRIM_TYPE, "trim");

    
    public FixedFileTrimTableProperties(String name) {
        super(name);
    }
    
    @Override
    public void setupProperties() {
        super.setupProperties();
        
        field.setValue(new ArrayList<String>());
        trim.setValue(new ArrayList<FixedFileInputProperties.TRIM>());
        
        FixedFileInputProperties.TRIM[] trimValues = FixedFileInputProperties.TRIM.class.getEnumConstants();
        trim.setPossibleValues(trimValues);
        trim.setStoredValue(TRIM.BOTH);
    }
   
    @Override
    public void setupLayout() {
        super.setupLayout();
        
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addColumn(Widget.widget(field).setWidgetType(Widget.DEFAULT_WIDGET_TYPE));
        mainForm.addColumn(Widget.widget(trim).setWidgetType(Widget.ENUMERATION_WIDGET_TYPE));
    }
    
    public Map<String, TRIM> getTrimByField() {
        Map<String, TRIM> trimByFld = new HashMap<>();
        
        if(field != null && field.getValue() != null) {
            for(int idx = 0; idx < field.getValue().size(); idx++) {
                trimByFld.put(field.getValue().get(idx), trim.getValue().get(idx));
            }
        }
        
        return trimByFld;
    }
    
}
