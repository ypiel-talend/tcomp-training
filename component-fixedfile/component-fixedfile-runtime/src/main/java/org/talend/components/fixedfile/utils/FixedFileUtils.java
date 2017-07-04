package org.talend.components.fixedfile.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.commons.logging.Log;
import org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties.TRIM;

public class FixedFileUtils {

    public static List<String> fixedSplit(String line, Integer length) throws IllegalArgumentException {
        if(length < 1) {
            throw new IllegalArgumentException("Can't retreive field with default length as '"+length+"'");
        }
        
        List<String> fields = new ArrayList<>();

        int s = 0;
        int e = 0;
        int i = 1;
        while (s < line.length()) {
            e = s + length;
            if (e >= line.length()) {
                e = line.length();
            }
            String f = line.substring(s, e);
            fields.add(f);
            s = s + length;
            i++;
        }
        
        return fields;
    }
    
    public static String trim(TRIM t, String s) {
        switch(t) {
            case BOTH:
                s = s.trim();
                break;
            case BEGIN:
                s = s.replaceAll("^\\s+", "");
                break;
            case END:
                s = s.replaceAll("\\s+$", "");
                break;
        }
        
        return s;
    }
    
}
