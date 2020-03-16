package com.dexels.navajo.document.json;

import com.dexels.navajo.document.types.Binary;
import com.dexels.navajo.document.types.NavajoType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class TmlNavajoTypeSerializer extends StdSerializer<NavajoType> {
    private static final Logger logger = LoggerFactory.getLogger(TmlNavajoTypeSerializer.class);

    private static final long serialVersionUID = 6193576754986884043L;

    public TmlNavajoTypeSerializer() {
        this(null);
    }

    public TmlNavajoTypeSerializer(Class<NavajoType> t) {
        super(t);
    }

    @Override
    public void serialize(NavajoType type, JsonGenerator jg, SerializerProvider provider) throws IOException {
        
        Writer w = null;
        
        Object o = jg.getOutputTarget();
        if (o instanceof Writer) {
            w = (Writer) o;
            
        } else if (o instanceof OutputStream) {
            w = new OutputStreamWriter((OutputStream)o);
        } else {
            logger.warn("Unknown outputtarget from JsonGenerator: {}", o);
            return;
        }
       
        if (type instanceof Binary) {
            // Write a quote as rawvalue to change the mode of JsonGenerator to VALUE
            jg.writeRawValue("\"");
            // Flush JsonGenerator since we will now be talking to the underlying stream itself
            jg.flush();
            // Write the binary to the stream, and end with another quote
            ((Binary) type).writeBase64(w, false);
            jg.writeRaw("\"");
        } else {
            jg.writeString(type.toString());
        }
        
    }

    
    

}
