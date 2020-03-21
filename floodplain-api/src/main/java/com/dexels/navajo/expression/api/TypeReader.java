package com.dexels.navajo.expression.api;

import com.dexels.navajo.expression.api.nanoimpl.CaseSensitiveXMLElement;
import com.dexels.navajo.expression.api.nanoimpl.XMLElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class TypeReader {

    private static TypeReader instance = null;
    private Map<String, Class<?>> toJavaType = new HashMap<>();
    private Map<String, String> toJavaGenericType = new HashMap<>();
    private Map<Class<?>, String> toNavajoType = new HashMap<>();

    private static final Logger logger = LoggerFactory
            .getLogger(TypeReader.class);


    public static TypeReader getInstance() {
        if(instance==null) {
            instance = new TypeReader();
            try {
                instance.readTypes();
            } catch (ClassNotFoundException|IOException e) {
                logger.error("Error reading types");
            }
        }
        return instance;
    }

    private void readTypes() throws ClassNotFoundException, IOException {
        ClassLoader cl = getClass().getClassLoader();
        if (cl == null) {
            logger.info("Bootstrap classloader detected!");
            cl = ClassLoader.getSystemClassLoader();
        }
        InputStream is = cl.getResourceAsStream("navajotypes.xml");
        CaseSensitiveXMLElement types = new CaseSensitiveXMLElement();
        types.parseFromStream(is);
        is.close();
        Vector<XMLElement> children = types.getChildren();
        for (int i = 0; i < children.size(); i++) {
            XMLElement child = children.get(i);
            String navajotype = (String) child.getAttribute("name");
            String javaclass = (String) child.getAttribute("type");
            String generic = (String) child.getAttribute("generic");
            Class<?> c = Class.forName(javaclass);
            toJavaType.put(navajotype, c);
            toJavaGenericType.put(navajotype, generic);
            toNavajoType.put(c, navajotype);
        }
    }

    public void addNavajoType(String typeId, Class<?> clz) {
        toJavaType.put(typeId, clz);
        toNavajoType.put(clz, typeId);
    }

    public String getNavajoType(Class<?> c) {
        if (c == null) {
            return "empty";
        } else if (toNavajoType.containsKey(c)) {
            return toNavajoType.get(c);
        } else {
            return c.getName();
        }

    }

    public Class<?> getJavaType(String p) {
        Class<?> javaClass = toJavaType.get(p);
        if(javaClass==null) {
            throw new RuntimeException("Can't resolve java class for floodplain type: "+p);
        }
        return javaClass;
    }

    public String getJavaTypeGeneric(String p) {
        return toJavaGenericType.get(p);
    }

    public Set<String> getNavajoTypes() {
        return toJavaType.keySet();
    }

}
