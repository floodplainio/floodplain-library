package com.dexels.navajo.mapping;

/**
 * <p>Title: Navajo Product Project</p>
 * <p>Description: This is the official source for the Navajo server</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels BV</p>
 * @author Arjen Schoneveld
 * @version $Id$
 */

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoException;
import com.dexels.navajo.document.NavajoFactory;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.Selection;
import com.dexels.navajo.document.types.Binary;
import com.dexels.navajo.document.types.NavajoExpression;
import com.dexels.navajo.document.types.TypeUtils;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.parser.Condition;
import com.dexels.navajo.script.api.Access;
import com.dexels.navajo.script.api.Mappable;
import com.dexels.navajo.script.api.MappableException;
import com.dexels.navajo.script.api.MappableTreeNode;
import com.dexels.navajo.script.api.MappingException;
import com.dexels.navajo.script.api.SystemException;
import com.dexels.navajo.script.api.UserException;
import com.dexels.navajo.server.DispatcherFactory;
import com.dexels.navajo.server.DispatcherInterface;

@SuppressWarnings({ "rawtypes", "unchecked" })
public final class MappingUtils {


    private MappingUtils() {
    	// --- no instances
    }

    public static final String determineNavajoType(Object o) {
    	return TypeUtils.determineNavajoType(o);

    }
    public static final Message[] addMessage(Navajo doc, Message parent, String message, String template, int count, String type,
            String mode, String orderby) throws MappingException {

        Message[] msgs = addMessage(doc, parent, message, template, count, type, mode);

        if (orderby != null && !orderby.equals("")) {
            for (int i = 0; i < msgs.length; i++) {
                msgs[i].setOrderBy(orderby);
            }
        }

        return msgs;
    }

    public static final String getBaseMessageName(String name) {
        if (name.startsWith("../")) {
            return getBaseMessageName(name.substring(3));
        }
        if (name.length() > 0 && name.charAt(0) == '/') {
            return name.substring(1);
        }
        return name;
    }

    /**
     * Get parent message of a (possibly non-existing) message name.
     * 
     * @param parent
     * @param name
     * @return
     */
    private static final Message getParentMessage(Message parent, String name) {
        if (name.startsWith("../")) {
            return getParentMessage(parent.getParentMessage(), name.substring(3));
        }
        return parent;
    }

    /**
     * @param template
     */
    public static final Message[] addMessage(Navajo doc, Message parent, String message, String template, int count, String type,
            String mode) throws NavajoException, MappingException {

        /**
         * Added 22/5/2007: support for relative message creation.
         */
        if ((message.length() == 0 || message.charAt(0) != '/') && message.indexOf(Navajo.MESSAGE_SEPARATOR) != -1 && parent == null) {
            throw new MappingException("No submessage constructs allowed in non-nested <message> tags: " + message);
        }

        Message[] messages = new Message[count];
        Message msg = null;
        int index = 0;

        // Check for existing message.
        Message existing = null;

        /**
         * Get the real parent message given the fact that message could contain a
         * relative name.
         */
        parent = (message.length() > 0 && message.charAt(0) == '/' ? null : getParentMessage(parent, message));

        if (parent != null) {
            existing = parent.getMessage(getBaseMessageName(message));
        } else {
            existing = doc.getMessage(getBaseMessageName(message));
        }

        // If existing message is array message, respect this and make
        // this message my parent instead of reusing existing message.
        if (existing != null && existing.isArrayMessage() && !Message.MSG_TYPE_ARRAY.equals(type)) {
            parent = existing;
            existing = null;
        }

        if (Message.MSG_MODE_OVERWRITE.equals(mode) && existing != null) {
            // remove existing message.

            if (parent != null) {
                parent.removeMessage(existing);
            } else {
                doc.removeMessage(existing);
            }
            existing = null;
        }

        // If there is an existing message withe same name and this message has a parent
        // that is NOT an arrayMessage
        // return this message as a result. If it has an array message parent, it is
        // assumed that the new message
        // is put under the existing array message parent.
        if ((existing != null)) {
            if (parent != null && !parent.isArrayMessage()) {
                messages[0] = existing;
                return messages;
            } else if (parent == null) {
                messages[0] = existing;
                return messages;
            }
        }

        if (getBaseMessageName(message).contains(Navajo.MESSAGE_SEPARATOR)) {
            throw new MappingException("No submessage constructs allowed in messagename: " + message);
        }

        /**
         * Added getBaseMessageName to support relative message creation.
         */
        msg = doc.getNavajoFactory().createMessage(doc, getBaseMessageName(message));

        if (mode != null && !mode.equals("")) {
            msg.setMode(mode);
        }

        if (count > 1) {
            msg.setName(getBaseMessageName(message) + "0");
            msg.setIndex(0);
            // msg.setType(Message.MSG_TYPE_ARRAY);
            // if (!mode.equals(Message.MSG_MODE_IGNORE)) {
            if (parent == null) {
                msg = doc.addMessage(msg, false);
            } else {
                msg = parent.addMessage(msg, false);
            }

            // }
            messages[index++] = msg;
        } else if (count == 1) {
            // if (!mode.equals(Message.MSG_MODE_IGNORE)) {
            if (parent == null) {
                msg = doc.addMessage(msg, false);
            } else {
                msg = parent.addMessage(msg, false);
            }

            // }
            messages[index++] = msg;
            if (type != null && !type.equals("")) {
                msg.setType(type);
            }
        }

        if (Message.MSG_MODE_IGNORE.equals(mode)) {
            msg.setMode(mode);
        }

        // Add additional messages based on the first messages that was added.
        for (int i = 1; i < count; i++) {
            Message extra = doc.copyMessage(msg, doc);
            extra.setName(getBaseMessageName(message) + i);
            extra.setIndex(i);
            if (parent == null) {
                extra = doc.addMessage(extra, false);
            } else {
                extra = parent.addMessage(extra, false);
            }
            messages[index++] = extra;
        }

        return messages;
    }

    private static final String constructGetMethod(String name) {

        StringBuilder methodNameBuffer = new StringBuilder();
        methodNameBuffer.append("get")
        	.append((name.charAt(0) + "").toUpperCase())
        	.append(name.substring(1, name.length()));

        return methodNameBuffer.toString();
    }

    private static final String constructSetMethod(String name) {

    	StringBuilder methodNameBuffer = new StringBuilder();
        methodNameBuffer.append("set")
        	.append((name.charAt(0) + "").toUpperCase())
        	.append(name.substring(1, name.length()));

        return methodNameBuffer.toString();
    }

    private static final Type getTypeForField(String name, Class c, boolean fetchGenericType) throws MappingException {

        if (name.indexOf('(') != -1) {
            name = name.substring(0, name.indexOf('('));
        }

        try {
            if (c.getField(name).getType().equals(Iterator.class) && fetchGenericType) {
                ParameterizedType pt = (ParameterizedType) c.getField(name).getGenericType();
                return (pt.getActualTypeArguments()[0]);
            } else {
                return c.getField(name).getType();
            }
        } catch (Exception e) {
            try {
                if (c.getDeclaredField(name).getType().equals(Iterator.class) && fetchGenericType) {
                    ParameterizedType pt = (ParameterizedType) c.getDeclaredField(name).getGenericType();
                    return (pt.getActualTypeArguments()[0]);
                } else {
                    return c.getDeclaredField(name).getType();
                }
            } catch (Exception e2) {
                Method[] methods = c.getMethods();
                String getMethod = constructGetMethod(name);
                String setMethod = constructSetMethod(name);

                for (int j = 0; j < methods.length; j++) {
                    if (methods[j].getName().equals(getMethod)) {
                        Method m = methods[j];
                        if (m.getReturnType().equals(Iterator.class) && fetchGenericType) {
                            ParameterizedType pt = (ParameterizedType) m.getGenericReturnType();
                            return (pt.getActualTypeArguments()[0]);
                        } else {
                            return m.getReturnType();
                        }

                    } else if (methods[j].getName().equals(setMethod)) {
                        Class[] types = methods[j].getParameterTypes();
                        return types[0];
                    }
                }
                throw new MappingException(
                        "Could not find method " + constructGetMethod(name) + " in Mappable object: " + c.getSimpleName());
            }
        }
    }


    private static final  Object getAttributeObject(MappableTreeNode o, String name, Object[] arguments)
            throws UserException, MappingException {

        Object result = null;
        String methodName = "";

        try {
            java.lang.reflect.Method m = o.getMethodReference(name, arguments);
            result = m.invoke(o.myObject, arguments);
        } catch (IllegalAccessException iae) {
            throw new MappingException(methodName + " illegally accessed in mappable class: " + o.myObject.getClass().getName());
        } catch (InvocationTargetException ite) {
            Throwable t = ite.getTargetException();
            if (t instanceof UserException) {
                throw (UserException) t;
            } else {
                throw new MappingException("Error getting attribute: " + name + " of object: " + o, ite);
            }
        }
        return result;
    }

    /**
     * The next two methods: getAttribute()/setAttribute() are used for the set/get
     * functionality of the new Mappable interface (see also Java Beans standard).
     * The use of set/get methods enables the use of triggers that can be
     * implemented within the set/get methods. So if there is a field: private
     * double noot; within a Mappable object, the following methods need to be
     * implemented: public double getNoot(); and public void setNoot(double d);
     */
    public static final Object getAttributeValue(MappableTreeNode o, String name, Object[] arguments)
            throws UserException, MappingException {

        // The ../ token is used to denote the parent of the current MappableTreeNode.
        // e.g., $../myField or $../../myField is used to identifiy respectively the
        // parent
        // and the grandparent of the current MappableTreeNode.

        while ((name.indexOf("../")) != -1) {
            o = o.parent;
            if (o == null) {
                throw new MappingException("Null parent object encountered: " + name);
            }
            name = name.substring(3, name.length());
        }

        Object result = getAttributeObject(o, name, arguments);

        if (result != null && result.getClass().isArray()) {
            // Encountered array cast to ArrayList.
            Object[] array = (Object[]) result;
            ArrayList list = new ArrayList();
            for (int i = 0; i < array.length; i++) {
                list.add(array[i]);
            }
            return list;
        } else {
            return result;
        }
    }
}
