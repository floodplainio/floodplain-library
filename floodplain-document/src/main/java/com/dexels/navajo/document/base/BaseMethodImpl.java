package com.dexels.navajo.document.base;

import com.dexels.navajo.document.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseMethodImpl extends BaseNode implements Method {
    private static final long serialVersionUID = 4307457202134807432L;
    protected List<BaseRequiredImpl> myRequiredMessages = new ArrayList<BaseRequiredImpl>();
    protected String myName = "";
    protected Message myParent = null;
    protected String myDescription = null;
    protected String myServer;

    public BaseMethodImpl(Navajo n) {
        super(n);
    }

    public BaseMethodImpl(Navajo n, String name) {
        super(n);
        myName = name;
    }

    @Override
    public final List<String> getRequiredMessages() {
        List<String> result = new ArrayList<String>();
        for (Required required : myRequiredMessages) {
            result.add(required.getMessage());
        }
        return result;
    }

    public final void setAllRequired(List<BaseRequiredImpl> al) {
        myRequiredMessages.addAll(al);
    }

    @Override
    public final String getName() {
        return myName;
    }

    @Override
    public final String getDescription() {
        return myDescription;
    }

    @Override
    public final void setDescription(String s) {
        myDescription = s;
    }

    public final void setParent(Message m) {
        myParent = m;
    }

    public final Message getParent() {
        return myParent;
    }

    @Override
    public final void setServer(String s) {
        myServer = s;
    }

    @Override
    public final String getServer() {
        return myServer;
    }

    @Override
    public final Method copy(Navajo n) {

        BaseMethodImpl m = (BaseMethodImpl) NavajoFactory.getInstance().createMethod(n, getName(), getServer());
        for (String d : getRequiredMessages()) {
            m.addRequired(d);
        }
        return m;
    }

    public final String getPath() {
        if (myParent != null) {
            return myParent.getFullMessageName() + "/" + getName();
        }
        return "/" + getName();
    }

    @Override
    public final void setName(String name) {
        myName = name;
    }

    @Override
    public final void addRequired(String message) {
        addRequired(message, null);
    }

    @Override
    public final void addRequired(String message, String filter) {

    }

    @Override
    public final void addRequired(Message message) {
        BaseRequiredImpl bri = new BaseRequiredImpl();
        bri.setMessage(message.getFullMessageName());
        myRequiredMessages.add(bri);
    }


    @Override
    public Map<String, String> getAttributes() {
        Map<String, String> m = new HashMap<String, String>();
        m.put("name", myName);
        return m;
    }

    @Override
    public List<BaseNode> getChildren() {
        // does not serialize required parts of methods. don't know why, but it
        // really couldn't work
        return null;
    }

    @Override
    public String getTagName() {
        return "method";
    }

    @Override
    public Object getRef() {
        throw new UnsupportedOperationException("getRef not possible on base type. Override it if you need it");
    }

}