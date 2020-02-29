package com.dexels.navajo.document.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dexels.navajo.document.Method;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.Operation;

/**
 * <p>Title: ShellApplet</p>
 * <p>Description: </p>
 * <p>Part of the Navajo mini client, based on the NanoXML parser</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels </p>
 * <p>$Id: b6437960c0b137527583198ef22cbefc84c9b26a $</p>
 * @author Frank Lyaruu
 * @version $Revision$
 */
public class BaseOperationsImpl extends BaseNode {

	private static final long serialVersionUID = 9105044646681827267L;
	private final List<BaseNode> myOperations = new ArrayList<>();
  public BaseOperationsImpl(Navajo n) {
    super(n);
  }

    @Override
	public Map<String,String> getAttributes() {
        return null;
    }

    @Override
	public List<BaseNode> getChildren() {
        return myOperations;
    }

    @Override
	public String getTagName() {
        return "operations";
    }

   
    public void addOperation(Operation m) {
        if (!(m instanceof BaseOperationImpl)) {
            throw new IllegalArgumentException("Wrong impl, ouwe!");
        }
        BaseOperationImpl bmi = (BaseOperationImpl)m;
        myOperations.add(bmi);
    }
    
    public Method getMethod(String s) {
        for (int i = 0; i < myOperations.size(); i++) {
          Method m = (Method)myOperations.get(i);
          if (m.getName().equals(s)) {
            return m;
          }
        }
        return null;
      }

    public List<Operation> getAllOperations() {
    	List<Operation> al = new ArrayList<>();
        for (int i = 0; i < myOperations.size(); i++) {
        	Operation m = (Operation) myOperations.get(i);
            al.add(m);
        }
        return al;
    }

    public void clear() {
    	myOperations.clear();
    }
    
}

// EOF $RCSfile$ //
