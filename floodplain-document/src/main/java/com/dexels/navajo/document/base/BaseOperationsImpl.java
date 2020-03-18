package com.dexels.navajo.document.base;

import com.dexels.navajo.document.Method;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


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
