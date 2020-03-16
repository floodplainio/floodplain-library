package com.dexels.navajo.functions;

import com.dexels.navajo.document.*;
import com.dexels.navajo.document.types.Money;
import com.dexels.navajo.document.types.Percentage;
import com.dexels.navajo.expression.api.StatefulFunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.parser.Condition;
import com.dexels.navajo.parser.Expression;
import com.dexels.navajo.script.api.SystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Title: Navajo Product Project</p>
 * <p>Description: This is the official source for the Navajo server</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels BV</p>
 * @author Arjen Schoneveld
 * @version 1.0
 */

public class SumProperties extends StatefulFunctionInterface {

	private final static Logger logger = LoggerFactory.getLogger(SumProperties.class);

  @Override
public String remarks() {
    return "Sums all properties in an array message";
  }

  @Override
public Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {

    if (getOperands().size() < 2) {
      for (int i = 0; i < getOperands().size(); i++) {
        Object o = getOperands().get(i);
        logger.info("Operand # " + i + " is: " + o.toString() + " - " +
                           o.getClass());
      }
      throw new TMLExpressionException(this,
                                       "Wrong number of arguments: " +
                                       getOperands().size());
    }
    if (! (getOperand(0) instanceof String && getOperand(1) instanceof String)) {
      throw new TMLExpressionException(this,
                                       "Wrong argument types: " +
                                       getOperand(0).getClass() + " and " +
                                       getOperand(1).getClass());
    }
    String messageName = (String) getOperand(0);
    String propertyName = (String) getOperand(1);
    String filter = null;
    if (getOperands().size() > 2) {
      filter = (String) getOperand(2);
    }
    Message parent = getCurrentMessage();
    Navajo doc = getNavajo();

    try {
      List<Message> arrayMsg = (parent != null ? parent.getMessages(messageName) :
                            doc.getMessages(messageName));
      if (arrayMsg == null) {
        throw new TMLExpressionException(this,
            "Empty or non existing array message: " + messageName);
      }

      String sumType = "int";
      double sum = 0;
      for (int i = 0; i < arrayMsg.size(); i++) {
        Message m = arrayMsg.get(i);
        Property p = m.getProperty(propertyName);
        boolean evaluate = (filter != null ?
                            Condition.evaluate(filter, doc, null, m,getAccess()) : true);
        if (evaluate) {
          if (p != null) {
            Object o = p.getTypedValue();
            if (o == null) {
              continue;
            }
            if ( 
                ! (o instanceof Integer || o instanceof Double ||
                   o instanceof Float || o instanceof Money ||
                   o instanceof Percentage || o instanceof Boolean || o instanceof String)) {
              throw new TMLExpressionException(this,
                  "Only numbers are supported a sum. Not: " +
                                               (o.getClass().toString())+" value: "+o);
            }
            if (o instanceof String) {
            	if ("".equals(o)) {
					// ignore
				} else {
					logger.error("Only numbers are supported a sum. Not strings. Value:  "+o);
		              throw new TMLExpressionException(this,
		                      "Only numbers are supported a sum. Not strings. Value:  "+o +
		                                                   (o.getClass().toString()));

				}
              }
            if (o instanceof Integer) {
              sumType = "int";
              sum += ( (Integer) o).doubleValue();
            }
            else if (o instanceof Double) {
//              if (!((Double)o).equals(Double.valueOf(Double.NaN))) {
              sumType = "float";
              sum += ( (Double) o).doubleValue();
//              }
            }
            else if (o instanceof Float) {
//              if (!((Float)o).equals(new Float(Float.NaN))) {
              sumType = "float";
              sum += ( (Float) o).doubleValue();
//              }
            }
            else if (o instanceof Money) {
//              if (!Double.valueOf(((Money)o).doubleValue()).equals(Double.valueOf(Float.NaN))) {
              sumType = "money";
              sum += ( (Money) o).doubleValue();
//              }
            }
            else if (o instanceof Percentage) {
//              if (!Double.valueOf(((Money)o).doubleValue()).equals(Double.valueOf(Float.NaN))) {
              sumType = "percentage";
              sum += ( (Percentage) o).doubleValue();
//              }
            } else if (o instanceof Boolean) {
              sumType = "int";
              sum += ( (Boolean) o).booleanValue() ? 1 : 0;
            }

          }
          else {
            throw new TMLExpressionException(this,
                                             "Property does not exist: " + propertyName);
          }
        }
      }
      if (sumType.equals("int")) {
        return Integer.valueOf( (int) sum);
      }
      else if (sumType.equals("money")) {
        return new Money(sum);

      }
      else if (sumType.equals("percentage")) {
        return new Percentage(sum);
      }
      else {
        return Double.valueOf(sum);
      }
    }
    catch (NavajoException ne) {
      throw new TMLExpressionException(this, ne.getMessage());
    }
    catch (SystemException se) {
      throw new TMLExpressionException(this, se.getMessage());
    }
  }

  @Override
public String usage() {
    return "SumProperties(<Array message name>,<Property name>[,<filter>])";
  }

  public static void main(String[] args) throws Exception {

    System.setProperty("com.dexels.navajo.DocumentImplementation", "com.dexels.navajo.document.nanoimpl.NavajoFactoryImpl");

    Navajo doc = NavajoFactory.getInstance().createNavajo();
    Message top = NavajoFactory.getInstance().createMessage(doc, "Top");
    doc.addMessage(top);
    Message array = NavajoFactory.getInstance().createMessage(doc, "MyArray",
        Message.MSG_TYPE_ARRAY);
    top.addMessage(array);
    for (int i = 0; i < 9; i++) {
      Message elt = NavajoFactory.getInstance().createMessage(doc, "MyArray",
          Message.MSG_TYPE_ARRAY_ELEMENT);
      array.addMessage(elt);
      Message array2 = NavajoFactory.getInstance().createMessage(doc, "NogEenArraytje"+i,
        Message.MSG_TYPE_ARRAY);
    elt.addMessage(array2);
    for (int j = 0; j < 2; j++) {
      Message elt2 = NavajoFactory.getInstance().createMessage(doc, "NogEenArraytje"+i,
          Message.MSG_TYPE_ARRAY_ELEMENT);
      array2.addMessage(elt2);
      Property p = NavajoFactory.getInstance().createProperty(doc,
          "MyBoolean",
          Property.BOOLEAN_PROPERTY, Property.TRUE, 0, "", Property.DIR_OUT);
      elt2.addProperty(p);
    }
    }
    //doc.write(System.err);

    Operand result = Expression.evaluate(
        "SumProperties('/Top/MyArray.*/NogEenArraytje.*', 'MyBoolean')", doc);
    System.err.println("result = " + result.value);
  }

}
