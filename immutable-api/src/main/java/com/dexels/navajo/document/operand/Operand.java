package com.dexels.navajo.document.operand;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;

import java.util.Date;
import java.util.List;



public class Operand {

  public final ValueType type;
  public final Object value;

  /**
   * Store a new Operand.
   * An operand is an internal Navajo representation of a value object.
   * Value contains the Java representation.
   * Type describes the Navajo type of the object.
   *
   * @param value
   * @param type
   */
  public Operand(Object value, ValueType type) {
	  if(value instanceof Operand) {
		  throw new RuntimeException("Should not embed Operands in other Operands");
	  }
      this.value = value;
      this.type = type;
  }

  public static final Operand FALSE = new Operand(false, ValueType.BOOLEAN);
  public static final Operand TRUE = new Operand(true, ValueType.BOOLEAN);
  public static final Operand NULL = new Operand(null, ValueType.UNKNOWN);
  public static Operand ofBoolean(boolean b) {
	  return b ? TRUE : FALSE;
  }
 
  public static Operand ofString(String string) {
	  return new Operand(string,ValueType.STRING);
  }

  public static Operand ofInteger(Integer value) {
	  return new Operand(value,ValueType.INTEGER);
  }

  public static Operand ofFloat(Double value) {
	  return new Operand(value,ValueType.DOUBLE);
  }

  public static Operand ofClockTime(ClockTime value) {
		return new Operand(value,ValueType.CLOCKTIME);
	}

public static Operand ofDynamic(Object o) {
	if(o==null) {
		return NULL;
	}
	return new Operand(o,TypeUtils.determineNavajoType(o));
}

public static Operand ofDynamic(Object o, ValueType defaultType) {
	if(o==null) {
		return NULL;
	}
	return new Operand(o,TypeUtils.determineNavajoType(o, defaultType));
}

public static Operand ofList(List<? extends Object> result) {
	return new Operand(result,ValueType.LIST);
}

public static Operand ofLong(long longValue) {
	return new Operand(longValue,ValueType.LONG);
}

public static Operand nullOperand(ValueType type) {
	return new Operand(null,type);
}

public static Operand ofCustom(Object value, ValueType type) {
	return new Operand(value,type);
}

public static Operand ofDate(Date value) {
	return new Operand(value,ValueType.DATE);
}

public static Operand ofStopwatchTime(StopwatchTime value) {
		return new Operand(value,ValueType.CLOCKTIME);
	}


public boolean booleanValue() {
	if(value instanceof Boolean) {
		return (boolean) value;
	}
	throw new ClassCastException("Operand does not have the required boolean type but: "+type+" value: "+value);
}

public String stringValue() {
	if(value instanceof String) {
		return (String) value;
	}
	throw new ClassCastException("Operand does not have the required string type but: "+type);
}

public int integerValue() {
	if(value instanceof Integer) {
		return (Integer) value;
	}
	throw new ClassCastException("Operand does not have the required integer type but: "+type);
}

public ImmutableMessage immutableMessageValue() {
	if(value instanceof ImmutableMessage) {
		return (ImmutableMessage) value;
	}
	throw new ClassCastException("Operand does not have the required immutablemessage type but: "+type);
}

@SuppressWarnings("unchecked")
public List<ImmutableMessage> immutableMessageList() {
	if(value instanceof List) {
		return (List<ImmutableMessage>) value;
	} 
	throw new ClassCastException("Operand does not have the required immutablemessage type but: "+type);
}

//
public static Operand ofBinary(Binary o) {
	return new Operand(o,ValueType.BINARY);
}



public static Operand ofImmutable(ImmutableMessage rm) {
	return new Operand(rm,ValueType.IMMUTABLE);
}

public static Operand ofImmutableList(List<ImmutableMessage> rm) {
	return new Operand(rm,ValueType.IMMUTABLELIST);
}

public String toString() {
	return "Operand["+this.type+","+this.value+"]";
}

}
