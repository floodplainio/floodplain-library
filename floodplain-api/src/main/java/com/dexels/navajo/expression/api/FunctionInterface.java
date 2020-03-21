package com.dexels.navajo.expression.api;

import com.dexels.immutable.api.ImmutableMessage;
//import com.dexels.navajo.document.NavajoFactory;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.immutable.api.ImmutableTypeParser;
import com.dexels.navajo.document.operand.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public abstract class FunctionInterface {
    public abstract String remarks();
    public boolean isPure() {
        return true;
    }
    protected List<Operand> operandList = null;
    protected FunctionDefinition myFunctionDefinition = null;
    protected static final HashSet<Class<? extends FunctionInterface>> initialized = new HashSet<>();
    protected static final Map<Class<? extends FunctionInterface>, Class[][]> types = new HashMap<>();
    protected static final Map<Class<? extends FunctionInterface>, ValueType> returnType = new HashMap<>();
    private Class[][] myinputtypes;


    private static final Logger logger = LoggerFactory.getLogger(FunctionInterface.class);

    // Act as if these attributes are final, they can only be set once.
    private static Object semahore = new Object();

    protected final List<Operand> operands() {
        return operandList;
    }

    public void setDefinition(FunctionDefinition fd) {
        myFunctionDefinition = fd;
    }

    private Map<String, Operand> namedParameters;

    private final Optional<ValueType> getMyReturnType() {
        return Optional.ofNullable(returnType.get(this.getClass()));
    }

    private final Class[][] getMyInputParameters() {
        return types.get(this.getClass());
    }

    private final String genPipedParamMsg(Class[] c) {
        if (c == null) {
            return "unknown";
        }
        TypeReader nf = TypeReader.getInstance();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.length; i++) {
            if (c[i] != null) {
                sb.append(nf.getNavajoType(c[i]));
            } else {
                sb.append("empty");
            }
            if (i < c.length - 1) {
                sb.append(" | ");
            }
        }
        return sb.toString();
    }


    public String usage() {
        StringBuilder sb = new StringBuilder();

        sb.append(getMyReturnType());
        sb.append(" " + this.getClass().getSimpleName() + "( ");
        if (getMyInputParameters() != null) {
            for (int i = 0; i < getMyInputParameters().length; i++) {
                sb.append(genPipedParamMsg(getMyInputParameters()[i]));
                if (i < getMyInputParameters().length - 1) {
                    sb.append(", ");
                }
            }
        } else {
            sb.append("unknown");
        }
        sb.append(")");
        return sb.toString();
    }


    // Legacy
    // TODO navajotypes should be enums, but unsure if this method is still needed at all
    public final void setTypes(String[][] navajotypes, ValueType[] navajoReturnType) {
        if (initialized.contains(this.getClass())) {
            return;
        }
        synchronized (semahore) {

            if (initialized.contains(this.getClass())) {
                return;
            }

            if (navajotypes != null) {

                Class[][] mytypes = loadInputTypes(navajotypes);
                types.put(this.getClass(), mytypes);
            }

            if (navajoReturnType != null) {
                // Set returntype.
                Optional<ValueType> myreturnType = loadReturnType(navajoReturnType);
                if(myreturnType.isPresent()) {
                    returnType.put(this.getClass(), myreturnType.get());
                }
            }

            initialized.add(this.getClass());

        }
    }

    private Optional<ValueType> loadReturnType(ValueType[] navajoReturnType) {
        if(navajoReturnType==null) {
            return Optional.empty();
        }
        if(navajoReturnType.length>1 || navajoReturnType.length ==0) {
            return Optional.empty();
        }
        return Optional.of(navajoReturnType[0]);
    }

    private Class[][] loadInputTypes(String[][] navajotypes) {
        // Convert navajo types to Java classes.

        TypeReader nf = TypeReader.getInstance();
        Class[][] mytypes = new Class[navajotypes.length][];
        boolean hasEmptyOptions = false;
        boolean hasMultipleOptions = false;

        for (int i = 0; i < navajotypes.length; i++) {
            mytypes[i] = new Class[navajotypes[i].length];
            boolean emptyOptionSpecified = false;
            boolean multipleOptionsSpecified = false;
            for (int j = 0; j < navajotypes[i].length; j++) {

                if (navajotypes[i][j] != null && navajotypes[i][j].trim().equals("...")) {
                    mytypes[i][j] = Set.class; // Use Set class to denote multiple parameter option.
                    multipleOptionsSpecified = true;
                    hasMultipleOptions = true;
                } else if (navajotypes[i][j] == null || navajotypes[i][j].trim().equalsIgnoreCase("empty")) {
                    mytypes[i][j] = null;
                    emptyOptionSpecified = true;
                    hasEmptyOptions = true;
                } else {
                    mytypes[i][j] = nf.getJavaType(navajotypes[i][j].trim());
                }
            }

            if (hasMultipleOptions && !multipleOptionsSpecified) {
                throw new IllegalArgumentException(
                        "Multiple parameter options can only be specified in one sequence of last parameters.");
            }

            if (hasEmptyOptions && !emptyOptionSpecified && !hasMultipleOptions) {
                throw new IllegalArgumentException(
                        "Empty parameter options can only be specified in one sequence of last parameters.");
            }
        }
        return mytypes;
    }


    public final void reset() {
        operandList = new ArrayList<>();
    }

    public final void insertOperand(Operand o) {
        if(o==null) {
            throw new NullPointerException("Don't add null operands, you can add Operand.NULL though. Function: "+this.getClass().getName());
        }
        operandList.add(o);
    }


    public final void insertStringOperand(String o) {
        insertOperand(Operand.ofString(o));
    }

    public final void insertIntegerOperand(Integer o) {
        operandList.add(Operand.ofInteger(o));
    }

    public final void insertBinaryOperand(Binary o) {
        operandList.add(Operand.ofBinary(o));
    }
    public final void insertFloatOperand(Double o) {
        operandList.add(Operand.ofFloat(o));
    }

    public Object evaluateWithTypeChecking() {
        return evaluate();
    }

    public Operand evaluateWithTypeCheckingOperand() {
        Object o = evaluate();
        ValueType type = TypeUtils.determineNavajoType(o);
        return new Operand(o,type);
    }

    public abstract Object evaluate();

    protected final List<Object> getOperands() {
        return operandList.stream().map(e->e.value).collect(Collectors.toList());
    }
    public void setNamedParameter(Map<String, Operand> named) {
        this.namedParameters = named;
    }

    protected Map<String,Operand> getNamedParameters() {
        return Collections.unmodifiableMap(this.namedParameters);
    }

    public String getStringOperand(int index) {
        return (String) operandWithType(index, ValueType.STRING);
    }

    public Integer getIntegerOperand(int index) {
        return (Integer) operandWithType(index, ValueType.INTEGER);
    }
    public Boolean getBooleanOperand(int index) {
        return (Boolean) operandWithType(index, ValueType.BOOLEAN);
    }


    public Binary getBinaryOperand(int index) {
        return (Binary) operandWithType(index, ValueType.BINARY);
    }
    public BinaryDigest getBinaryDigestOperand(int index) {
        return (BinaryDigest) operandWithType(index, ValueType.BINARY_DIGEST);

    }
    public Date getDateOperand(int index) {
        return (Date) operandWithType(index, ValueType.DATE);
    }


    public void insertDateOperand(Date o) {
        insertOperand(Operand.ofDate(o));
    }

    public void insertFloatOperand(Float o) {
        insertOperand(Operand.ofFloat(o.doubleValue()));

    }

    public void insertStopwatchOperand(StopwatchTime stopwatchTime) {
        insertOperand(Operand.ofStopwatchTime(stopwatchTime));
    }

    public void insertClockTimeOperand(ClockTime clockTime) {
        insertOperand(Operand.ofClockTime(clockTime));
    }

    public void insertBooleanOperand(boolean b) {
        insertOperand(Operand.ofBoolean(b));
    }

    public void insertListOperand(List<? extends Object> list) {
        insertOperand(Operand.ofList(list));
    }

    public void insertLongOperand(long value) {
        insertOperand(Operand.ofLong(value));
    }

    public void insertDynamicOperand(Object value) {
        insertOperand(Operand.ofDynamic(value));
    }

    /**
     * @deprecated
     * @param index
     * @return
     */
    @Deprecated
    protected final Object getOperand(int index) {
        return operand(index).value;
    }

    public Object operandWithType(int index, ValueType type) {
        Operand d = operand(index);
        if(d.value==null) {
            return null;
        }
        if(d.type.equals(type)) {
            return d.value;
        }
        Object value = d.value;
        Class<?> valueClass = value == null ? null : value.getClass();
        throw new TMLExpressionException("Illegal operand type operand (index = " + index + ") should be of type: "+ ImmutableTypeParser.typeName(type)+" but was of type: "+ImmutableTypeParser.typeName(d.type)+" the value class is: "+valueClass);
    }
    protected Operand operand(int index) {
        if (index >= operandList.size())
            throw new TMLExpressionException("Function Exception: Missing operand (index = " + index + ")");
        else
            return operandList.get(index);
    }

    public Optional<ValueType> getReturnType() {
        ValueType type = returnType.get(this.getClass());
        if(type==null || type.equals("any")) {
            return Optional.empty();
        }
        return Optional.ofNullable(type);
    }

    public boolean isInitialized() {
        return initialized.contains(this.getClass());
    }

    @SuppressWarnings("unused")
    public List<String> typeCheck(List<ContextExpression> l, String expression) {
        // Disabled for now

        if(true) {
            return Collections.emptyList();
        }
        String[][] inputTypes = myFunctionDefinition.getInputParams();
        if(inputTypes==null) {
            logger.warn("Can't type check function {} as it does not have defined input types. Expression: {}",myFunctionDefinition.getFunctionClass(),expression);
        } else {
            int i = 0;
            for (String[] classes : inputTypes) {
                int j = 0;
                for (String c : classes) {
                    logger.info("class: {} type: {} -> {}",i,j,c);
                    j++;
                }
                i++;
            }
            int index = 0;
            for (String[] alternatives : inputTypes) {
                logger.info("Number of alternatives for arg # {} is :{}",index,alternatives.length);
                List<String> typeCheckProblems = typeCheckOption(alternatives,l,index,expression);
                if(typeCheckProblems.isEmpty()) {
                    logger.info("Found typechecked option!");
                    return typeCheckProblems;
                }
                index++;
            }
        }
        return Arrays.asList("Could not find a suitable type solution to this function: "+getClass().getName()+" in expression: "+expression);
    }

    private List<String> typeCheckOption(String[] alternatives, List<ContextExpression> l, int argumentIndex, String expression) {
        int argumentNumber = 0;
        List<String> problems = new ArrayList<>();
        for (ContextExpression contextExpression : l) {
            Optional<ValueType> foundReturnType = contextExpression.returnType();
            if(!foundReturnType.isPresent()) {
                continue;
            }
            // TODO fix alternatives
            ValueType rt = foundReturnType.get();
            boolean isCompatible = isCompatible(rt,alternatives[argumentNumber]);
            if(!isCompatible) {
                problems.add("Argument # "+argumentNumber+" of type: "+alternatives[argumentNumber]+" is incompatible with: "+rt);
            }
            argumentNumber++;
        }
        return problems;
    }

    private boolean isCompatible(ValueType rt, String inputType) {
        if(rt==null || inputType==null) {
            return true;
        }
        return rt.equals(inputType);
    }

}
