package com.dexels.navajo.document;


public class NavajoException extends RuntimeException {

	private static final long serialVersionUID = 8965495998372888144L;


	public NavajoException(Throwable e) {
		super(e);
	}
	
  /**
   * Default constructor, calls superclass
   */
  public NavajoException() {
      super();
    }

    /**
     * Construct a NavajoException with a given message (text)
     * @param message String
     */
    public NavajoException(String message) {
      super(message);
    }

    
    /**
     * Construct a NavajoException with a given message (text) AND a root cause
     * @param message String
     */
    public NavajoException(String message, Throwable root) {
      super(message,root);
    }

    
    /**
     * Gets the wrapped exception.
     *
     * @return the wrapped exception.
     */
   
}
