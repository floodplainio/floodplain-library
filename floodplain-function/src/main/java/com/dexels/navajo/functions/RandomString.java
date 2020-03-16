package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.util.Date;
import java.util.Random;

public final class RandomString extends FunctionInterface {

  public final String vcIdent = "$Id$";

  private static Random rand = null;

  private String charSet =
      "abcdefghijklmnopqrstuvwxyzABCDEFJHIJKLMNOPQRSTUVWXYZ0123456789-_";

  // -------------------------------------------------------------- constructors

  public RandomString() {

    if (rand == null) {
      rand = new Random( (new Date()).getTime());
    }

  }

  // ------------------------------------------------------------ public methods

  @Override
public String remarks() {

    return ( "allows generation of random string values given a specified " +
      "length using a specific character set, sometimes useful for testing fixtures ..." +
      "if a character set is not provided, a default one is used" );

  }

  @Override
public final Object evaluate()
    throws TMLExpressionException {

    final Object len = this.getOperand(0);

    if ( ! ( len instanceof Integer ) ) {
      throw
        new TMLExpressionException(
          "Illegal argument type for function RandomString(): " +
          len.getClass().getName() + "  " + this.usage() ) ;
    }

    if ( this.getOperands().size() > 1 ) {

      Object s = this.getOperand(1);

      if (! (s instanceof String)) {
        throw
            new TMLExpressionException(
            "Illegal argument type for function RandomString(): " +
            s.getClass().getName() + "  " + this.usage());
      }

      this.charSet = (String) s;
    }

    // rand.setSeed( ( new Date() ).getTime() );

    final int l = ( (Integer)len ).intValue();
    final StringBuffer result = new StringBuffer( l );

    for ( int i = 0; i < l; i++ ) {
      final int j = rand.nextInt( this.charSet.length() );
      result.insert( i, charSet.charAt(j) );
    }

    return ( result.toString() );

  }

  @Override
public String usage() {
    return( "usage: RandomString( int <length> [, String <char. set> ] )" );
  }

} // public class RandomString extends FunctionInterface

// EOF: $RCSfile$ //
