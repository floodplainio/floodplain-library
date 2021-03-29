/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.kotlindsl.example;

import java.math.BigInteger;

public class CreatePublicIdTransformer {
    private static final int MAX_INT = Integer.MAX_VALUE;

    public String apply(String prefix, int prime, int modInverse, int random, int field) {
        Optimus opt = new Optimus(prime, modInverse, random);
        int result =  opt.encode(field);
        return prefix + result;
    }

    /*
     * Taken from https://github.com/jadrio/optimus-java at 21-07-2017
     * The MIT License (MIT)
     * Copyright (c) 2015 Jose Diaz
     */
    private class Optimus {

        private final int prime;
        private final int modInverse;
        private final int randomNumber;

        public Optimus(int prime, int modInverse, int randomNumber) {

            if (!isProbablyPrime(prime))
                throw new IllegalArgumentException(String.format("%d is not a prime number", prime));

            this.prime = prime;
            this.modInverse = modInverse;
            this.randomNumber = randomNumber;
        }

        public int encode(int n) {
            return ((n * this.prime) & MAX_INT) ^ this.randomNumber;
        }

        @SuppressWarnings("unused")
		public int decode(int n) {
            return ((n ^ this.randomNumber) * this.modInverse) & MAX_INT;
        }

    }

    public static int ModInverse(int n) {

        BigInteger p = BigInteger.valueOf(n);
        long l = Long.valueOf(MAX_INT) + 1L;
        BigInteger m = BigInteger.valueOf(l);
        return p.modInverse(m).intValue();
    }

    public static boolean isProbablyPrime(int n) {
        return BigInteger.valueOf(n - 1).nextProbablePrime().equals(BigInteger.valueOf(n));
    }

}
