package com.dexels.kafka.streams.transformer.custom;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.math.BigInteger;
import java.util.Map;

public class CreatePublicIdTransformer implements MessageTransformer {
    private static final int MAX_INT = Integer.MAX_VALUE;

    @Override
    public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
        String fieldId = params.get("field");
        String prefix = params.get("prefix");
        Object fieldValue = msg.columnValue(fieldId);
        int field;
        if (fieldValue instanceof Integer) {
            field = (Integer) fieldValue;
        } else {
            field = Integer.valueOf(fieldValue.toString());
        }

        String to = params.get("to");
        Integer prime = Integer.valueOf(params.get("prime"));
        Integer modInverse = Integer.valueOf(params.get("modInverse"));
        Integer random = Integer.valueOf(params.get("random"));

        Optimus opt = new Optimus(prime, modInverse, random);
        int result =  opt.encode(field);
        return msg.with(to, prefix + result, "string");
    }

    /*
     * Taken from https://github.com/jadrio/optimus-java at 21-07-2017
     * 
     * 
     * The MIT License (MIT)
     * 
     * Copyright (c) 2015 Jose Diaz
     * 
     * Permission is hereby granted, free of charge, to any person obtaining a copy
     * of this software and associated documentation files (the "Software"), to deal
     * in the Software without restriction, including without limitation the rights
     * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
     * copies of the Software, and to permit persons to whom the Software is
     * furnished to do so, subject to the following conditions:
     * 
     * The above copyright notice and this permission notice shall be included in
     * all copies or substantial portions of the Software.
     * 
     * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
     * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
     * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
     * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
     * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
     * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
     * SOFTWARE.
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

        @SuppressWarnings("unused")
		public Optimus(int prime, int randomNumber) {
            this(prime, ModInverse(prime), randomNumber);
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
