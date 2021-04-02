/*
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
package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.message.IMessage
import java.math.BigInteger

private val logger = mu.KotlinLogging.logger {}

fun createPublicId(prefix: String, prime: Int, modInverse: Int, random: Int, field: Int): String {
    val opt: Optimus = Optimus(prime, modInverse, random)
    val result = opt.encode(field)
    return prefix + result
}

fun filterValidCalendarActivityId(key: String, calendarDay: IMessage): Boolean {
    val activityId = calendarDay.optionalInteger("activityid")
    if (activityId == null) {
        logger.warn(
            "Null activityid! key: {}. Message: {}",
            key,
            calendarDay.toString()
        )
        return false
    }
    return activityId >= 20
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
private class Optimus(prime: Int, modInverse: Int, randomNumber: Int) {
    private val prime: Int
    private val modInverse: Int
    private val randomNumber: Int
    fun encode(n: Int): Int {
        return n * prime and Int.MAX_VALUE xor randomNumber
    }

    fun decode(n: Int): Int {
        return (n xor randomNumber) * modInverse and Int.MAX_VALUE
    }

    init {
        require(isProbablyPrime(prime)) { String.format("%d is not a prime number", prime) }
        this.prime = prime
        this.modInverse = modInverse
        this.randomNumber = randomNumber
    }
}

fun ModInverse(n: Int): Int {
    val p = BigInteger.valueOf(n.toLong())
    val l = java.lang.Long.valueOf(Int.MAX_VALUE.toLong()) + 1L
    val m = BigInteger.valueOf(l)
    return p.modInverse(m).toInt()
}

fun isProbablyPrime(n: Int): Boolean {
    return BigInteger.valueOf((n - 1).toLong()).nextProbablePrime() == BigInteger.valueOf(n.toLong())
}
