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
package io.floodplain

import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select

/**
 * Buffered timeout, that will save up entries until a certain size or time limit.
 * See: https://dev.to/psfeng/a-story-of-building-a-custom-flow-operator-buffertimeout-4d95
 * Eventually Kotlin should supply this natively, but afaict it isn't there at the time of writing
 * https://github.com/Kotlin/kotlinx.coroutines/issues/540
 * @param size: After this number of entries, the buffer will be propagated immediately
 * @param duration: The operation will not wait longer for entries than this number (in millis)
 */
@OptIn(kotlinx.coroutines.ObsoleteCoroutinesApi::class)
fun <T> Flow<T>.bufferTimeout(size: Int, duration: Long): Flow<List<T>> {
    require(size > 0) { "Window size should be greater than 0" }
    require(duration > 0) { "Duration should be greater than 0" }

    return flow {
        coroutineScope {
            val events = ArrayList<T>(size)
            val tickerChannel = ticker(duration)
            try {
                val upstreamValues = produce { collect { send(it) } }

                while (isActive) {
                    var hasTimedOut = false

                    select<Unit> {
                        upstreamValues.onReceive {
                            events.add(it)
                        }

                        tickerChannel.onReceive {
                            hasTimedOut = true
                        }
                    }

                    if (events.size == size || (hasTimedOut && events.isNotEmpty())) {
                        emit(events)
                        events.clear()
                    }
                }
            } catch (e: ClosedReceiveChannelException) {
                // drain remaining events
                if (events.isNotEmpty()) emit(events)
            } finally {
                tickerChannel.cancel()
            }
        }
    }
}
