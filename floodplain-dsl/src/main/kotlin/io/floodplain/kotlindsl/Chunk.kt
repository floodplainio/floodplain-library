package io.floodplain.kotlindsl

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
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
