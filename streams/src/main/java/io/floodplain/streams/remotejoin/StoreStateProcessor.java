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
package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import io.floodplain.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class StoreStateProcessor extends AbstractProcessor<String, ReplicationMessage> {


    private final String lookupStoreName;
    private KeyValueStore<String, ImmutableMessage> lookupStore;
    private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;
    public static final String COMMONKEY = "singlerestore";

    public StoreStateProcessor(String lookupStoreName, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {
        this.lookupStoreName = lookupStoreName;
        this.keyExtractor = keyExtractor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.lookupStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(lookupStoreName);
        super.init(context);
    }

    @Override
    public void process(String key, ReplicationMessage inputValue) {
//        String extracted = keyExtractor.orElse((msg, state) -> COMMONKEY).apply(inputValue.message(), inputValue.paramMessage().orElse(ImmutableFactory.empty())); //  keyExtractor.map(e->e.apply(Optional.of(inputValue.message()),inputValue.paramMessage())).map(e->(String)e.value);
        Optional<ImmutableMessage> paramMessage = inputValue.paramMessage();

        // Use .get() here, I prefer to fail when that is missing//.get();
        if(paramMessage.isEmpty()) {
            throw new RuntimeException("In store state there should definitely be a secondary message");
        }
        ImmutableMessage storeMessage = paramMessage.get();
        lookupStore.put(key, storeMessage);
        super.context().forward(key, ReplicationFactory.standardMessage(storeMessage));
    }

}
