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
package io.floodplain.streams.api;

import java.util.Objects;

public class ProcessorName {
    private final String definition;

    @Deprecated
    public static ProcessorName from(String definition) {
        return new ProcessorName(definition);
    }
    private ProcessorName(String definition) {
        this.definition = definition;
    }

    public String definition() {
        return definition;
    }

    public String toString() {
        return definition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProcessorName)) return false;
        ProcessorName that = (ProcessorName) o;
        return definition.equals(that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition);
    }
}
