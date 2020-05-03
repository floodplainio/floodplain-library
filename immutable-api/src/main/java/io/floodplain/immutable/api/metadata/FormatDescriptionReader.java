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
package io.floodplain.immutable.api.metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;

/**
 * Read {@link FormatDescription} objects from a semicolon-separated text file.
 *
 * @author Marco Schmidt
 */
public class FormatDescriptionReader implements Serializable {

    private static final long serialVersionUID = -6016285390695170319L;
    private transient BufferedReader in;

    public FormatDescriptionReader() {

    }

    public FormatDescriptionReader(Reader reader) {
        in = new BufferedReader(reader);
    }

    public FormatDescription read() throws IOException {
        String line;
        do {
            line = in.readLine();
            if (line == null) {
                return null;
            }
        }
        while (line.length() < 1 || line.charAt(0) == '#');
        String[] items = line.split(";");
        FormatDescription desc = new FormatDescription();
        desc.setGroup(items[0]);
        desc.setShortName(items[1]);
        desc.setLongName(items[2]);
        desc.addMimeTypes(items[3]);
        desc.addFileExtensions(items[4]);
        desc.setOffset(Integer.valueOf(items[5]));
        desc.setMagicBytes(items[6]);
        desc.setMinimumSize(Integer.valueOf(items[7]));
        return desc;
    }
}
