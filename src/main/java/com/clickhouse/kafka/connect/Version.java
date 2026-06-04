/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.clickhouse.kafka.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String VERSION;

    private static final String VERSION_FILE = "/clickhouse-kafka-connect-version.properties";

    static {
        String v = "unknown";
        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = Version.class.getResourceAsStream(VERSION_FILE)) {
                props.load(versionFileStream);
                v = props.getProperty("version", v).trim();
            }
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
        VERSION = v;
    }

    private Version() {}

    public static String getVersion() {
        return VERSION;
    }
}
