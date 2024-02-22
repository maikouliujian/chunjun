/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.config;

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.mapping.MappingConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.LinkedList;
//todo jobconfig

/****
 *{
 *   "job": {
 *     "content": [
 *       {
 *         "reader": {
 *           "parameter": {
 *             "table": ["dev.test"],
 *             "password": "Abc12345",
 *             "database": "dev",
 *             "port": 3306,
 *             "cat": "insert,update,delete",
 *             "host": "172.16.100.186",
 *             "jdbcUrl": "jdbc:mysql://172.16.100.186:3306/dev",
 *             "pavingData": true,
 *             "username": "dev"
 *           },
 *           "name": "binlogreader"
 *         },
 *         "writer": {
 *           "parameter": {
 *             "print": true,
 *             "fullColumnName": ["data","database"],
 *             "fullColumnType": ["string","string"],
 *             "writeMode": "overwrite",
 *             "partitionType": "DAY",
 *             "tablesColumn" : "{\"test\":[{\"part\":false,\"comment\":\"\",\"type\":\"INT\",\"key\":\"before_data\"},{\"comment\":\"\",\"type\":\"INT\",\"key\":\"after_data\",\"part\":false},{\"part\":false,\"comment\":\"\",\"type\":\"VARCHAR\",\"key\":\"before_database\"},{\"comment\":\"\",\"type\":\"VARCHAR\",\"key\":\"after_database\",\"part\":false},{\"part\":false,\"comment\":\"\",\"type\":\"TIMESTAMP\",\"key\":\"before_create_date\"},{\"comment\":\"\",\"type\":\"TIMESTAMP\",\"key\":\"after_create_date\",\"part\":false},{\"comment\":\"\",\"type\":\"varchar\",\"key\":\"type\"},{\"comment\":\"\",\"type\":\"varchar\",\"key\":\"schema\"},{\"comment\":\"\",\"type\":\"varchar\",\"key\":\"table\"},{\"comment\":\"\",\"type\":\"bigint\",\"key\":\"ts\"}]}",
 *             "partition": "pt",
 *             "hadoopConfig": {
 *               "dfs.ha.namenodes.ns1": "nn1,nn2",
 *               "dfs.namenode.rpc-address.ns1.nn2": "kudu2:9000",
 *               "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
 *               "dfs.namenode.rpc-address.ns1.nn1": "kudu1:9000",
 *               "dfs.nameservices": "ns1"
 *             },
 *
 *             "jdbcUrl": "jdbc:hive2://kudu2:10000/dev",
 *             "defaultFS": "hdfs://ns1",
 *             "fileType": "orc",
 *             "charsetName": "utf-8",
 *             "username": "admin"
 *           },
 *           "name": "hivewriter"
 *         }
 *       }
 *     ],
 *     "setting": {
 *       "restore": {
 *         "isRestore": true,
 *         "isStream": true
 *       },
 *       "errorLimit": {},
 *       "speed": {
 *         "bytes": 0,
 *         "channel": 1
 *       }
 *     }
 *   }
 * }
 */
@Data
public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1976555497399746622L;
    //todo reader和writer承载配置,reader和writer都在content.get(0)中
    private LinkedList<ContentConfig> content;

    private SettingConfig setting = new SettingConfig();
    //todo reader
    public OperatorConfig getReader() {
        return content.get(0).getReader();
    }
    //todo writer
    public OperatorConfig getWriter() {
        return content.get(0).getWriter();
    }

    public CdcConfig getCdcConf() {
        return content.get(0).getRestoration();
    }

    public MappingConfig getNameMapping() {
        return content.get(0).getNameMapping();
    }
    //todo transformer
    public TransformerConfig getTransformer() {
        return content.get(0).getTransformer();
    }
}
