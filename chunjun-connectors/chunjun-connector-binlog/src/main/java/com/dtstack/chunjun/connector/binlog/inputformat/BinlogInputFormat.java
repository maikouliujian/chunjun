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

package com.dtstack.chunjun.connector.binlog.inputformat;

import com.dtstack.chunjun.connector.binlog.conf.BinlogConf;
import com.dtstack.chunjun.connector.binlog.listener.BinlogAlarmHandler;
import com.dtstack.chunjun.connector.binlog.listener.BinlogEventSink;
import com.dtstack.chunjun.connector.binlog.listener.BinlogJournalValidator;
import com.dtstack.chunjun.connector.binlog.listener.BinlogPositionManager;
import com.dtstack.chunjun.connector.binlog.listener.HeartBeatController;
import com.dtstack.chunjun.connector.binlog.util.BinlogUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public class BinlogInputFormat extends BaseRichInputFormat {

    protected BinlogConf binlogConf;
    protected volatile EntryPosition entryPosition;
    protected List<String> categories = new ArrayList<>();
    protected AbstractCDCRowConverter rowConverter;

    protected transient MysqlEventParser controller;
    protected transient BinlogEventSink binlogEventSink;
    protected List<String> tableFilters;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        LOG.info(
                "binlog FilterBefore:{}, tableBefore: {}",
                binlogConf.getFilter(),
                binlogConf.getTable());
        ClassUtil.forName(BinlogUtil.DRIVER_NAME, getClass().getClassLoader());

        if (StringUtils.isNotEmpty(binlogConf.getCat()) || !binlogConf.isDdlSkip()) {
            if (StringUtils.isNotEmpty(binlogConf.getCat())) {
                // todo binlog监听事件类型同步，比如update、delete、insert
                categories =
                        Arrays.stream(
                                        binlogConf
                                                .getCat()
                                                .toUpperCase()
                                                .split(ConstantValue.COMMA_SYMBOL))
                                .collect(Collectors.toList());
            }

            if (!binlogConf.isDdlSkip()) {
                categories.add(CanalEntry.EventType.CREATE.name());
                categories.add(CanalEntry.EventType.ALTER.name());
                categories.add(CanalEntry.EventType.ERASE.name());
                categories.add(CanalEntry.EventType.TRUNCATE.name());
                categories.add(CanalEntry.EventType.RENAME.name());
                categories.add(CanalEntry.EventType.CINDEX.name());
                categories.add(CanalEntry.EventType.DINDEX.name());
                categories.add(CanalEntry.EventType.QUERY.name());
            }
        }
        /*
         mysql 数据解析关注的表，Perl正则表达式.

        多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)


        常见例子：

        1.  所有表：.*   or  .*\\..*
        2.  canal schema下所有表： canal\\..*
        3.  canal下的以canal打头的表：canal\\.canal.*
        4.  canal schema下的一张表：canal\\.test1

        5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
        */
        String jdbcUrl = binlogConf.getJdbcUrl();
        if (StringUtils.isNotBlank(jdbcUrl)) {
            String database = BinlogUtil.getDataBaseByUrl(jdbcUrl);
            List<String> tables = binlogConf.getTable();
            if (CollectionUtils.isNotEmpty(tables)) {
                tableFilters =
                        tables.stream()
                                // 每一个表格式化为schema.tableName格式
                                .map(t -> BinlogUtil.formatTableName(database, t))
                                .collect(Collectors.toList());
                String filter = String.join(ConstantValue.COMMA_SYMBOL, tableFilters);

                binlogConf.setFilter(filter);
            } else if (StringUtils.isBlank(binlogConf.getFilter())) {
                // 如果table未指定  filter未指定 只消费此schema下的数据
                binlogConf.setFilter(database + "\\..*");
                tableFilters = new ArrayList<>();
                tableFilters.add(database + "\\..*");
            } else if (StringUtils.isNotBlank(binlogConf.getFilter())) {
                tableFilters = Arrays.asList(binlogConf.getFilter().split(","));
            }
            LOG.info(
                    "binlog FilterAfter:{},tableAfter: {}",
                    binlogConf.getFilter(),
                    binlogConf.getTable());
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("binlog openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        LOG.info("binlog openInternal split number:{} start...", inputSplit.getSplitNumber());
        LOG.info("binlog config:{}", JsonUtil.toPrintJson(binlogConf));

        binlogEventSink = new BinlogEventSink(this);
        controller = getController(binlogConf.username, binlogConf.getFilter(), binlogEventSink);

        // 任务启动前 先初始化表结构
        if (binlogConf.isInitialTableStructure()) {
            binlogEventSink.initialTableStructData(tableFilters);
        }
        // todo 监听数据库变更
        controller.start();
    }

    protected MysqlEventParser getController(
            String username, String filter, BinlogEventSink binlogEventSink) {
        MysqlEventParser controller = new MysqlEventParser();
        controller.setConnectionCharset(
                String.valueOf(Charset.forName(binlogConf.getConnectionCharset())));
        controller.setSlaveId(binlogConf.getSlaveId());
        controller.setDetectingEnable(binlogConf.isDetectingEnable());
        controller.setDetectingSQL(binlogConf.getDetectingSQL());
        controller.setMasterInfo(
                new AuthenticationInfo(
                        new InetSocketAddress(binlogConf.getHost(), binlogConf.getPort()),
                        username,
                        binlogConf.getPassword(),
                        BinlogUtil.getDataBaseByUrl(binlogConf.getJdbcUrl())));
        controller.setEnableTsdb(binlogConf.isEnableTsdb());
        controller.setDestination("example");
        controller.setTsdbJdbcUrl("example");
        controller.setTsdbJdbcUserName("example");
        controller.setTsdbJdbcPassword("example");
        controller.setParallel(binlogConf.isParallel());
        controller.setParallelBufferSize(binlogConf.getBufferSize());
        controller.setParallelThreadSize(binlogConf.getParallelThreadSize());
        controller.setIsGTIDMode(binlogConf.isGTIDMode());

        controller.setAlarmHandler(new BinlogAlarmHandler());
        controller.setTransactionSize(binlogConf.getTransactionSize());
        // todo eventparser中持有binlogEventSink
        controller.setEventSink(binlogEventSink);

        controller.setLogPositionManager(new BinlogPositionManager(this));
        // 添加connection心跳回调处理器
        HeartBeatController heartBeatController = new HeartBeatController();
        heartBeatController.setBinlogEventSink(binlogEventSink);
        controller.setHaController(heartBeatController);
        controller.setUseDruidDdlFilter(true);
        controller.setFilterQueryDdl(true);
        controller.setFilterQueryDcl(true);
        controller.setFilterTableError(true);
        EntryPosition startPosition = findStartPosition();
        if (startPosition != null) {
            controller.setMasterPosition(startPosition);
        }

        if (StringUtils.isNotEmpty(filter)) {
            LOG.info("binlogFilter最终值：{},current username: {}", filter, username);
            controller.setEventFilter(new AviaterRegexFilter(filter));
        }
        return controller;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(entryPosition);
        }
        return formatState;
    }

    @Override
    protected RowData nextRecordInternal(RowData row) {
        if (binlogEventSink != null) {
            // todo 从队列中获取数据
            return binlogEventSink.takeRowDataFromQueue();
        }
        LOG.warn("binlog park start");
        LockSupport.park(this);
        LOG.warn("binlog park end...");
        return null;
    }

    @Override
    protected void closeInternal() {
        if (controller != null && controller.isStart()) {
            controller.stop();
            controller = null;
            LOG.info(
                    "binlog closeInternal..., entryPosition:{}",
                    formatState != null ? formatState.getState() : null);
        }
    }

    /**
     * 设置binlog文件起始位置
     *
     * @return
     */
    protected EntryPosition findStartPosition() {
        EntryPosition startPosition = null;
        if (formatState != null
                && formatState.getState() != null
                && formatState.getState() instanceof EntryPosition) {
            startPosition = (EntryPosition) formatState.getState();
            checkBinlogFile(startPosition.getJournalName());
        } else if (MapUtils.isNotEmpty(binlogConf.getStart())) {
            startPosition = new EntryPosition();
            // todo 设置初始化
            String journalName = (String) binlogConf.getStart().get("journal-name");
            checkBinlogFile(journalName);

            if (StringUtils.isNotEmpty(journalName)) {
                startPosition.setJournalName(journalName);
            }

            startPosition.setTimestamp(MapUtils.getLong(binlogConf.getStart(), "timestamp"));
            startPosition.setPosition(MapUtils.getLong(binlogConf.getStart(), "position"));
        }

        return startPosition;
    }

    /**
     * 校验Binlog文件是否存在
     *
     * @param journalName
     */
    private void checkBinlogFile(String journalName) {
        if (StringUtils.isNotEmpty(journalName)) {
            if (!new BinlogJournalValidator(
                            binlogConf.getHost(),
                            binlogConf.getPort(),
                            binlogConf.getUsername(),
                            binlogConf.getPassword())
                    .check(journalName)) {
                throw new IllegalArgumentException("Can't find journal-name: " + journalName);
            }
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public BinlogConf getBinlogConf() {
        return binlogConf;
    }

    public void setBinlogConf(BinlogConf binlogConf) {
        this.binlogConf = binlogConf;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setEntryPosition(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    public AbstractCDCRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
