package com.netease.arctic.server.optimizing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.io.ArcticHadoopFileIO;
import com.netease.arctic.io.DataTestHelpers;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.RefUtil;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.thrift.TException;

import java.io.IOException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TestMixedHiveOptimizing extends AbstractOptimizingTest {
  private final ArcticTable arcticTable;
  private final HiveMetaStoreClient hiveClient;
  private final BaseOptimizingChecker checker;

  public TestMixedHiveOptimizing(ArcticTable arcticTable, HiveMetaStoreClient hiveClient) {
    this.arcticTable = arcticTable;
    this.hiveClient = hiveClient;
    this.checker = new BaseOptimizingChecker(arcticTable.id());
  }

  public void testHiveKeyedTableMajorOptimizeNotMove() throws TException, IOException {
    KeyedTable table = arcticTable.asKeyedTable();
    // Step1: write 1 data file into base node(0,0)
    updateProperties(table, TableProperties.BASE_FILE_INDEX_HASH_BUCKET, 1 + "");
    writeBase(table, rangeFromTo(1, 100, "aaa", quickDateWithZone(3)));
    // wait Full Optimize result
    OptimizingProcessMeta optimizeHistory = checker.waitOptimizeResult();
    checker.assertOptimizingProcess(optimizeHistory, OptimizingType.FULL, 1, 1);
    assertIdRange(readRecords(table), 1, 100);
    // assert file are in hive location
    assertIdRange(readHiveTableData(), 1, 100);

    // Step2: write 1 change delete record
    writeChange(table, Lists.newArrayList(
        newRecord(101, "aaa", quickDateWithZone(3))
    ), Lists.newArrayList(
        newRecord(1, "aaa", quickDateWithZone(3))
    ));
    // wait Minor Optimize result, generate 1 pos-delete file and 1 data file, but not move to hive location
    optimizeHistory = checker.waitOptimizeResult();
    checker.assertOptimizingProcess(optimizeHistory, OptimizingType.MINOR, 3, 2);
    assertIdRange(readRecords(table), 2, 101);
    assertIdRange(readHiveTableData(), 1, 100);

    // Step3: write 2 small files to base
    writeBase(table, rangeFromTo(102, 103, "aaa", quickDateWithZone(3)));
    // should not optimize with 1 small file
    checker.assertOptimizeHangUp();
    writeBase(table, rangeFromTo(104, 105, "aaa", quickDateWithZone(3)));
    // wait Minor Optimize result, generate 1 data file, but not move to hive location
    optimizeHistory = checker.waitOptimizeResult();
    checker.assertOptimizingProcess(optimizeHistory, OptimizingType.MINOR, 3, 1);
    assertIdRange(readRecords(table), 2, 105);
    assertIdRange(readHiveTableData(), 1, 100);

    checker.assertOptimizeHangUp();
  }

  public void testHiveKeyedTableMajorOptimizeAndMove() throws TException, IOException {
    KeyedTable table = arcticTable.asKeyedTable();
    // Step1: write 1 data file into base node(0,0)
    updateProperties(table, TableProperties.BASE_FILE_INDEX_HASH_BUCKET, 1 + "");
    updateProperties(table, TableProperties.SELF_OPTIMIZING_FULL_TRIGGER_INTERVAL, 1000 + "");
    updateProperties(table, TableProperties.SELF_OPTIMIZING_FULL_REWRITE_ALL_FILES, false + "");
    writeBase(table, rangeFromTo(1, 100, "aaa", quickDateWithZone(3)));
    // wait Full Optimize result
    OptimizingProcessMeta optimizeHistory = checker.waitOptimizeResult();
    checker.assertOptimizingProcess(optimizeHistory, OptimizingType.FULL, 1, 1);
    assertIdRange(readRecords(table), 1, 100);
    // assert file are in hive location
    assertIdRange(readHiveTableData(), 1, 100);

    // Step2: write 1 small file to base
    writeBase(table, rangeFromTo(101, 102, "aaa", quickDateWithZone(3)));
    // wait Major Optimize result, generate 1 data file from 2 small files, but not move to hive location
    optimizeHistory = checker.waitOptimizeResult();
    checker.assertOptimizingProcess(optimizeHistory, OptimizingType.FULL, 1, 1);
    assertIdRange(readRecords(table), 1, 102);
    assertIdRange(readHiveTableData(), 1, 102);

    checker.assertOptimizeHangUp();
  }

  public void testFullSnapshotHiveKeyedTable() throws TException, IOException, InterruptedException {
    KeyedTable table = arcticTable.asKeyedTable();
    updateProperties(table, TableProperties.ENABLE_SELF_OPTIMIZING, "false");
    // Step1: write 1 data file into base node(0,0)
    updateProperties(table, TableProperties.BASE_FILE_INDEX_HASH_BUCKET, 1 + "");
    writeBase(table, rangeFromTo(1, 100, "aaa", quickDateWithZone(3)));
    Thread.sleep(2);
    table.changeTable().newAppend().commit();
    String branchName = RefUtil.getDayBranchName(LocalDate.now().minusDays(1), arcticTable.properties());
    String tagName = RefUtil.getDayTagName(LocalDate.now().minusDays(1), arcticTable.properties());
    checker.waitBranch(arcticTable, branchName);
    updateProperties(table, TableProperties.ENABLE_SELF_OPTIMIZING, "true");

    // wait Full Optimize result
    OptimizingProcessMeta optimizeHistory = checker.waitOptimizeResult();
    checker.assertOptimizingProcess(optimizeHistory, OptimizingType.FULL, 1, 1);

    checker.checkBranchNotExist(table, branchName);
    checker.checkTagExist(table, tagName);

    assertIdRange(readRecords(table), 1, 100);
    // assert file are in hive location
    assertIdRange(readHivePartitionData(), 1, 100);

    checker.assertOptimizeHangUp();
  }

  private Record newRecord(Object... val) {
    return newRecord(arcticTable.schema(), val);
  }

  public void emptyCommit(KeyedTable table) {
    AppendFiles appendFiles = table.changeTable().newAppend();
    appendFiles.commit();
  }

  protected List<Record> rangeFromTo(int from, int to, String name, OffsetDateTime dateTime) {
    List<Record> records = new ArrayList<>();
    for (Integer id : range(from, to)) {
      records.add(newRecord(id, name, dateTime));
    }
    return records;
  }
  
  private List<Record> readHivePartitionData() throws TException, IOException {
    Table table = hiveClient.getTable(arcticTable.id().getDatabase(), arcticTable.id().getTableName());
    List<Partition> partitions =
        hiveClient.listPartitions(arcticTable.id().getDatabase(), arcticTable.id().getTableName(), (short) 1);
    if (partitions == null || partitions.isEmpty()) {
      return Collections.emptyList();
    }
    String location = partitions.get(0).getSd().getLocation();
    List<String> files = filesInLocation(location);
    List<Record> records = new ArrayList<>();
    for (String file : files) {
      records.addAll(DataTestHelpers.readDataFile(FileFormat.PARQUET, arcticTable.schema(), file));
    }
    return records;
  }

  private List<Record> readHiveTableData() throws TException, IOException {
    Table table = hiveClient.getTable(arcticTable.id().getDatabase(), arcticTable.id().getTableName());
    String location = table.getSd().getLocation();
    List<String> files = filesInLocation(location);
    List<Record> records = new ArrayList<>();
    for (String file : files) {
      records.addAll(DataTestHelpers.readDataFile(FileFormat.PARQUET, arcticTable.schema(), file));
    }
    return records;
  }

  private List<String> filesInLocation(String location) {
    ArcticHadoopFileIO io = ((SupportHive) arcticTable).io();
    return Streams.stream(io.listDirectory(location))
        .map(FileInfo::location)
        .collect(Collectors.toList());
  }
}
