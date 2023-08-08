/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.op;

import com.netease.arctic.table.BaseTable;
import com.netease.arctic.table.KeyedTable;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateStatistics;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract transaction operation on {@link BaseTable} which will change
 * max transaction id map
 */
public abstract class PartitionTransactionOperation implements PendingUpdate<StatisticsFile> {

  KeyedTable keyedTable;
  private Transaction tx;
  private boolean skipEmptyCommit = false;

  protected final Map<String, String> properties;

  public PartitionTransactionOperation(KeyedTable keyedTable) {
    this.keyedTable = keyedTable;
    this.properties = new HashMap<>();
  }

  public PartitionTransactionOperation set(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  /**
   * If this operation change nothing.
   *
   * @return true for nothing will be changed
   */
  protected abstract boolean isEmptyCommit();

  /**
   * Add some operation in transaction and change optimized sequence map.
   *
   * @param transaction table transaction
   * @return changed partition properties
   */
  protected abstract StatisticsFile apply(Transaction transaction);

  @Override
  public StatisticsFile apply() {
    return apply(tx);
  }

  /**
   * Skip empty commit.
   *
   * @return this for chain
   */
  public PartitionTransactionOperation skipEmptyCommit() {
    this.skipEmptyCommit = true;
    return this;
  }

  public void commit() {
    if (this.skipEmptyCommit && isEmptyCommit()) {
      return;
    }
    this.tx = keyedTable.baseTable().newTransaction();

    StatisticsFile statisticsFile = apply();
    if (statisticsFile != null) {
      UpdateStatistics updateStatistics = this.tx.updateStatistics();
      updateStatistics.setStatistics(statisticsFile.snapshotId(), statisticsFile);
      updateStatistics.commit();
    }

    tx.commitTransaction();
  }
}
