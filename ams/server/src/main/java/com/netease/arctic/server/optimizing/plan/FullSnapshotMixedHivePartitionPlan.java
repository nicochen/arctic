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

package com.netease.arctic.server.optimizing.plan;

import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.optimizing.OptimizingInputProperties;
import com.netease.arctic.server.table.TableRuntime;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.RefUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.util.PropertyUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class FullSnapshotMixedHivePartitionPlan extends MixedIcebergPartitionPlan {

  private final String branch;
  private String customHiveSubdirectory;

  public FullSnapshotMixedHivePartitionPlan(TableRuntime tableRuntime,
                                            ArcticTable table, String partition, long planTime, String branch) {
    super(tableRuntime, table, partition, planTime);
    this.branch = branch;
  }

  private boolean isOptimizingTag() {
    return branch != null;
  }

  @Override
  protected OptimizingInputProperties buildTaskProperties() {
    OptimizingInputProperties properties = super.buildTaskProperties();
    if (isOptimizingTag()) {
      properties.setOutputDir(constructCustomHiveSubdirectory());
    }
    return properties;
  }

  @Override
  protected CommonPartitionEvaluator buildEvaluator() {
    return new FullSnapshotMixedHivePartitionEvaluator(tableRuntime, partition, partitionProperties, planTime,
        isKeyedTable(), branch);
  }

  protected static class FullSnapshotMixedHivePartitionEvaluator extends MixedIcebergPartitionEvaluator {
    private final String branch;

    public FullSnapshotMixedHivePartitionEvaluator(TableRuntime tableRuntime, String partition,
                                                   Map<String, String> partitionProperties, long planTime,
                                                   boolean keyedTable, String branch) {
      super(tableRuntime, partition, partitionProperties, planTime, keyedTable);
      this.branch = branch;
    }

    @Override
    public boolean isFullNecessary() {
      if (branch != null) {
        return true;
      } else {
        return super.isFullNecessary();
      }
    }

    @Override
    protected boolean fileShouldFullOptimizing(DataFile dataFile, List<ContentFile<?>> deleteFiles) {
      if (branch != null) {
        return true;
      } else {
        return super.fileShouldFullOptimizing(dataFile, deleteFiles);
      }
    }
  }

  private String constructCustomHiveSubdirectory() {
    if (customHiveSubdirectory == null) {
      String partitionDateFormat =
          PropertyUtil.propertyAsString(tableObject.properties(), HiveTableProperties.HIVE_PARTITION_FORMAT,
              HiveTableProperties.HIVE_PARTITION_FORMAT_DEFAULT);
      String hivePartitionColumn = PropertyUtil.propertyAsString(tableObject.properties(),
          HiveTableProperties.HIVE_EXTRA_PARTITION_COLUMN, HiveTableProperties.HIVE_EXTRA_PARTITION_COLUMN_DEFAULT);
      String branchFormat =
          PropertyUtil.propertyAsString(tableObject.properties(), TableProperties.AUTO_CREATE_TAG_BRANCH_FORMAT,
              TableProperties.AUTO_CREATE_TAG_BRANCH_FORMAT_DEFAULT);
      LocalDate dateOfRef = RefUtil.getDateOfRef(branch, branchFormat);
      String formattedPartitionDate = dateOfRef.format(DateTimeFormatter.ofPattern(partitionDateFormat));
      customHiveSubdirectory = hivePartitionColumn + "=" + formattedPartitionDate;
    }
    return customHiveSubdirectory;
  }
}
