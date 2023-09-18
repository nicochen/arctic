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

package com.netease.arctic.hive.catalog;

import com.netease.arctic.BasicTableTestHelper;
import com.netease.arctic.TableTestHelper;
import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.catalog.TestMixedCatalog;
import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.hive.TestHMS;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

import static com.netease.arctic.hive.HiveTableProperties.ARCTIC_TABLE_FLAG;
import static com.netease.arctic.hive.HiveTableProperties.ARCTIC_TABLE_ROOT_LOCATION;

@RunWith(JUnit4.class)
public class TestMixedHiveCatalog extends TestMixedCatalog {

  private static final PartitionSpec IDENTIFY_SPEC = PartitionSpec.builderFor(BasicTableTestHelper.TABLE_SCHEMA)
      .identity("op_time").build();

  @ClassRule
  public static TestHMS TEST_HMS = new TestHMS();

  public TestMixedHiveCatalog() {
    super(new HiveCatalogTestHelper(TableFormat.MIXED_HIVE, TEST_HMS.getHiveConf()));
  }

  @Override
  protected PartitionSpec getCreateTableSpec() {
    return IDENTIFY_SPEC;
  }

  private void validateTableArcticProperties(TableIdentifier tableIdentifier) throws TException {
    String dbName = tableIdentifier.getDatabase();
    String tbl = tableIdentifier.getTableName();
    Map<String, String> tableParameter = TEST_HMS.getHiveClient()
        .getTable(dbName, tbl).getParameters();

    Assert.assertTrue(tableParameter.containsKey(ARCTIC_TABLE_ROOT_LOCATION));
    Assert.assertTrue(tableParameter.get(ARCTIC_TABLE_ROOT_LOCATION).endsWith(tbl));
    Assert.assertTrue(tableParameter.containsKey(ARCTIC_TABLE_FLAG));
  }

  @Override
  protected void validateCreatedTable(ArcticTable table) throws TException {
    super.validateCreatedTable(table);
    validateTableArcticProperties(table.id());
  }

  @Override
  protected void assertIcebergTableStore(Table tableStore, boolean isBaseStore, boolean isKeyedTable) {
    // mixed-hive does not check the table store
  }

  @Test
  public void testCreateKeyedTableWithTag() throws TException {
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION,
        HiveTableProperties.BASE_HIVE_PARTITION_PROJECTION_MODE_TAG);

    KeyedTable createTable = getCatalog()
        .newTableBuilder(TableTestHelper.TEST_TABLE_ID, getCreateTableSchema())
        .withPrimaryKeySpec(BasicTableTestHelper.PRIMARY_KEY_SPEC)
        .withProperties(properties)
        .create()
        .asKeyedTable();
    validateCreateKeyedTableWithTag(createTable);

    KeyedTable loadTable = getCatalog().loadTable(TableTestHelper.TEST_TABLE_ID).asKeyedTable();
    validateCreateKeyedTableWithTag(loadTable);
  }

  private void validateCreateKeyedTableWithTag(ArcticTable table) throws TException {
    Assert.assertEquals(getCreateTableSchema().asStruct(), table.schema().asStruct());
    Assert.assertEquals(TableTestHelper.TEST_TABLE_ID, table.id());
    Assert.assertTrue(table.isKeyedTable());
    KeyedTable keyedTable = (KeyedTable)table;
    Assert.assertEquals(BasicTableTestHelper.PRIMARY_KEY_SPEC, keyedTable.primaryKeySpec());
    Assert.assertEquals(getCreateTableSchema().asStruct(), keyedTable.baseTable().schema().asStruct());
    Assert.assertEquals(getCreateTableSchema().asStruct(), keyedTable.changeTable().schema().asStruct());
    Assert.assertEquals(table.properties().get(HiveTableProperties.AUTO_SYNC_HIVE_SCHEMA_CHANGE), "false");
    Assert.assertEquals(table.properties().get(TableProperties.ENABLE_AUTO_CREATE_TAG), "true");

    String dbName = table.id().getDatabase();
    String tbl = table.id().getTableName();
    org.apache.hadoop.hive.metastore.api.Table hiveTable = TEST_HMS.getHiveClient().getTable(dbName, tbl);
    Map<String,String> tableParameter = hiveTable.getParameters();

    Assert.assertEquals(1, hiveTable.getPartitionKeys().size());
    Assert.assertEquals(HiveTableProperties.HIVE_EXTRA_PARTITION_COLUMN_DEFAULT,
        hiveTable.getPartitionKeys().get(0).getName());
    Assert.assertTrue(tableParameter.containsKey(ARCTIC_TABLE_ROOT_LOCATION));
    Assert.assertTrue(tableParameter.get(ARCTIC_TABLE_ROOT_LOCATION).endsWith(tbl));
    Assert.assertTrue(tableParameter.containsKey(ARCTIC_TABLE_FLAG));
  }
}
