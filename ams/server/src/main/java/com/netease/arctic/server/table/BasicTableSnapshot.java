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

package com.netease.arctic.server.table;

public class BasicTableSnapshot implements TableSnapshot {
  private final long snapshotId;
  
  private final String ref;

  public BasicTableSnapshot(long snapshotId) {
    this.snapshotId = snapshotId;
    this.ref = null;
  }

  public BasicTableSnapshot(long snapshotId, String ref) {
    this.snapshotId = snapshotId;
    this.ref = ref;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public String ref() {
    return null;
  }
}
