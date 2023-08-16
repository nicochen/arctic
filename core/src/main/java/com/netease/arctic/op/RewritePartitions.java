package com.netease.arctic.op;


import org.apache.iceberg.DataFile;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.StatisticsFile;

import java.util.List;

public interface RewritePartitions extends PendingUpdate<List<StatisticsFile>> {

  /**
   * Add a {@link DataFile} to the table.
   *
   * @param file a data file
   * @return this for method chaining
   */
  RewritePartitions addDataFile(DataFile file);

  /**
   * Update optimized sequence for changed partitions.
   * The files of ChangeStore whose sequence is bigger than optimized sequence should migrate to BaseStore later.
   *
   * @param sequence - optimized sequence
   * @return this for method chaining
   */
  RewritePartitions updateOptimizedSequenceDynamically(long sequence);

}
