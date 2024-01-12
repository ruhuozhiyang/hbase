package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DPStoreConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreConfig.class);

  /**
   * The minimum number of files to compact within a dPartition;
   * same as for regular compaction.
   */
  public static final String MIN_FILES_KEY = "hbase.store.dPartition.compaction.minFiles";
  private final int dPartitionCompactMinFiles;

  public DPStoreConfig(Configuration config) {
    int minFiles = config.getInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, -1);
    this.dPartitionCompactMinFiles = config.getInt(MIN_FILES_KEY, Math.max(3, minFiles));
  }

  public int getDPartitionCompactMinFiles() {
    return dPartitionCompactMinFiles;
  }
}
