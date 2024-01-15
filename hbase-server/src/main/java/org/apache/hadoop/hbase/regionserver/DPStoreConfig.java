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
  public static final String MAX_REGION_SPLIT_IMBALANCE_KEY =
    "hbase.store.stripe.region.split.max.imbalance";
  private final int dPartitionCompactMinFiles;

  public float getMaxSplitImbalance() {
    return maxRegionSplitImbalance;
  }

  private final float maxRegionSplitImbalance;
  private static final double EPSILON = 0.001;

  public DPStoreConfig(Configuration config) {
    int minFiles = config.getInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, -1);
    this.dPartitionCompactMinFiles = config.getInt(MIN_FILES_KEY, Math.max(3, minFiles));
    this.maxRegionSplitImbalance = getFloat(config, MAX_REGION_SPLIT_IMBALANCE_KEY, 1.5f, true);
  }

  public int getDPartitionCompactMinFiles() {
    return dPartitionCompactMinFiles;
  }

  private static float getFloat(Configuration config, String key, float defaultValue,
    boolean moreThanOne) {
    float value = config.getFloat(key, defaultValue);
    if (value < EPSILON) {
      LOG.warn(
        String.format("%s is set to 0 or negative; using default value of %f", key, defaultValue));
      value = defaultValue;
    } else if ((value > 1f) != moreThanOne) {
      value = 1f / value;
    }
    return value;
  }
}
