package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;

@InterfaceAudience.Private
public class DynamicPartitionCompactionPolicy extends CompactionPolicy {
  private final static Logger LOG = LoggerFactory.getLogger(DynamicPartitionCompactionPolicy.class);

  public DynamicPartitionCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override public boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
    throws IOException {
    return false;
  }

  @Override public boolean throttleCompaction(long compactionSize) {
    return false;
  }
}
