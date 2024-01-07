package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@InterfaceAudience.Private
public class DPCompactionPolicy extends CompactionPolicy {
  private final static Logger LOG = LoggerFactory.getLogger(DPCompactionPolicy.class);

  public DPCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  public PartitionCompactionRequest selectCompaction(PartitionInformationProvider pi,
    List<HStoreFile> filesCompacting, boolean isOffpeak) throws IOException {
    return null;
  }

  public boolean needsCompactions(PartitionInformationProvider pi, List<HStoreFile> filesCompacting) {
    // Approximation on whether we need compaction.
    return filesCompacting.isEmpty() && (StoreUtils.hasReferences(pi.getStorefiles()));
  }

  @Override public boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
    throws IOException {
    return false;
  }

  @Override public boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  public abstract static class PartitionCompactionRequest {

  }

  /** The information about partitions that the policy needs to do its stuff */
  public static interface PartitionInformationProvider {
    Collection<HStoreFile> getStorefiles();

    /**
     * Gets the start row for a given partition.
     * @param partitionIndex partition index.
     * @return Start row. May be an open key.
     */
    byte[] getStartRow(int partitionIndex);

    /**
     * Gets the end row for a given partition.
     * @param partitionIndex partition index.
     * @return End row. May be an open key.
     */
    byte[] getEndRow(int partitionIndex);

    /** Returns Level 0 files. */
    List<HStoreFile> getLevel0Files();

    /** Returns All partition boundaries; including the open ones on both ends. */
    List<byte[]> getPartitionBoundaries();

    /** Returns The partitions. */
    ArrayList<ImmutableList<HStoreFile>> getPartitions();

    /** Returns partition count. */
    int getPartitionCount();
  }
}
