package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Class to pick which files if any to compact together. This class will do batch compaction
 * based on local optimum and if it gets stuck it will choose the smallest set of files to compact.
 */
public class LocalOptimumCompactionPolicy extends RatioBasedCompactionPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOptimumCompactionPolicy.class);
  public LocalOptimumCompactionPolicy(final Configuration conf,
    final StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  protected ArrayList<HStoreFile> applyCompactionPolicy(ArrayList<HStoreFile> candidates,
    boolean mayUseOffPeak, boolean mightBeStuck) {
    return new ArrayList<>(applyCompactionPolicy(candidates, mightBeStuck, mayUseOffPeak,
      comConf.getMinFilesToCompact(), comConf.getMaxFilesToCompact()));
  }

  @Override
  public boolean needsCompaction(Collection<HStoreFile> storeFiles, List<HStoreFile> filesCompacting) {
    ArrayList<Long> throttleValues = new ArrayList<>();
    long totalSize = getTotalStoreSize(storeFiles);
    long totalSize1 = getTotalStoreSize(filesCompacting);
    double averageSize = (totalSize - totalSize1) / (long) (storeFiles.size() - filesCompacting.size());
    int level = (int) (averageSize / storeConfigInfo.getMemStoreFlushSize());
    return totalSize > throttleValues.get(level);
  }

  public List<HStoreFile> applyCompactionPolicy(List<HStoreFile> candidates, boolean mightBeStuck,
    boolean mayUseOffPeak, int minFiles, int maxFiles) {
    return null;
  }

  /**
   * Find the total size of a list of store files.
   * @param potentialMatchFiles StoreFile list.
   * @return Sum of StoreFile.getReader().length();
   */
  private long getTotalStoreSize(Collection<HStoreFile> potentialMatchFiles) {
    return potentialMatchFiles.stream().mapToLong(sf -> sf.getReader().length()).sum();
  }


}
