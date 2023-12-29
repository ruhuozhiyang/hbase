package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@InterfaceAudience.Private
public class SlidingWindowBasedCompactionPolicy extends RatioBasedCompactionPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowBasedCompactionPolicy.class);
  private static final String WINDOW_COMPACTION_POLICY_RATIO = "hbase.store.window.compaction.ratio";
  private final int ratio;
  public SlidingWindowBasedCompactionPolicy(Configuration conf,
    StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
    ratio = conf.getInt(WINDOW_COMPACTION_POLICY_RATIO, 2);
  }

  @Override protected ArrayList<HStoreFile> applyCompactionPolicy(ArrayList<HStoreFile> candidates,
    boolean mayUseOffPeak, boolean mightBeStuck) {
    return new ArrayList<>(applyCompactionPolicy(candidates, mightBeStuck, mayUseOffPeak,
      comConf.getMaxFilesToCompact(), comConf.getMinFilesToCompact()));
  }

  public List<HStoreFile> applyCompactionPolicy(List<HStoreFile> candidates, boolean mightBeStuck,
    boolean mayUseOffPeak, int maxFiles, int minFiles) {
    LOG.info("Apply Compaction Policy, MinFiles:{}, MaxFiles:{}, MightBeStuck:{}", minFiles, maxFiles, mightBeStuck);
    final double currentRatio =
      mayUseOffPeak ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();

    // Start off choosing nothing.
    List<HStoreFile> bestSelection = new ArrayList<>(0);
    List<HStoreFile> smallest = mightBeStuck ? new ArrayList<>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    int opts = 0, optsInRatio = 0, bestStart = -1;
    for (int start = 0; start < candidates.size(); start++) {
      for (int currentEnd = start + minFiles - 1; currentEnd < candidates.size(); currentEnd++) {
        List<HStoreFile> potentialMatchFiles = candidates.subList(start, currentEnd + 1);
        if (potentialMatchFiles.size() < minFiles) {
          continue;
        }
        long size = getTotalStoreSize(potentialMatchFiles);
        if (mightBeStuck && size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }
        ++opts;
        if (
          size >= comConf.getMinCompactSize() && !filesInRatio(potentialMatchFiles, currentRatio)
        ) {
          continue;
        }
        ++optsInRatio;
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck,
          maxFiles)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
          bestStart = start;
        }
      }
    }
    if (bestSelection.isEmpty() && mightBeStuck) {
      LOG.debug("Exploring compaction algorithm has selected " + smallest.size() + " files of size "
        + smallestSize + " because the store might be stuck");
      return new ArrayList<>(smallest);
    }
    LOG.debug(
      "Exploring compaction algorithm has selected {}  files of size {} starting at "
        + "candidate #{} after considering {} permutations with {} in ratio",
      bestSelection.size(), bestSize, bestStart, opts, optsInRatio);
    return new ArrayList<>(bestSelection);
  }

  /**
   * Find the total size of a list of store files.
   * @param potentialMatchFiles StoreFile list.
   * @return Sum of StoreFile.getReader().length();
   */
  private long getTotalStoreSize(Collection<HStoreFile> potentialMatchFiles) {
    return potentialMatchFiles.stream().mapToLong(sf -> sf.getReader().length()).sum();
  }

  private boolean isBetterSelection(List<HStoreFile> bestSelection, long bestSize,
    List<HStoreFile> selection, long size, boolean mightBeStuck, int maxFiles) {
    LOG.info("====Whether in Stuck:{}", mightBeStuck);
    if (mightBeStuck && bestSize > 0 && size > 0) {
      final double REPLACE_IF_BETTER_BY = 1.05;
      double thresholdQuality = ((double) bestSelection.size() / bestSize) * REPLACE_IF_BETTER_BY;
      return thresholdQuality < ((double) selection.size() / size);
    }
    int filesNum = selection.size();
    long memStoreFlushSize = storeConfigInfo.getMemStoreFlushSize();
//    long middleFileSize = selection.get(filesNum / 2).getReader().length();
    long meanFileSize = size / filesNum;
    int level = (int) Math.pow((int) meanFileSize / memStoreFlushSize, 1.0 / maxFiles);
    long throttle = (long) (memStoreFlushSize * (Math.pow(maxFiles, level == 0 ? level + 1 : level) / ratio));
    LOG.info("====TotalSize:{}, FileNum:{}, MeanFileSize:{}, Level:{}, Throttle:{}",
      size, filesNum, meanFileSize, level, throttle);
    LOG.info("====Whether Better Selection:{}", size > throttle && size > bestSize);
    return size > throttle && size > bestSize;
  }

  /**
   * Check that all files satisfy the constraint
   *
   * <pre>
   * FileSize(i) <= ( Sum(0,N,FileSize(_)) - FileSize(i)) * Ratio.
   * </pre>
   *
   * @param files        List of store files to consider as a compaction candidate.
   * @param currentRatio The ratio to use.
   * @return a boolean if these files satisfy the ratio constraints.
   */
  private boolean filesInRatio(List<HStoreFile> files, double currentRatio) {
    if (files.size() < 2) {
      return true;
    }

    long totalFileSize = getTotalStoreSize(files);

    for (HStoreFile file : files) {
      long singleFileSize = file.getReader().length();
      long sumAllOtherFileSizes = totalFileSize - singleFileSize;

      if (singleFileSize > sumAllOtherFileSizes * currentRatio) {
        return false;
      }
    }
    return true;
  }
}
