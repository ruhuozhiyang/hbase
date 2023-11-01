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

  public List<HStoreFile> applyCompactionPolicy(List<HStoreFile> candidates, boolean mightBeStuck,
    boolean mayUseOffPeak, int minFiles, int maxFiles) {
    final double currentRatio =
            mayUseOffPeak ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();

    // Start off choosing nothing.
    List<HStoreFile> bestSelection = new ArrayList<>(0);
    List<HStoreFile> smallest = mightBeStuck ? new ArrayList<>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    int opts = 0, optsInRatio = 0, bestStart = -1; // for debug logging
    // Consider every starting place.
    for (int start = 0; start < candidates.size(); start++) {
      // Consider every different sub list permutation in between start and end with min files.
      for (int currentEnd = start + minFiles - 1; currentEnd < candidates.size(); currentEnd++) {
        List<HStoreFile> potentialMatchFiles = candidates.subList(start, currentEnd + 1);

        // Sanity checks
        if (potentialMatchFiles.size() < minFiles) {
          continue;
        }
        if (potentialMatchFiles.size() > maxFiles) {
          continue;
        }

        // Compute the total size of files that will
        // have to be read if this set of files is compacted.
        long size = getTotalStoreSize(potentialMatchFiles);

        // Store the smallest set of files. This stored set of files will be used
        // if it looks like the algorithm is stuck.
        if (mightBeStuck && size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }

        if (size > comConf.getMaxCompactSize(mayUseOffPeak)) {
          continue;
        }

        ++opts;
        if (
                size >= comConf.getMinCompactSize() && !filesInRatio(potentialMatchFiles, currentRatio)
        ) {
          continue;
        }

        ++optsInRatio;
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
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


}
