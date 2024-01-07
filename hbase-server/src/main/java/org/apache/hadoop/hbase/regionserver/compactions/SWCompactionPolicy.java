package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

@InterfaceAudience.Private
public class SWCompactionPolicy extends RatioBasedCompactionPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SWCompactionPolicy.class);
  private static final String WINDOW_COMPACTION_POLICY_RATIO = "hbase.store.window.compaction.ratio";
  private static final String WINDOW_COMPACTION_POLICY_THROTTLE = "hbase.store.window.compaction.throttle";
  private static final String COMPACTION_STATUS_RECORD_PATH = "hbase.store.window.compaction.record.path";
  private static final String HREGION_MEMSTORE_BLOCK_MULTIPLIER = "hbase.hregion.memstore.block.multiplier";
  private final int ratio;
  private final int throttle;
  private final File compactionRecordFile;
  private final int multi;
  public SWCompactionPolicy(Configuration conf,
    StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
    ratio = conf.getInt(WINDOW_COMPACTION_POLICY_RATIO, 2);
    throttle = conf.getInt(WINDOW_COMPACTION_POLICY_THROTTLE, 0);
    compactionRecordFile = new File(conf.get(COMPACTION_STATUS_RECORD_PATH,
      "/tmp/better-compaction-record"));
    if (!compactionRecordFile.exists()) {
      try {
        compactionRecordFile.createNewFile();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    multi = conf.getInt(HREGION_MEMSTORE_BLOCK_MULTIPLIER, 4);
  }

  @Override protected ArrayList<HStoreFile> applyCompactionPolicy(ArrayList<HStoreFile> candidates,
    boolean mayUseOffPeak, boolean mightBeStuck) {
    return new ArrayList<>(applyCompactionPolicy(candidates, mightBeStuck, mayUseOffPeak,
      comConf.getMinFilesToCompact()));
  }

  public List<HStoreFile> applyCompactionPolicy(List<HStoreFile> candidates, boolean mightBeStuck,
    boolean mayUseOffPeak, int minFiles) {
    // Start off choosing nothing.
    List<HStoreFile> bestSelection = new ArrayList<>(0);
    List<HStoreFile> smallest = mightBeStuck ? new ArrayList<>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    int bestStart = -1;
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
        if (size > comConf.getMaxCompactSize(mayUseOffPeak)) {
          continue;
        }
        if (!whetherPass0(size, potentialMatchFiles)) {
          continue;
        }
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
          bestStart = start;
        }
      }
    }
    if (bestSelection.isEmpty() && mightBeStuck) {
      LOG.info("Exploring compaction algorithm has selected " + smallest.size() + " files of size "
        + smallestSize + " because the store might be stuck");
      compactionRecord("small", smallest);
      return new ArrayList<>(smallest);
    }
    LOG.info(
      "Exploring compaction algorithm has selected {}  files of size {} starting at candidate #{} ",
      bestSelection.size(), bestSize, bestStart);
    compactionRecord("best", bestSelection);
    return new ArrayList<>(bestSelection);
  }

  private boolean whetherPass(List<HStoreFile> potentialMatchFiles) {
    int f = 0;
    long memStoreFlushSize = storeConfigInfo.getMemStoreFlushSize();
    long middleSize = getHStoreFilesMiddleSize(potentialMatchFiles);
    for (int i = 0; i < potentialMatchFiles.size(); i++) {
      if (f > throttle) {
        break;
      }
      long gapSize = potentialMatchFiles.get(i).getReader().length() - middleSize;
      if (gapSize > ratio * memStoreFlushSize) {
        f++;
        LOG.info("Num:{}, GapSize:{}", f, gapSize);
      }
    }
    if (f > throttle) {
      LOG.info("PotentialMatchFiles Num:{}", potentialMatchFiles.size());
      LOG.info("MiddleFileSize:{}", middleSize);
      return false;
    }
    return true;
  }

  private boolean whetherPass0(long totalSize, List<HStoreFile> potentialMatchFiles) {
    long memStoreFlushSize = storeConfigInfo.getMemStoreFlushSize();
    long meanSize = totalSize / potentialMatchFiles.size();
    if (meanSize > comConf.getMaxFilesToCompact() * memStoreFlushSize) {
      return false;
    }
    long rangeSize = getHStoreFilesRangeSize(potentialMatchFiles);
    return rangeSize < multi * memStoreFlushSize;
  }

  private long getHStoreFilesRangeSize(List<HStoreFile> potentialMatchFiles) {
    long[] fileSizeList = new long[potentialMatchFiles.size()];
    for (int i = 0; i < potentialMatchFiles.size(); i++) {
      fileSizeList[i] = potentialMatchFiles.get(i).getReader().length();
    }
    Arrays.sort(fileSizeList);
    return fileSizeList[potentialMatchFiles.size() - 1] - fileSizeList[0];
  }

  private long getHStoreFilesMiddleSize(List<HStoreFile> potentialMatchFiles) {
    long[] fileSizeList = new long[potentialMatchFiles.size()];
    long middleSize;
    for (int i = 0; i < potentialMatchFiles.size(); i++) {
      fileSizeList[i] = potentialMatchFiles.get(i).getReader().length();
    }
    Arrays.sort(fileSizeList);
    int startLen;
    int endLen;
    if (fileSizeList.length % 2 == 0) {
      endLen = fileSizeList.length / 2;
      startLen = endLen - 1;
      middleSize = (fileSizeList[startLen] + fileSizeList[endLen]) / 2;
    } else {
      startLen = fileSizeList.length / 2;
      middleSize = fileSizeList[startLen];
    }
    return middleSize;
  }

  private void compactionRecord(String flag, List<HStoreFile> bestSelection) {
    try {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
      BufferedWriter writer = new BufferedWriter(new FileWriter(compactionRecordFile, true));
      StringBuilder line = new StringBuilder();
      line.append(formatter.format(new Date()) + flag);
      for (int i = 0; i < bestSelection.size(); i++) {
        line.append(" ");
        line.append(bestSelection.get(i).getReader().length());
      }
      line.append("\n");
      writer.write(line.toString());
      writer.flush();
      writer.close();
    } catch (IOException e) {
      LOG.warn("Failed to record the compaction info for [{}].", e.getMessage(), e);
    }
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
    List<HStoreFile> selection, long size, boolean mightBeStuck) {
    if (mightBeStuck && bestSize > 0 && size > 0) {
      final double REPLACE_IF_BETTER_BY = 1.05;
      double thresholdQuality = ((double) bestSelection.size() / bestSize) * REPLACE_IF_BETTER_BY;
      return thresholdQuality < ((double) selection.size() / size);
    }
    // Keep if this gets rid of more files. Or the same number of files for less io.
    return selection.size() > bestSelection.size()
      || (selection.size() == bestSelection.size() && size < bestSize);

//    if (selection.size() > bestSelection.size()) {
//      long memStoreFlushSize = storeConfigInfo.getMemStoreFlushSize();
//      //    long middleFileSize = selection.get(filesNum / 2).getReader().length();
//      long meanFileSize = size / selection.size();
//      int level = (int) Math.pow((int) meanFileSize / memStoreFlushSize, 1.0 / maxFiles);
//      long throttle = (long) (memStoreFlushSize * (Math.pow(maxFiles, level) / ratio));
//      LOG.info("====MightBeStuck:{}, TotalSize:{}, FileNum:{}, MeanFileSize:{}, "
//          + "Level:{}, Throttle:{}, Whether Better Selection:{}",
//        mightBeStuck, size, filesNum, meanFileSize, level, throttle,
//        size > throttle && size > bestSize);
//      return size > throttle && size > bestSize;
//      long memStoreFlushSize = storeConfigInfo.getMemStoreFlushSize();
//      double meanFileSize = size / selection.size();
//      double p = size / meanFileSize;
//      if (selection.size() > maxFiles * memStoreFlushSize) {
//        return true;
//      }
//    }
//    return selection.size() > bestSelection.size() && size > maxFiles * memStoreFlushSize;
  }
}
