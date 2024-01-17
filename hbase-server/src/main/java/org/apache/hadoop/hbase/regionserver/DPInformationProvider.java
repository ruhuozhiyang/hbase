package org.apache.hadoop.hbase.regionserver;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** The information about partitions that the policy needs to do its stuff */
@InterfaceAudience.Private
public interface DPInformationProvider {
  Collection<HStoreFile> getStorefiles();

  /**
   * Gets the start row for a given dynamic partition.
   * @param dpIndex dp index.
   * @return Start row. May be an open key.
   */
  byte[] getStartRow(int dpIndex);

  /**
   * Gets the end row for a given dynamic partition.
   * @param dpIndex dp index.
   * @return End row. May be an open key.
   */
  byte[] getEndRow(int dpIndex);

  /** Returns Level 0 files. */
  List<HStoreFile> getLevel0Files();

  /** Returns All dp boundaries; including the open ones on both ends. */
  List<byte[]> getDPBoundaries();

  /** Returns The dynamic partitions. */
  ArrayList<ImmutableList<HStoreFile>> getDPs();

  /** Returns dynamic partition count. */
  int getDPCount();
}
