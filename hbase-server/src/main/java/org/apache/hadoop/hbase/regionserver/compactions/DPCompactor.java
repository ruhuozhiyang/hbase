package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.DPBoundaryMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

@InterfaceAudience.Private
public class DPCompactor extends AbstractMultiOutputCompactor<DPBoundaryMultiFileWriter>{
  private static final Logger LOG = LoggerFactory.getLogger(DPCompactor.class);

  public DPCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override protected List<Path> commitWriter(DPBoundaryMultiFileWriter writer, FileDetails fd,
    CompactionRequestImpl request) throws IOException {
    return null;
  }

  private final class DPInternalScannerFactory implements InternalScannerFactory {
    private final byte[] majorRangeFromRow;
    private final byte[] majorRangeToRow;

    public DPInternalScannerFactory(byte[] majorRangeFromRow, byte[] majorRangeToRow) {
      this.majorRangeFromRow = majorRangeFromRow;
      this.majorRangeToRow = majorRangeToRow;
    }

    @Override
    public ScanType getScanType(CompactionRequestImpl request) {
      // If majorRangeFromRow and majorRangeToRow are not null, then we will not use the return
      // value to create InternalScanner. See the createScanner method below. The return value is
      // also used when calling coprocessor hooks.
      return ScanType.COMPACT_RETAIN_DELETES;
    }

    @Override
    public InternalScanner createScanner(ScanInfo scanInfo, List<StoreFileScanner> scanners,
      ScanType scanType, FileDetails fd, long smallestReadPoint) throws IOException {
      return (majorRangeFromRow == null)
        ? DPCompactor.this.createScanner(store, scanInfo, scanners, scanType, smallestReadPoint,
        fd.earliestPutTs)
        : DPCompactor.this.createScanner(store, scanInfo, scanners, smallestReadPoint,
        fd.earliestPutTs, majorRangeFromRow, majorRangeToRow);
    }
  }

  public List<Path> compact(CompactionRequestImpl request, final List<byte[]> targetBoundaries,
    final byte[] majorRangeFromRow, final byte[] majorRangeToRow,
    ThroughputController throughputController, User user) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("Executing compaction with " + targetBoundaries.size() / 2 + " boundaries:");
    for (int j = 0; j < targetBoundaries.size(); j = j + 2) {
      byte[] left = targetBoundaries.get(j);
      byte[] right = targetBoundaries.get(j + 1);
      sb.append(" [").append(Bytes.toString(left)).append(Bytes.toString(right)).append("]");
    }
    LOG.info(sb.toString());
    return compact(request, new DPCompactor.DPInternalScannerFactory(majorRangeFromRow, majorRangeToRow),
      new CellSinkFactory<DPBoundaryMultiFileWriter>() {
        @Override
        public DPBoundaryMultiFileWriter createWriter(InternalScanner scanner, FileDetails fd,
          boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker)
          throws IOException {
          DPBoundaryMultiFileWriter writer = new DPBoundaryMultiFileWriter(store.getComparator(),
            targetBoundaries, majorRangeFromRow, majorRangeToRow);
          initMultiWriter(writer, scanner, fd, shouldDropBehind, major, writerCreationTracker);
          return writer;
        }
      }, throughputController, user);
  }
}
