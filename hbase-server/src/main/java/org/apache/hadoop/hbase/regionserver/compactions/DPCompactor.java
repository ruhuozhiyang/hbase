package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@InterfaceAudience.Private
public class DPCompactor extends AbstractMultiOutputCompactor<DPBoundaryMultiFileWriter>{
  private static final Logger LOG = LoggerFactory.getLogger(DPCompactor.class);

  public DPCompactor(Configuration conf, HStore store) {
    super(conf, store);
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
          boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker) throws IOException {
          DPBoundaryMultiFileWriter writer = new DPBoundaryMultiFileWriter(store.getComparator(),
            doCA2GetDPBoundaries(targetBoundaries, scanner), majorRangeFromRow, majorRangeToRow);
          initMultiWriter(writer, scanner, fd, shouldDropBehind, major, writerCreationTracker);
          return writer;
        }
      }, throughputController, user);
  }

  private List<byte[]> doCA2GetDPBoundaries(List<byte[]> oldBoundaries, InternalScanner scanner) throws IOException {
    List<Cell> kvs = new ArrayList<>();
    List<byte[]> rowKeys = new ArrayList<>();

    boolean hasMore;
    int flagForDebug = 0;
    do {
      hasMore = scanner.next(kvs);
      if (!kvs.isEmpty()) {
        for (Cell cell : kvs) {
          byte[] rowArray = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          if (flagForDebug < 3) {
            LOG.info("In Compaction, Key String:{}", Bytes.toString(rowArray));
          }
          rowKeys.add(rowArray);
        }
        kvs.clear();
      }
      ++flagForDebug;
    } while (hasMore);

    DPClusterAnalysis dpCA = new DPClusterAnalysis();
    dpCA.loadData(rowKeys);
    dpCA.setOldDPBoundaries(oldBoundaries);
    dpCA.setKernels();
    dpCA.kMeans();
    return dpCA.getDpBoundaries();
  }

  @Override protected List<Path> commitWriter(DPBoundaryMultiFileWriter writer, FileDetails fd,
    CompactionRequestImpl request) throws IOException {
    List<Path> newFiles = writer.commitWriters(fd.maxSeqId, request.isMajor(), request.getFiles());
    assert !newFiles.isEmpty() : "Should have produced an empty file to preserve metadata.";
    return newFiles;
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
}
