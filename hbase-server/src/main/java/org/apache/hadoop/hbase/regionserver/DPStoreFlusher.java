package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

@InterfaceAudience.Private
public class DPStoreFlusher extends StoreFlusher{
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreFlusher.class);
  private final Object flushLock = new Object();
  private final DPStoreFileManager dPFileInfor;
  private final DPAreaOfTS ats;

  public DPStoreFlusher(Configuration conf, HStore store, DPStoreFileManager dPFileInfor,
    DPAreaOfTS ats) {
    super(conf, store);
    this.dPFileInfor = dPFileInfor;
    this.ats = ats;
  }

  private DPBoundaryMultiFileWriter.WriterFactory createWriterFactory(MemStoreSnapshot snapshot,
    Consumer<Path> writerCreationTracker) {
    return new DPBoundaryMultiFileWriter.WriterFactory() {
      @Override
      public StoreFileWriter createWriter() throws IOException {
        return DPStoreFlusher.this.createWriter(snapshot, true, writerCreationTracker);
      }
    };
  }

  @Override public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
    MonitoredTask status, ThroughputController throughputController, FlushLifeCycleTracker tracker,
    Consumer<Path> writerCreationTracker) throws IOException {
    List<Path> result = new ArrayList<>();
    int cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) {
      return result;
    }

    InternalScanner scanner = createScanner(snapshot.getScanners(), tracker);
    InternalScanner scannerForCA = createScanner(snapshot.getScanners(), tracker);

    DPFlushRequest req = selectFlush(scannerForCA, store.getComparator(), this.dPFileInfor);

    boolean success = false;
    DPBoundaryMultiFileWriter mw = null;
    try {
      mw = req.createWriter(this.ats); // Writer according to the policy.
      DPBoundaryMultiFileWriter.WriterFactory factory =
        createWriterFactory(snapshot, writerCreationTracker);
      StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
      mw.init(storeScanner, factory);
      mw.initWriterForL0();

      synchronized (flushLock) {
        LOG.info("====Start Flushing KVs, Has More:[{}].", scanner.next(new ArrayList<>()));
        performFlush(scanner, mw, throughputController);
        LOG.info("====Complete Flushing KVs.");
        result = mw.commitWriters(cacheFlushSeqNum, false);
        success = true;
      }
    } finally {
      if (!success && (mw != null)) {
        for (Path leftoverFile : mw.abortWriters()) {
          try {
            store.getFileSystem().delete(leftoverFile, false);
          } catch (Exception e) {
            LOG.error("Failed to delete a file after failed flush: " + e);
          }
        }
      }
      try {
        scanner.close();
        scannerForCA.close();
      } catch (IOException ex) {
        LOG.warn("Failed to close flush scanner, ignoring", ex);
      }
    }
    return result;
  }

  public DPFlushRequest selectFlush(InternalScanner scannerForCA, CellComparator cellComparator,
    DPStoreFileManager dPFileInfo) {
    return dPFileInfo.getDPBoundaries().size() == 0
            ? new GenBoundaryAndDPFlushRequest(cellComparator, scannerForCA)
            : new BoundaryDPFlushRequest(cellComparator, dPFileInfo);
  }

  public static abstract class DPFlushRequest {
    protected final CellComparator cellComparator;

    public DPFlushRequest(CellComparator cellComparator) {
      this.cellComparator = cellComparator;
    }

    public abstract DPBoundaryMultiFileWriter createWriter(DPAreaOfTS ats) throws IOException;
  }

  public static class GenBoundaryAndDPFlushRequest extends DPFlushRequest {
    private InternalScanner scannerForCA;

    public GenBoundaryAndDPFlushRequest(CellComparator cellComparator, InternalScanner scannerForCA) {
      super(cellComparator);
      this.scannerForCA = scannerForCA;
    }

    @Override
    public DPBoundaryMultiFileWriter createWriter(DPAreaOfTS ats) throws IOException {
      List<byte[]> newDpBoundaries = doCA2GetDPBoundaries();
      LOG.info("[Gen DPBoundaries] Gen dPBoundaries:{}, size:[{}].",
              newDpBoundaries.toString(), newDpBoundaries.size());
//      this.dip.updateStateDPBoundaries(newDpBoundaries);
      return new DPBoundaryMultiFileWriter(cellComparator, newDpBoundaries,
              null, null, ats);
    }

    private List<byte[]> doCA2GetDPBoundaries() throws IOException {
      List<Cell> kvs = new ArrayList<>();
      List<byte[]> rowKeys = new ArrayList<>();

      boolean hasMore;
      int flagForDebug = 0;
      do {
        hasMore = scannerForCA.next(kvs);
        if (!kvs.isEmpty()) {
          for (Cell cell : kvs) {
            byte[] rowArray = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            if (flagForDebug < 3) {
              LOG.info("[Gen DPBoundaries], Key Example[{}]:[{}].", flagForDebug + 1, Bytes.toString(rowArray));
            }
            rowKeys.add(rowArray);
          }
          kvs.clear();
        }
        ++flagForDebug;
      } while (hasMore);

      DPClusterAnalysis dpCA = new DPClusterAnalysis();
      dpCA.loadData(rowKeys);
      dpCA.initKernels();
      dpCA.kMeans();
      dpCA.prune2GetDPBoundaries();
      return dpCA.getDpBoundaries();
    }
  }

  /**
   * Dynamic-partition flush request wrapper based on boundaries.
   */
  public static class BoundaryDPFlushRequest extends DPFlushRequest {
    private DPStoreFileManager dPFileInfo;

    /**
     * @param cellComparator used to compare cells.
     * @param dPFileInfo New files should be written with these boundaries in dPFileInfo.
     */
    public BoundaryDPFlushRequest(CellComparator cellComparator, DPStoreFileManager dPFileInfo) {
      super(cellComparator);
      this.dPFileInfo = dPFileInfo;
    }

    @Override
    public DPBoundaryMultiFileWriter createWriter(DPAreaOfTS ats) throws IOException {
//      DPClusterAnalysis.boundariesExpansion(targetBoundaries);
//      LOG.info("Expansion dpBoundaries:{}", deBug(targetBoundaries));
      List<byte[]> newDpBoundaries = doCA2UpdateDPBoundaries(ats, this.dPFileInfo.getDPBoundaries());
//      this.dip.updateStateDPBoundaries(newDpBoundaries);
      LOG.info("[Update DPBoundaries], Update DPBoundaries:{}, size:[{}].",
              serializeDPBoundaries2String(newDpBoundaries), newDpBoundaries.size());
      return new DPBoundaryMultiFileWriter(cellComparator, newDpBoundaries,
              null, null, ats);
    }

    private List<byte[]> doCA2UpdateDPBoundaries(DPAreaOfTS ats, List<byte[]> oldBoundaries) {
      List<Cell> kvs = ats.getAllCellsAndReset();
      List<byte[]> rowKeys = new ArrayList<>();

      int countForDebug = 0;
      for (Cell cell : kvs) {
        byte[] rowArray = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        if (countForDebug < 3) {
          LOG.info("[Update DPBoundaries], Key Example[{}]:[{}].", countForDebug + 1, Bytes.toString(rowArray));
        }
        rowKeys.add(rowArray);
        ++countForDebug;
      }

      DPClusterAnalysis dpCA = new DPClusterAnalysis();
      dpCA.loadData(rowKeys);
      dpCA.initKernels();
      dpCA.kMeans();
      dpCA.setOldDPBoundaries(oldBoundaries);
      dpCA.prune2GetDPBoundaries();
      return dpCA.getDpBoundaries();
    }

    public static String serializeDPBoundaries2String(List<byte[]> dPBoundaries) {
      Iterator<byte[]> it = dPBoundaries.iterator();
      if (! it.hasNext())
        return "[]";
      StringBuilder sb = new StringBuilder();
      sb.append('[');
      for (;;) {
        byte[] e = it.next();
        sb.append((e instanceof byte[]) ? new String(e) : e);
        if (! it.hasNext())
          return sb.append(']').toString();
        sb.append(',');
      }
    }
  }
}
