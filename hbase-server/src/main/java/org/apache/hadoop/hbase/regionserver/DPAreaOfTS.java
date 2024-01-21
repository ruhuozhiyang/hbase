package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
public class DPAreaOfTS {
  private static final Logger LOG = LoggerFactory.getLogger(DPAreaOfTS.class);
  private volatile MutableSegment transitStoreArea;
  private HStore store;
  private Configuration conf;
  private final ReentrantReadWriteLock atsLock = new ReentrantReadWriteLock();

  public DPAreaOfTS(HStore store, Configuration conf) {
    this.store = store;
    this.conf = conf;
    this.transitStoreArea = SegmentFactory.instance().createMutableSegment(this.conf,
      this.store.getComparator(), null);
  }

  public void add(Cell cell) {
    this.atsLock.readLock().lock();
    this.transitStoreArea.add(cell, false, null, false);
    this.atsLock.readLock().unlock();
  }

  public List<Cell> getAllCellsAndReset() {
    List<Cell> result = new ArrayList<>();
    LOG.info("Current ATS Num:[{}], and Ready to Get Write-Lock.", this.transitStoreArea.getCellsCount());
    this.atsLock.writeLock().lock();
    LOG.info("Current ATS Num:[{}], and Have Gotten Write-Lock.", this.transitStoreArea.getCellsCount());
    for (Cell cell : this.transitStoreArea.getCellSet()) {
      result.add(cell);
    }
    this.transitStoreArea = SegmentFactory.instance().createMutableSegment(this.conf,
      store.getComparator(), null);
    this.atsLock.writeLock().unlock();
    LOG.info("Current ATS Num:[{}], and Lease Write-Lock.", this.transitStoreArea.getCellsCount());
    return result;
  }
}
