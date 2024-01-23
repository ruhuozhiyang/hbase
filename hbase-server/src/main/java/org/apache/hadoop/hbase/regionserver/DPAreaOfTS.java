package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
public class DPAreaOfTS {
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

  public int getCellCount() {
    this.atsLock.readLock().lock();
    int cellCount = this.transitStoreArea.getCellsCount();
    this.atsLock.readLock().unlock();
    return cellCount;
  }

  public void add(Cell cell) {
    this.atsLock.readLock().lock();
    this.transitStoreArea.add(cell, false, null, false);
    this.atsLock.readLock().unlock();
  }

  public List<Cell> getAllCellsAndSnapShotAndReset() {
    List<Cell> result = new ArrayList<>();
    this.atsLock.writeLock().lock();
    for (Cell cell : this.transitStoreArea.getCellSet()) {
      result.add(cell);
    }
    this.transitStoreArea = SegmentFactory.instance().createMutableSegment(this.conf,
      store.getComparator(), null);
    this.atsLock.writeLock().unlock();
    return result;
  }
}
