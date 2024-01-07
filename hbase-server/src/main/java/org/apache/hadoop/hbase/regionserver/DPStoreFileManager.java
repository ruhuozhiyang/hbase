package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class DPStoreFileManager implements StoreFileManager{
  @Override public void loadFiles(List<HStoreFile> storeFiles) {

  }

  @Override public void insertNewFiles(Collection<HStoreFile> sfs) {

  }

  @Override public void addCompactionResults(Collection<HStoreFile> compactedFiles,
    Collection<HStoreFile> results) {

  }

  @Override public void removeCompactedFiles(Collection<HStoreFile> compactedFiles) {

  }

  @Override public ImmutableCollection<HStoreFile> clearFiles() {
    return null;
  }

  @Override public Collection<HStoreFile> clearCompactedFiles() {
    return null;
  }

  @Override public Collection<HStoreFile> getStorefiles() {
    return null;
  }

  @Override public Collection<HStoreFile> getCompactedfiles() {
    return null;
  }

  @Override public int getStorefileCount() {
    return 0;
  }

  @Override public int getCompactedFilesCount() {
    return 0;
  }

  @Override public Collection<HStoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow) {
    return null;
  }

  @Override public Iterator<HStoreFile> getCandidateFilesForRowKeyBefore(KeyValue targetKey) {
    return null;
  }

  @Override public Iterator<HStoreFile> updateCandidateFilesForRowKeyBefore(
    Iterator<HStoreFile> candidateFiles, KeyValue targetKey, Cell candidate) {
    return null;
  }

  @Override public Optional<byte[]> getSplitPoint() throws IOException {
    return Optional.empty();
  }

  @Override public int getStoreCompactionPriority() {
    return 0;
  }

  @Override
  public Collection<HStoreFile> getUnneededFiles(long maxTs, List<HStoreFile> filesCompacting) {
    return null;
  }

  @Override public double getCompactionPressure() {
    return 0;
  }

  @Override public Comparator<HStoreFile> getStoreFileComparator() {
    return null;
  }
}
