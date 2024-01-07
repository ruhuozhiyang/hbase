package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactionPolicy;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.*;

public class DPStoreFileManager implements StoreFileManager, DPCompactionPolicy.PartitionInformationProvider {
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

  @Override
  public byte[] getStartRow(int partitionIndex) {
    return new byte[0];
  }

  @Override
  public byte[] getEndRow(int partitionIndex) {
    return new byte[0];
  }

  @Override
  public List<HStoreFile> getLevel0Files() {
    return null;
  }

  @Override
  public List<byte[]> getPartitionBoundaries() {
    return null;
  }

  @Override
  public ArrayList<ImmutableList<HStoreFile>> getPartitions() {
    return null;
  }

  @Override
  public int getPartitionCount() {
    return 0;
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
