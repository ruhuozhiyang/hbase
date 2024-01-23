## Notes about Hbase Branch-2.5
DEFAULT_COMPACTION_POLICY_CLASS = ExploringCompactionPolicy.class

CompactionPolicy
->
SortedCompactionPolicy:An abstract compaction policy that select files on seq id order.
```java
class SortedCompactionPolicy {
  public CompactionRequestImpl selectCompaction(Collection<HStoreFile> candidateFiles, 
                   List<HStoreFile> filesCompacting, 
                   boolean isUserCompaction, 
                   boolean mayUseOffPeak, 
                   boolean forceMajor) {
      
  }
  protected ArrayList<HStoreFile> getCurrentEligibleFiles(ArrayList<HStoreFile> candidateFiles,
                                                          final List<HStoreFile> filesCompacting) {
    // candidates = all storefiles not already in compaction queue
    if (!filesCompacting.isEmpty()) {
      // exclude all files older than the newest file we're currently
      // compacting. this allows us to preserve contiguity (HBASE-2856)
      HStoreFile last = filesCompacting.get(filesCompacting.size() - 1);
      int idx = candidateFiles.indexOf(last);
      Preconditions.checkArgument(idx != -1);
      candidateFiles.subList(0, idx + 1).clear();
    }
    return candidateFiles;
  }
}
```
->
RatioBasedCompactionPolicy
->
ExploringCompactionPolicy


候选文件是根据 sequence id 从旧到新排序的，顺序是由 DefaultStoreFileManager 管理保持的。
基于 sequence id 保证连续合并，为了数据一致性。

整个候选文件队列文件的大小，不是某个文件的大小
maxCompactSize = conf.getLong(HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, Long.MAX_VALUE);
整个候选文件队列文件的大小，不是某个文件的大小
minCompactSize = conf.getLong(HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, storeConfigInfo.getMemStoreFlushSize());
整个候选文件队列文件的数量
minFilesToCompact = Math.max(2, conf.getInt(HBASE_HSTORE_COMPACTION_MIN_KEY, conf.getInt("hbase.hstore.compactionThreshold", 3)));
整个候选文件队列文件的数量
maxFilesToCompact = conf.getInt(HBASE_HSTORE_COMPACTION_MAX_KEY, 10);
