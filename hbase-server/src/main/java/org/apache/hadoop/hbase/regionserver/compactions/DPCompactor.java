package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.DPMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.List;

@InterfaceAudience.Private
public class DPCompactor extends AbstractMultiOutputCompactor<DPMultiFileWriter>{

  public DPCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override protected List<Path> commitWriter(DPMultiFileWriter writer, FileDetails fd,
    CompactionRequestImpl request) throws IOException {
    return null;
  }

  @Override protected void abortWriter(DPMultiFileWriter writer) throws IOException {

  }
}
