package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class LocalOptimumStoreConfig {
    private static final Logger LOG = LoggerFactory.getLogger(LocalOptimumStoreConfig.class);
    public static final String MAX_FILES_KEY = "hbase.store.localOptimum.compaction.throttle";

    public LocalOptimumStoreConfig(Configuration config, StoreConfigInformation sci) {

    }
}
