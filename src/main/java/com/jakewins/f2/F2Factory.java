package com.jakewins.f2;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.ResourceType;

import java.time.Clock;

import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * This makes F2 available to Neo4j, assuming neo can find it on the classpath.
 */
public class F2Factory extends Locks.Factory {

    public static final Setting<Integer> numPartitions = setting( "unsupported.dbms.f2.partitions", Settings.INTEGER, "128" );

    public F2Factory() {
        super("f2");
    }

    @Override
    public Locks newInstance(Config config, Clock clocks, ResourceType[] resourceTypes) {
        return new F2Locks(resourceTypes, config.get(numPartitions));
    }
}
