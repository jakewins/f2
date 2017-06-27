package com.jakewins.f2;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;

public class F2Factory_Test {

    @Rule
    public TemporaryFolder workDir = new TemporaryFolder();

    @Test
    public void neo4jShouldLoadF2() throws IOException {
        // When I launch Neo4j with lock_manager set to f2
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(workDir.newFolder())
                .setConfig(GraphDatabaseFacadeFactory.Configuration.lock_manager, "f2")
                .newGraphDatabase();

        try {
            // Then it should be using F2
            Locks lockManager = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Locks.class);
            assert lockManager instanceof F2Locks : String.format("Expected F2 to be in use, found %s", lockManager.getClass().getCanonicalName());

            // And it should pass a basic smoke test..
            try(Transaction tx = db.beginTx()) {
                Node node = db.createNode();
                tx.acquireReadLock(node);
                tx.success();
            }
        } finally {
            db.shutdown();
        }
    }
}
