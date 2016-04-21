package org.impress.storage.scripts

import com.orientechnologies.orient.client.remote.OServerAdmin
import groovy.util.logging.Slf4j

@Slf4j
class DropDB {
    static run() {
        log.info("Dropping DB: iot.")
        def admin = new OServerAdmin('remote:localhost/iot');
        admin.connect("root", "root");

        admin.dropDatabase("iot");
        log.info("Database dropped.")
    }
}

DropDB.run()