package org.impress.storage.requestProcessor

import org.impress.storage.databaseInterfacer.GroupInterfacer

class GroupRequestProcessor extends RequestProcessor {
    GroupRequestProcessor(factory) {
        super(factory, new GroupInterfacer())
    }
}
