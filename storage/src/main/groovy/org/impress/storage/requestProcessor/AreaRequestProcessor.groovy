package org.impress.storage.requestProcessor

import org.impress.storage.databaseInterfacer.AreaInterfacer

class AreaRequestProcessor extends RequestProcessor {
    AreaRequestProcessor(factory) {
        super(factory, new AreaInterfacer())
    }
}
