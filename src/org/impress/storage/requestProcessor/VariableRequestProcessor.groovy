package org.impress.storage.requestProcessor

import org.impress.storage.databaseInterfacer.VariableInterfacer

class VariableRequestProcessor extends RequestProcessor {
    VariableRequestProcessor(factory) {
        super(factory, new VariableInterfacer())
    }
}
