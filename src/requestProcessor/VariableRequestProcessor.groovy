package requestProcessor

import databaseInterfacer.VariableInterfacer

class VariableRequestProcessor extends RequestProcessor {
    VariableRequestProcessor(factory) {
        super(factory, new VariableInterfacer())
    }
}
