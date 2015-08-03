package requestProcessor

import databaseInterfacer.VariableInterfacer

class VariableRequestProcessor extends RequestProcessor {
    VariableRequestProcessor(factory) {
        super(new VariableInterfacer(factory), ["name", "domainData", "unit"])
    }
}
