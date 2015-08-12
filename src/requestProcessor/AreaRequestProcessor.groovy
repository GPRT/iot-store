package requestProcessor

import databaseInterfacer.AreaInterfacer

class AreaRequestProcessor extends RequestProcessor {
    AreaRequestProcessor(factory) {
        super(new AreaInterfacer(factory))
    }
}
