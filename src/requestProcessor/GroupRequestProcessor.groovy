package requestProcessor

import databaseInterfacer.GroupInterfacer

class GroupRequestProcessor extends RequestProcessor {
    GroupRequestProcessor(factory) {
        super(factory, new GroupInterfacer())
    }
}
