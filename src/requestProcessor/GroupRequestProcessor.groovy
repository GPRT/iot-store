package requestProcessor

import databaseInterfacer.GroupInterfacer

class GroupRequestProcessor extends RequestProcessor {
    GroupRequestProcessor(factory) {
        super(new GroupInterfacer(factory), ["name", "domainData", "devices"])
    }
}
