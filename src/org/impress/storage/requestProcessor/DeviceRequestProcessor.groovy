package org.impress.storage.requestProcessor

import org.impress.storage.databaseInterfacer.DeviceInterfacer

class DeviceRequestProcessor extends RequestProcessor {
    DeviceRequestProcessor(factory) {
        super(factory, new DeviceInterfacer())
    }
}
