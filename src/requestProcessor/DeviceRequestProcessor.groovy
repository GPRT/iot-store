package requestProcessor

import databaseInterfacer.DeviceInterfacer

class DeviceRequestProcessor extends RequestProcessor {
    DeviceRequestProcessor(factory) {
        super(factory, new DeviceInterfacer())
    }
}
