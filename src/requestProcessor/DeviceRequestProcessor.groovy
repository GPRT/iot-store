package requestProcessor

import databaseInterfacer.DeviceInterfacer

class DeviceRequestProcessor extends RequestProcessor {
    DeviceRequestProcessor(factory) {
        super(new DeviceInterfacer(factory))
    }
}
