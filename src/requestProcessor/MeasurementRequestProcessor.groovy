package requestProcessor

import databaseInterfacer.MeasurementInterfacer

class MeasurementRequestProcessor extends RequestProcessor {
    MeasurementRequestProcessor(factory) {
        super(new MeasurementInterfacer(factory))
    }
}
