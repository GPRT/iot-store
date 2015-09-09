package org.impress.storage.requestProcessor

import org.impress.storage.databaseInterfacer.SimulationInterfacer

class SimulationRequestProcessor extends RequestProcessor {
    SimulationRequestProcessor(factory) {
        super(factory, new SimulationInterfacer())
    }
}
