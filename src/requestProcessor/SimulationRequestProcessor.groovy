package requestProcessor

import databaseInterfacer.SimulationInterfacer

class SimulationRequestProcessor extends RequestProcessor {
    SimulationRequestProcessor(factory) {
        super(factory, new SimulationInterfacer())
    }
}
