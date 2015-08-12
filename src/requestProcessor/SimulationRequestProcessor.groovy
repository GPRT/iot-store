package requestProcessor

import databaseInterfacer.SimulationInterfacer

class SimulationRequestProcessor extends RequestProcessor {
    SimulationRequestProcessor(factory) {
        super(new SimulationInterfacer(factory))
    }
}
