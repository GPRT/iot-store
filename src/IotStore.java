import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import requestProcessor.*;
import utils.JsonTransformer;
import exceptions.ResponseErrorException;

import static spark.Spark.*;

public class IotStore {
    static OPartitionedDatabasePoolFactory factory = new OPartitionedDatabasePoolFactory();

    static AreaRequestProcessor areaRequestProcessor;
    static VariableRequestProcessor variableRequestProcessor;
    static DeviceRequestProcessor deviceRequestProcessor;
    static GroupRequestProcessor groupRequestProcessor;
    static SimulationRequestProcessor simulationRequestProcessor;
    static MeasurementRequestProcessor measurementRequestProcessor;

    public static void main(String[] args) {
        areaRequestProcessor = new AreaRequestProcessor(factory);
        variableRequestProcessor = new VariableRequestProcessor(factory);
        deviceRequestProcessor = new DeviceRequestProcessor(factory);
        groupRequestProcessor = new GroupRequestProcessor(factory);
        simulationRequestProcessor = new SimulationRequestProcessor(factory);
        measurementRequestProcessor = new MeasurementRequestProcessor(factory);

        final JsonTransformer jsonTransformer = new JsonTransformer();

        get("/areas", "application/json", areaRequestProcessor::get, jsonTransformer);
        get("/areas/:id", "application/json", areaRequestProcessor::getById, jsonTransformer);
        put("/areas/:id", "application/json", areaRequestProcessor::setById, jsonTransformer);
        delete("/areas/:id", "application/json", areaRequestProcessor::delete, jsonTransformer);
        post("/areas", "application/json", areaRequestProcessor::create, jsonTransformer);

        get("/variables", "application/json", variableRequestProcessor::get, jsonTransformer);
        get("/variables/:id", "application/json", variableRequestProcessor::getById, jsonTransformer);
        put("/variables/:id", "application/json", variableRequestProcessor::setById, jsonTransformer);
        delete("/variables/:id", "application/json", variableRequestProcessor::delete, jsonTransformer);
        post("/variables", "application/json", variableRequestProcessor::create, jsonTransformer);

        get("/devices", "application/json", deviceRequestProcessor::get, jsonTransformer);
        get("/devices/:id", "application/json", deviceRequestProcessor::getById, jsonTransformer);
        put("/devices/:id", "application/json", deviceRequestProcessor::setById, jsonTransformer);
        delete("/devices/:id", "application/json", deviceRequestProcessor::delete, jsonTransformer);
        post("/devices", "application/json", deviceRequestProcessor::create, jsonTransformer);

        get("/groups", "application/json", groupRequestProcessor::get, jsonTransformer);
        get("/groups/:id", "application/json", groupRequestProcessor::getById, jsonTransformer);
        put("/groups/:id", "application/json", groupRequestProcessor::setById, jsonTransformer);
        delete("/groups/:id", "application/json", groupRequestProcessor::delete, jsonTransformer);
        post("/groups", "application/json", groupRequestProcessor::create, jsonTransformer);

        get("/simulations", "application/json", simulationRequestProcessor::get, jsonTransformer);
        get("/simulations/:id", "application/json", simulationRequestProcessor::getById, jsonTransformer);
        put("/simulations/:id", "application/json", simulationRequestProcessor::setById, jsonTransformer);
        delete("/simulations/:id", "application/json", simulationRequestProcessor::delete, jsonTransformer);
        post("/simulations", "application/json", simulationRequestProcessor::create, jsonTransformer);

        get("/devices/:id/measurements", "application/json", measurementRequestProcessor::get, jsonTransformer);
        post("/devices/:id/measurements", "application/json", measurementRequestProcessor::create, jsonTransformer);

        exception(ResponseErrorException.class, (e, req, res) -> {
            ResponseErrorException error = (ResponseErrorException)e;
            res.type("application/json");
            res.status(error.statusCode());
            res.body(error.toString());
        });
    }
}