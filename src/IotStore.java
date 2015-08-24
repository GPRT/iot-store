import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import requestProcessor.*;
import utils.Endpoints;
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

        Endpoints.setMainUrl("http://192.168.0.71:4567");
        Endpoints.addClass("area", "areas");
        Endpoints.addClass("measurementvariable", "variables");
        Endpoints.addClass("resource", "devices");
        Endpoints.addClass("group", "groups");
        Endpoints.addClass("simulation", "simulations");

        String areasPath = Endpoints.getPath("area");
        get(areasPath, "application / json", areaRequestProcessor::get, jsonTransformer);
        get(areasPath + "/:id", "application/json", areaRequestProcessor::getById, jsonTransformer);
        put(areasPath + "/:id", "application/json", areaRequestProcessor::setById, jsonTransformer);
        delete(areasPath + "/:id", "application/json", areaRequestProcessor::delete, jsonTransformer);
        post(areasPath, "application/json", areaRequestProcessor::create, jsonTransformer);

        String variablesPath = Endpoints.getPath("measurementvariable");
        get(variablesPath, "application/json", variableRequestProcessor::get, jsonTransformer);
        get(variablesPath + "/:id", "application/json", variableRequestProcessor::getById, jsonTransformer);
        put(variablesPath + "/:id", "application/json", variableRequestProcessor::setById, jsonTransformer);
        delete(variablesPath + "/:id", "application/json", variableRequestProcessor::delete, jsonTransformer);
        post(variablesPath, "application/json", variableRequestProcessor::create, jsonTransformer);

        String devicesPath = Endpoints.getPath("resource");
        get(devicesPath, "application/json", deviceRequestProcessor::get, jsonTransformer);
        get(devicesPath + "/:id", "application/json", deviceRequestProcessor::getById, jsonTransformer);
        put(devicesPath + "/:id", "application/json", deviceRequestProcessor::setById, jsonTransformer);
        delete(devicesPath + "/:id", "application/json", deviceRequestProcessor::delete, jsonTransformer);
        post(devicesPath, "application/json", deviceRequestProcessor::create, jsonTransformer);

        String groupsPath = Endpoints.getPath("group");
        get(groupsPath, "application/json", groupRequestProcessor::get, jsonTransformer);
        get(groupsPath + "/:id", "application/json", groupRequestProcessor::getById, jsonTransformer);
        put(groupsPath + "/:id", "application/json", groupRequestProcessor::setById, jsonTransformer);
        delete(groupsPath + "/:id", "application/json", groupRequestProcessor::delete, jsonTransformer);
        post(groupsPath, "application/json", groupRequestProcessor::create, jsonTransformer);

        String simulationsPath = Endpoints.getPath("simulation");
        get(simulationsPath, "application/json", simulationRequestProcessor::get, jsonTransformer);
        get(simulationsPath + "/:id", "application/json", simulationRequestProcessor::getById, jsonTransformer);
        put(simulationsPath + "/:id", "application/json", simulationRequestProcessor::setById, jsonTransformer);
        delete(simulationsPath + "/:id", "application/json", simulationRequestProcessor::delete, jsonTransformer);
        post(simulationsPath, "application/json", simulationRequestProcessor::create, jsonTransformer);

        get(devicesPath + "/:id/measurements", "application/json", measurementRequestProcessor::get, jsonTransformer);
        post(devicesPath + "/:id/measurements", "application/json", measurementRequestProcessor::create, jsonTransformer);

        exception(ResponseErrorException.class, (e, req, res) -> {
            ResponseErrorException error = (ResponseErrorException)e;
            res.type("application/json");
            res.status(error.statusCode());
            res.body(error.toString());
        });
    }
}