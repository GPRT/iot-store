package org.impress.storage;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import org.impress.storage.requestProcessor.*;
import org.impress.storage.utils.Endpoints;
import org.impress.storage.utils.JsonTransformer;
import org.impress.storage.exceptions.ResponseErrorException;

import java.net.MalformedURLException;
import java.net.URL;

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

        String iotStoreUrl = System.getProperty("iotStore.url");

        if (iotStoreUrl == null)
            throw new RuntimeException("iotStore.url was not found in your JVM properties");

        try {
            URL mainUrl = new URL(iotStoreUrl);
            Endpoints.setMainUrl(mainUrl.toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException("[" + iotStoreUrl + "] is not a valid URL for your iotStore.url property");
        }

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
        get(areasPath + "/:id/variables/:variableId/measurements", "application/json", measurementRequestProcessor::getFromArea, jsonTransformer);
        get(areasPath + "/:id/variables", "application/json", measurementRequestProcessor::getVariablesFromArea, jsonTransformer);

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
        get(devicesPath + "/:id/variables", "application/json", measurementRequestProcessor::getVariables, jsonTransformer);
        get(devicesPath + "/:id/variables/:variableId", "application/json", measurementRequestProcessor::getVariables, jsonTransformer);
        get(devicesPath + "/:id/variables/:variableId/measurements", "application/json", measurementRequestProcessor::get, jsonTransformer);
        post(devicesPath + "/:id/measurements", "application/json", measurementRequestProcessor::create, jsonTransformer);

        String groupsPath = Endpoints.getPath("group");
        get(groupsPath, "application/json", groupRequestProcessor::get, jsonTransformer);
        get(groupsPath + "/:id", "application/json", groupRequestProcessor::getById, jsonTransformer);
        put(groupsPath + "/:id", "application/json", groupRequestProcessor::setById, jsonTransformer);
        delete(groupsPath + "/:id", "application/json", groupRequestProcessor::delete, jsonTransformer);
        post(groupsPath, "application/json", groupRequestProcessor::create, jsonTransformer);
        get(groupsPath + "/:id/variables/:variableId/measurements", "application/json", measurementRequestProcessor::getFromGroup, jsonTransformer);
        get(groupsPath + "/:id/variables", "application/json", measurementRequestProcessor::getVariablesFromGroup, jsonTransformer);

        String simulationsPath = Endpoints.getPath("simulation");
        get(simulationsPath, "application/json", simulationRequestProcessor::get, jsonTransformer);
        get(simulationsPath + "/:id", "application/json", simulationRequestProcessor::getById, jsonTransformer);
        put(simulationsPath + "/:id", "application/json", simulationRequestProcessor::setById, jsonTransformer);
        delete(simulationsPath + "/:id", "application/json", simulationRequestProcessor::delete, jsonTransformer);
        post(simulationsPath, "application/json", simulationRequestProcessor::create, jsonTransformer);


        exception(ResponseErrorException.class, (e, req, res) -> {
            ResponseErrorException error = (ResponseErrorException)e;
            res.type("application/json");
            res.status(error.statusCode());
            res.body(error.toString());
        });
    }
}
