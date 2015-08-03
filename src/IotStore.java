import com.tinkerpop.blueprints.impls.orient.*;
import requestProcessor.*;
import utils.JsonTransformer;
import exceptions.ResponseErrorException;

import static spark.Spark.*;

public class IotStore {
    static OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/iot").setupPool(1, 10);
    static AreaRequestProcessor areaRequestProcessor;
    static VariableRequestProcessor variableRequestProcessor;
    static DeviceRequestProcessor deviceRequestProcessor;
    static GroupRequestProcessor groupRequestProcessor;

    public static void main(String[] args) {
        areaRequestProcessor = new AreaRequestProcessor(factory);
        variableRequestProcessor = new VariableRequestProcessor(factory);
        deviceRequestProcessor = new DeviceRequestProcessor(factory);
        groupRequestProcessor = new GroupRequestProcessor(factory);

        final JsonTransformer jsonTransformer = new JsonTransformer();

        get("/areas", "application/json", areaRequestProcessor::get, jsonTransformer);
        get("/areas/:id", "application/json", areaRequestProcessor::getById, jsonTransformer);
        delete("/areas/:id", "application/json", areaRequestProcessor::delete, jsonTransformer);
        post("/areas", "application/json", areaRequestProcessor::create, jsonTransformer);

        get("/variables", "application/json", variableRequestProcessor::get, jsonTransformer);
        get("/variables/:id", "application/json", variableRequestProcessor::getById, jsonTransformer);
        delete("/variables/:id", "application/json", variableRequestProcessor::delete, jsonTransformer);
        post("/variables", "application/json", variableRequestProcessor::create, jsonTransformer);

        get("/devices", "application/json", deviceRequestProcessor::get, jsonTransformer);
        get("/devices/:id", "application/json", deviceRequestProcessor::getById, jsonTransformer);
        delete("/devices/:id", "application/json", deviceRequestProcessor::delete, jsonTransformer);
        post("/devices", "application/json", deviceRequestProcessor::create, jsonTransformer);

        get("/groups", "application/json", groupRequestProcessor::get, jsonTransformer);
        get("/groups/:id", "application/json", groupRequestProcessor::getById, jsonTransformer);
        delete("/groups/:id", "application/json", groupRequestProcessor::delete, jsonTransformer);
        post("/groups", "application/json", groupRequestProcessor::create, jsonTransformer);

        exception(ResponseErrorException.class, (e, req, res) -> {
            res.type("application/json");
            res.body(e.toString());
        });
    }
}