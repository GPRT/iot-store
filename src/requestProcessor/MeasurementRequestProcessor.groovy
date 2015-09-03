package requestProcessor

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import spark.Request
import spark.Response

import databaseInterfacer.MeasurementInterfacer
import utils.InputValidator

class MeasurementRequestProcessor extends RequestProcessor {

    MeasurementRequestProcessor(factory) {
        super(factory, new MeasurementInterfacer())
    }

    private HashMap initializeGet(Request req, Response res) {
        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["beginTimestamp", "endTimestamp",
                                          "granularity","page","pageLimit"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))

        def beginTimestampParam = req.queryParams("beginTimestamp")
        def endTimestampParam = req.queryParams("endTimestamp")
        def granularityParam = req.queryParams("granularity")
        def pageParam = req.queryParams("page")
        def pageLimitParam = req.queryParams("pageLimit")

        def timestamps = InputValidator.processTimestampsParam(beginTimestampParam,endTimestampParam)
        def beginTimestamp = timestamps.beginTimestamp
        def endTimestamp = timestamps.endTimestamp
        def granularity = InputValidator.processGranularityParam(granularityParam)
        int pageField = InputValidator.processPageParam(pageParam)
        int pageLimitField = InputValidator.processPageLimitParam(pageLimitParam)

        ODatabaseDocumentTx db = this.getDatabase(login, pass)
        LinkedHashMap params = ["beginTimestamp":beginTimestamp,
                                "endTimestamp":endTimestamp,
                                "granularity":granularity,
                                "pageField":pageField,
                                "pageLimitField":pageLimitField]

        [db:db,params:params,optionalParams:[id:id.toLong()]]
    }

    LinkedHashMap create(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        LinkedHashMap data = InputValidator.processJson(json)

        def db = this.getDatabase(login, pass)
        try {
            return this.databaseInterfacer.create(db, data, ["networkId": id.toLong()])
        } finally {
            db.close()
        }
    }

    List<LinkedHashMap> get(Request req, Response res) {
        res.type("application/json");
        res.status(200);

        def getParams = this.initializeGet(req,res)

        try {
            return this.databaseInterfacer.get(getParams.db,
                                                getParams.params,
                                                getParams.optionalParams)
        } finally {
            getParams.db.close()
        }
    }

    List<LinkedHashMap> getFromArea(Request req, Response res) {
        res.type("application/json");
        res.status(200);

        def getParams = this.initializeGet(req,res)

        try {
            return this.databaseInterfacer.getFromArea(getParams.db,
                                                        getParams.params,
                                                        getParams.optionalParams)
        } finally {
            getParams.db.close()
        }
    }

    List<LinkedHashMap> getFromGroup(Request req, Response res) {
        res.type("application/json");
        res.status(200);

        def getParams = this.initializeGet(req,res)

        try {
            return this.databaseInterfacer.getFromGroup(getParams.db,
                                                        getParams.params,
                                                        getParams.optionalParams)
        } finally {
            getParams.db.close()
        }
    }
}
