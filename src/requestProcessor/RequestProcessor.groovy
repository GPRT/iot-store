package requestProcessor

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OSecurityAccessException
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import databaseInterfacer.ClassInterfacer
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import spark.Request
import spark.Response
import utils.InputValidator

class RequestProcessor {
    final ClassInterfacer databaseInterfacer
    final OPartitionedDatabasePoolFactory factory

    RequestProcessor(OPartitionedDatabasePoolFactory factory, ClassInterfacer databaseInterfacer) {
        this.factory = factory
        this.databaseInterfacer = databaseInterfacer
    }

    protected OrientGraph getGraph(String login, String password) {
        try {
            return new OrientGraph(this.factory.get("remote:localhost/iot", login, password))
        } catch (OSecurityAccessException e) {
            throw new ResponseErrorException(ResponseErrorCode.AUTHENTICATION_ERROR,
                    400,
                    "The basic auth failed!",
                    "Check if your login and password are correct")
        }
    }

    protected ODatabaseDocumentTx getDatabase(String login="root", String password="123456") {
        try {
            return this.factory.get("remote:localhost/iot", login, password).acquire()
        } catch (OSecurityAccessException e) {
            throw new ResponseErrorException(ResponseErrorCode.AUTHENTICATION_ERROR,
                    400,
                    "The basic auth failed!",
                    "Check if your login and password are correct")
        }
    }

    final List<LinkedHashMap> get(Request req, Response res) {
        res.type("application/json");

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["fields", "filter", "sort", "expanded", "page", "pageLimit"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        def fieldsParam = req.queryParams("fields")
        def expandedParam = req.queryParams("expanded")
        def filterFieldsParam = req.queryParams("filter")
        def sortFieldsParam = req.queryParams("sort")
        def pageParam = req.queryParams("page")
        def pageLimitParam = req.queryParams("pageLimit")

        boolean isExpanded = InputValidator.processExpandedParam(expandedParam)
        Set filterFields = InputValidator.processFilterParam(filterFieldsParam)
        Set sortFields = InputValidator.processSortParam(sortFieldsParam)
        int pageField = InputValidator.processPageParam(pageParam)
        int pageLimitField = InputValidator.processPageLimitParam(pageLimitParam)

        Set allowedFieldNames = null
        Set listFields = null

        if (isExpanded) {
            listFields = InputValidator.processListFieldsParam(fieldsParam, this.databaseInterfacer.getExpandedNames())
        } else {
            listFields = InputValidator.processListFieldsParam(fieldsParam, this.databaseInterfacer.getFieldNames())
        }

        OrientGraph graph = this.getGraph(login, pass)
        try {
            return this.databaseInterfacer.get(graph, listFields, filterFields, sortFields, pageField, pageLimitField)
        } finally {
            graph.shutdown()
        }
    }

    final LinkedHashMap setById(Request req, Response res) {
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

        OrientGraph graph = this.getGraph(login, pass)
        try {
            return this.databaseInterfacer.setById(graph, id, InputValidator.processJson(json))
        } finally {
            graph.shutdown()
        }
    }

    final LinkedHashMap getById(Request req, Response res) {
        res.type("application/json");

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["expanded"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))

        def expandedParam = req.queryParams("expanded")

        def isExpanded = InputValidator.processExpandedParam(expandedParam)

        Set listFields = null

        if (isExpanded) {
            listFields = this.databaseInterfacer.getExpandedNames()
        } else {
            listFields = this.databaseInterfacer.getFieldNames()
        }

        OrientGraph graph = this.getGraph(login, pass)
        try {
            return this.databaseInterfacer.getById(graph, id, listFields)
        } finally {
            graph.shutdown()
        }
    }

    final LinkedHashMap delete(Request req, Response res) {
        res.type("application/json");
        res.status(204);

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))

        OrientGraph graph = this.getGraph(login, pass)
        try {
            return this.databaseInterfacer.delete(graph, id)
        } finally {
            graph.shutdown()
        }
    }

    final LinkedHashMap createVertex(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        OrientGraph graph = this.getGraph(login, pass)
        try {
            return this.databaseInterfacer.create(graph, InputValidator.processJson(json))
        } finally {
            graph.shutdown()
        }
    }

    final LinkedHashMap createDocument(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        OrientGraph graph = this.getGraph(login, pass)
        try {
            if(id>=0)
                return this.databaseInterfacer.create(InputValidator.processJson(json),id.toLong())
            else
                return this.databaseInterfacer.create(InputValidator.processJson(json))
        } finally {
            graph.shutdown()
        }
    }
}
