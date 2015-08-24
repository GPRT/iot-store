package requestProcessor

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OSecurityAccessException
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import databaseInterfacer.ClassInterfacer
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import spark.Request
import spark.Response
import utils.Endpoints
import utils.InputValidator

class RequestProcessor {
    final ClassInterfacer databaseInterfacer
    final OPartitionedDatabasePoolFactory factory

    RequestProcessor(OPartitionedDatabasePoolFactory factory, ClassInterfacer databaseInterfacer) {
        this.factory = factory
        this.databaseInterfacer = databaseInterfacer
        this.databaseInterfacer.setDefaultClusterId(getDatabase("support", "support"))
    }

    protected OrientGraph getGraph(String login, String password) {
        try {
            return new OrientGraph(this.getDatabase(login, password))
        } catch (OSecurityAccessException e) {
            throw new ResponseErrorException(ResponseErrorCode.AUTHENTICATION_ERROR,
                    400,
                    "The basic auth failed!",
                    "Check if your login and password are correct")
        }
    }

    protected ODatabaseDocumentTx getDatabase(String login, String password) {
        try {
            return this.factory.get("remote:localhost/iot", login, password).acquire()
        } catch (OSecurityAccessException e) {
            throw new ResponseErrorException(ResponseErrorCode.AUTHENTICATION_ERROR,
                    400,
                    "The basic auth failed!",
                    "Check if your login and password are correct")
        }
    }

    List<LinkedHashMap> get(Request req, Response res) {
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

        Set listFields = null

        if (isExpanded) {
            listFields = InputValidator.processListFieldsParam(fieldsParam, this.databaseInterfacer.getExpandedNames())
        } else {
            listFields = InputValidator.processListFieldsParam(fieldsParam, this.databaseInterfacer.getFieldNames())
        }

        ODatabaseDocumentTx db = this.getDatabase(login, pass)
        LinkedHashMap params = ["listFields":listFields,
                                "filterFields":filterFields,
                                "sortFields":sortFields,
                                "pageField":pageField,
                                "pageLimitField":pageLimitField]

        try {
            return this.databaseInterfacer.get(db, params)
        } finally {
            db.close()
        }
    }

    LinkedHashMap setById(Request req, Response res) {
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

        ODatabaseDocumentTx db = this.getDatabase(login, pass)
        try {
            return this.databaseInterfacer.setById(db, id, InputValidator.processJson(json))
        } finally {
            db.close()
        }
    }

    LinkedHashMap getById(Request req, Response res) {
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

        ODatabaseDocumentTx db = this.getDatabase(login, pass)
        try {
            return this.databaseInterfacer.getById(db, id, listFields)
        } finally {
            db.close()
        }
    }

    LinkedHashMap delete(Request req, Response res) {
        res.type("application/json");
        res.status(204);

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))

        ODatabaseDocumentTx db = this.getDatabase(login, pass)
        try {
            return this.databaseInterfacer.delete(db, id)
        } finally {
            db.close()
        }
    }

    LinkedHashMap create(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        String authentication = req.headers("Authorization");
        def (login, pass) = InputValidator.processAuthentication(authentication)

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        ODatabaseDocumentTx db = this.getDatabase(login, pass)
        try {
            return this.databaseInterfacer.create(db, InputValidator.processJson(json))
        } finally {
            db.close()
        }
    }
}
