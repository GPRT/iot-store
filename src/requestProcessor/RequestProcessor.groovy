package requestProcessor

import databaseInterfacer.ClassInterfacer
import spark.Request
import spark.Response
import utils.InputValidator

class RequestProcessor {
    final List fields
    final ClassInterfacer databaseInterfacer

    RequestProcessor(ClassInterfacer databaseInterfacer, List fields) {
        this.fields = fields
        this.databaseInterfacer = databaseInterfacer
    }

    final List<LinkedHashMap> get(Request req, Response res) {
        res.type("application/json");

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["fields", "filter", "sort", "expanded", "page", "pageLimit"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        def fieldsParam = req.queryParams("fields")
        def expandedParam = req.queryParams("expanded")
        def filterFieldsParam = req.queryParams("filter")
        def sortFieldsParam = req.queryParams("sort")
        def pageParam = req.queryParams("page")
        def pageLimitParam = req.queryParams("pageLimit")

        def listFields = InputValidator.processListFieldsParam(fieldsParam, this.fields)
        def isExpanded = InputValidator.processExpandedParam(expandedParam)
        def filterFields = InputValidator.processFilterParam(filterFieldsParam)
        def sortFields = InputValidator.processSortParam(sortFieldsParam)
        def pageField = InputValidator.processPageParam(pageParam)
        def pageLimitField = InputValidator.processPageLimitParam(pageLimitParam)

        if (isExpanded)
            return this.databaseInterfacer.getExpandedVertices(listFields,filterFields, sortFields, pageField, pageLimitField)
        else
            return this.databaseInterfacer.getVertices(listFields, filterFields, sortFields, pageField, pageLimitField)
    }

    final LinkedHashMap setById(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"));

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        return this.databaseInterfacer.setVertexById(id, InputValidator.processJson(json))
    }

    final LinkedHashMap getById(Request req, Response res) {
        res.type("application/json");

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["expanded"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"));

        def expandedParam = req.queryParams("expanded")

        def isExpanded = InputValidator.processExpandedParam(expandedParam)

        if (isExpanded)
            return this.databaseInterfacer.getExpandedVertexById(id)
        else
            return this.databaseInterfacer.getVertexById(id)
    }

    final LinkedHashMap delete(Request req, Response res) {
        res.type("application/json");
        res.status(204);

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"));
        return this.databaseInterfacer.deleteVertex(id);
    }

    final LinkedHashMap create(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        return this.databaseInterfacer.createVertex(InputValidator.processJson(json))
    }
}
