package requestProcessor

import databaseInterfacer.ClassInterfacer
import spark.Request
import spark.Response
import utils.InputValidator

class RequestProcessor {
    final ClassInterfacer databaseInterfacer

    RequestProcessor(ClassInterfacer databaseInterfacer) {
        this.databaseInterfacer = databaseInterfacer
    }

    public List<LinkedHashMap> get(Request req, Response res) {
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

        return this.databaseInterfacer.get(listFields, filterFields, sortFields, pageField, pageLimitField)

    }

    final LinkedHashMap setById(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        return this.databaseInterfacer.setById(id, InputValidator.processJson(json))
    }

    final LinkedHashMap getById(Request req, Response res) {
        res.type("application/json");

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["expanded"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"));

        def expandedParam = req.queryParams("expanded")

        def isExpanded = InputValidator.processExpandedParam(expandedParam)
        Set listFields = null

        if (isExpanded) {
            listFields = this.databaseInterfacer.getExpandedNames()
        } else {
            listFields = this.databaseInterfacer.getFieldNames()
        }

        return this.databaseInterfacer.getById(id, listFields)
    }

    final LinkedHashMap delete(Request req, Response res) {
        res.type("application/json");
        res.status(204);

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = InputValidator.processId(req.params(":id"))
        return this.databaseInterfacer.delete(id);
    }

    final LinkedHashMap create(Request req, Response res) {
        res.type ( "application/json" );
        res.status(201);

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = []

        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        Long id = -1
        if(req.params(":id"))
            id = InputValidator.processId(req.params(":id"))

        String json = req.body()
        json = (!json.isEmpty()) ? json : "{}"

        if(id>=0)
            return this.databaseInterfacer.create(InputValidator.processJson(json),id.toLong())
        else
            return this.databaseInterfacer.create(InputValidator.processJson(json))
    }
}
