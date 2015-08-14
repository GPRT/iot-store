package requestProcessor
import spark.Request
import spark.Response

import databaseInterfacer.MeasurementInterfacer
import utils.InputValidator

class MeasurementRequestProcessor extends RequestProcessor {

    MeasurementRequestProcessor(factory) {
        super(new MeasurementInterfacer(factory))
    }
    public List<LinkedHashMap> get(Request req, Response res) {
        res.type("application/json");

        Set<String> queryFields = req.queryParams()
        Set<String> allowedQueryParams = ["fields", "filter", "sort", "expanded", "page", "pageLimit",
                                          "beginTimestamp", "endTimestamp", "granularity"]
        InputValidator.validateQueryParams(queryFields, allowedQueryParams)

        def fieldsParam = req.queryParams("fields")
        def expandedParam = req.queryParams("expanded")
        def filterFieldsParam = req.queryParams("filter")
        def sortFieldsParam = req.queryParams("sort")
        def pageParam = req.queryParams("page")
        def pageLimitParam = req.queryParams("pageLimit")
        def beginTimestampParam = req.queryParams("beginTimestamp")
        def endTimestampParam = req.queryParams("endTimestamp")
        def granularityParam = req.queryParams("granularity")
        def id = req.params(':id')

        boolean isExpanded = InputValidator.processExpandedParam(expandedParam)
        Set filterFields = InputValidator.processFilterParam(filterFieldsParam)
        Set sortFields = InputValidator.processSortParam(sortFieldsParam)
        int pageField = InputValidator.processPageParam(pageParam)
        int pageLimitField = InputValidator.processPageLimitParam(pageLimitParam)
        def beginTimestamp = InputValidator.processTimestampParam(beginTimestampParam)
        def endTimestamp = InputValidator.processTimestampParam(endTimestampParam)
        def granularity = InputValidator.processGranularityParam(granularityParam)

        Set allowedFieldNames = null
        Set listFields = null

        if (isExpanded) {
            listFields = InputValidator.processListFieldsParam(fieldsParam, this.databaseInterfacer.getExpandedNames())
        } else {
            listFields = InputValidator.processListFieldsParam(fieldsParam, this.databaseInterfacer.getFieldNames())
        }

        return this.databaseInterfacer.get(listFields, filterFields, sortFields, pageField, pageLimitField,
                                            "Measurements",id,beginTimestamp, endTimestamp, granularity)

    }
}
