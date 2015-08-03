package utils

import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import groovy.json.JsonException
import groovy.json.JsonSlurper

class InputValidator {
    static String validateQueryParams(fields, allowedQueryParams) {
        fields.each { queryParamName ->
            if (!(queryParamName in allowedQueryParams))
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        400,
                        "[" + queryParamName + "] is not a valid field!",
                        "This endpoint does not support [" + queryParamName + "] as a field")
        }
    }

    static def processRequiredField(String fieldName, data) {
        if (!data.isEmpty()) {
            return data
        } else {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "[" + fieldName + "] is empty!",
                    "The provided [" + fieldName + "] field cannot be parsed")
        }
    }

    static Long processId(String data) {
        try {
            def id =  data.toLong()

            if (id < 0) {
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        404,
                        "[" + data + "] is not a valid ID!",
                        "ID must be an unsigned integer")
            }

            return id
        } catch(NumberFormatException e) {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    404,
                    "[" + data + "] is not a valid ID!",
                    "ID must be an unsigned integer")
        }
    }

    static Boolean processExpandedParam(String data) {
        if (!data)
            return false

        if (data == "1")
            return true
        else if (data == "0")
            return false

        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "[" + data + "] is not a valid boolean!",
                "Expanded must be a boolean")
    }

    static List processListFields(String data, List allowedListFields) {
        if (!data)
            return ["*"]

        String[] listFields = data.split(",")
        def results = []

        listFields.each { fieldString ->
            if (fieldString.isEmpty())
                return

            if (!(fieldString in allowedListFields))
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        400,
                        "[" + fieldString + "] is not a valid field!",
                        "Field must be one of " + allowedListFields)

            results.add(fieldString)
        }

        return results
    }

    static List processFilterParam(String data) {
        if (!data)
            return []

        String[] filterFields = data.split(",")
        String[] availableFieldOperators = ['gt', 'lt', 'gte', 'lte', 'eq', 'neq']
        def results = []

        filterFields.each { filterString ->
            if (filterString.isEmpty())
                return

            def (fieldName, fieldOperator, constant) = filterString.split(":")

            if (!(fieldOperator in availableFieldOperators))
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        400,
                        "[" + fieldOperator + "] is not a valid field operator!",
                        "Field order must be one of " + availableFieldOperators)

            results.add([fieldName, fieldOperator, constant])
        }

        return results
    }

    static List processSortParam(String data) {
        if (!data)
            return []

        String[] sortFields = data.split(",")
        String[] availableFieldOrders = ['asc', 'desc']
        def results = []

        sortFields.each { sortString ->
            if (sortString.isEmpty())
                return

            def (fieldOrder, fieldName) = sortString.split(":")

            if (fieldName == "id")
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        400,
                        "[" + fieldName + "] is not a valid field for ordering!",
                        "To filter by id, just do not use sortFields")

            if (!(fieldOrder in availableFieldOrders))
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        400,
                        "[" + fieldOrder + "] is not a valid field order!",
                        "Field order must be one of " + availableFieldOrders)

            results.add([fieldName, fieldOrder])
        }

        return results
    }

    static HashMap processJson(String data) {
        try {
            return new JsonSlurper().parseText(data)
        } catch (JsonException e) {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    404,
                    "[" + data + "] is not a valid json!",
                    "The provided json cannot be parsed")
        }
    }
}
