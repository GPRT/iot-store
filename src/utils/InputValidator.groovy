package utils

import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import groovy.json.JsonException
import groovy.json.JsonSlurper

class InputValidator {

    static enum Granularity {
        YEARS,MONTHS,DAYS,HOURS,MINUTES,SAMPLES
    }

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

    static def processAuthentication(String encoded) {
        if (encoded == null)
            throw new ResponseErrorException(ResponseErrorCode.AUTHENTICATION_ERROR,
                    401,
                    "Credentials were not provided!",
                    "You should provide your authentication as basic auth")

            encoded = encoded.toString().split(" ")[1]
        String decoded = new String(Base64.getDecoder().decode(encoded), "UTF-8")

        String login = decoded.split(":")[0]
        String pass =  decoded.split(":")[1]
        return [login, pass]
    }

    static Long processId(String data) {
        try {
            def id =  data.toLong()

            if (id < 0) {
                throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                        400,
                        "[" + data + "] is not a valid ID!",
                        "ID must be an unsigned integer")
            }

            return id
        } catch(NumberFormatException e) {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
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

    static Integer processPageParam(String data) {
        if (!data)
            return 0

        Integer page = 0

        try {
            page = data.toInteger()
        } catch (NumberFormatException) {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "Page is not valid!",
                    "Choose an integer greater or equal to 0")
        }

        if (page < 0)
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "Page is out of range!",
                    "Choose a number greater or equal to 0")

        return page
    }

    static Integer processPageLimitParam(String data) {
        if (!data)
            return 10

        Integer pageLimit = 10

        try {
            pageLimit = data.toInteger()
        } catch (NumberFormatException) {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "Page limit is not valid!",
                    "Choose an integer between 10 and 100")
        }

        if (pageLimit < 10 || pageLimit > 100)
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "Page limit is out of range!",
                    "Choose a number between 10 and 100")

        return pageLimit
    }

    static Set processListFieldsParam(String data, Set allowedListFields) {
        if (!data)
            return allowedListFields

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
                    400,
                    "[" + data + "] is not a valid json!",
                    "The provided json cannot be parsed")
        }
    }

   static HashMap processTimestampsParam(String beginTimestamp, String endTimestamp) {
       def newBegin, newEnd
       try{
           if (!beginTimestamp)
               newBegin = new Date("01/01/01 00:00:00")
           else
               newBegin = new Date(beginTimestamp)
       }
       catch (IllegalArgumentException e){
           throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                   400,
                   "beginTimestamp ["+beginTimestamp+"] is invalid!",
                   'Possible format "MM/DD/YYYY hh:mm:ss"')
       }
       try{
           if (!endTimestamp)
               newEnd = new Date()
           else
               newEnd = new Date(endTimestamp)
       }
       catch (IllegalArgumentException e){
           throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                   400,
                   "endTimestamp ["+endTimestamp+"] is invalid!",
                   'Possible format "MM/DD/YYYY hh:mm:ss"')
       }
       return ['beginTimestamp':newBegin,'endTimestamp':newEnd]
   }

    static Granularity processGranularityParam(String granularity) {
        try{
            if (!granularity)
                return Granularity.SAMPLES
            else
                return Granularity.valueOf(granularity)

        }
        catch (IllegalArgumentException e){
            throw new ResponseErrorException(ResponseErrorCode.INVALID_GRANULARITY,
                    400,
                    "Granularity ["+granularity+"] is invalid!",
                    "Possible granularities are: YEARS,MONTHS,DAYS,HOURS,MINUTES,SAMPLES")
        }
    }
}
