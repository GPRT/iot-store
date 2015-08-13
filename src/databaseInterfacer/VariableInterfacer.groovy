package databaseInterfacer

import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

class VariableInterfacer extends VertexInterfacer {
    def VariableInterfacer(factory) {
        super(factory, "MeasurementVariable",
                ["name": "name",
                 "domainData": "domainData",
                 "unit": "unit"],
                [:])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                404,
                "Variable [" + id + "] was not found!",
                "The variable does not exist")
    }

    void vertexNotFoundByIndex(String name) {
        throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                404,
                "Variable called [" + name + "] was not found!",
                "The variable does not exist")
    }

    void duplicatedVertex() {
        throw new ResponseErrorException(ResponseErrorCode.DUPLICATES_FOUND,
                400,
                "Duplicated variable found!",
                "The provided variable already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid variable properties!",
                "The valid ones are " + this.getExpandedNames())
    }

    protected final LinkedHashMap generateVertexProperties(HashMap data) {
        def areaName = data.name
        def unitName = data.unit
        def domainData =  data.domainData

        return ["name": areaName,
                "unit": unitName,
                "domainData": domainData]
    }

    protected void generateVertexRelations(OrientVertex vertex, HashMap data) {
    }
}