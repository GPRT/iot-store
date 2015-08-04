package databaseInterfacer

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

class DeviceInterfacer extends ClassInterfacer {
    def DeviceInterfacer(factory) {
        super(factory, "Resource", ["name", "domainData", "networkId"])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                404,
                "Device [" + id + "] was not found!",
                "The device does not exist")
    }

    void vertexNotFoundByIndex(String name) {
        throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                404,
                "Device called [" + name + "] was not found!",
                "The device does not exist")
    }

    void duplicatedVertex() {
        throw new ResponseErrorException(ResponseErrorCode.DUPLICATES_FOUND,
                404,
                "Duplicated device found!",
                "The provided device already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                404,
                "Invalid device properties!",
                "The valid ones are " + this.fields)
    }

    protected final LinkedHashMap generateVertexProperties(HashMap data) {
        def areaName = data.name
        def networkId = data.networkId
        def domainData =  data.domainData

        return ["name": areaName,
                "networkId": networkId,
                "domainData": domainData]
    }

    protected void generateVertexRelations(OrientVertex vertex, HashMap data) {
    }

    protected LinkedHashMap getExpandedVertex(OrientVertex vertex) {
        def parentEdge = vertex.getEdges(Direction.IN, "HasResource").getAt(0)
        def parentName = ""
        if (parentEdge)
            parentName = parentEdge.getVertex(Direction.OUT).getProperty("name")

        return ["parentName": parentName]
    }
}