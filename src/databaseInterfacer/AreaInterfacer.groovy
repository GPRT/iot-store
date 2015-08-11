package databaseInterfacer

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

class AreaInterfacer extends VertexInterfacer {
    def AreaInterfacer(factory) {
        super(factory, "Area", ["name", "domainData", "parentArea", "devices"])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                404,
                "Area [" + id + "] was not found!",
                "The area does not exist")
    }

    void vertexNotFoundByIndex(String name) {
        throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                404,
                "Area called [" + name + "] was not found!",
                "The area does not exist")
    }

    void duplicatedVertex() {
        throw new ResponseErrorException(ResponseErrorCode.DUPLICATES_FOUND,
                400,
                "Duplicated area found!",
                "The provided area already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid area properties!",
                "The valid ones are " + this.fields)
    }

    protected final LinkedHashMap generateVertexProperties(HashMap data) {
        def areaName = data.name
        def domainData =  data.domainData

        return ["name": areaName,
                "domainData": domainData]
    }

    protected void generateVertexRelations(OrientVertex vertex, HashMap data) {
        def parentAreaName = data.parentArea

        if (parentAreaName && !parentAreaName.isEmpty()) {
            OrientVertex parent = getVerticesByIndex("name", parentAreaName).getAt(0)
            if (parent) {
                parent.addEdge("HasArea", vertex)
            }
            else {
                vertexNotFoundByIndex(parentAreaName)
            }
        }

        def resourcesNames = data.devices.unique()

        for (resourceName in resourcesNames) {
            if (String.isInstance(resourceName) && !resourceName.isEmpty()) {
                OrientVertex device = getVerticesByIndex("name", resourceName, "Resource").getAt(0)
                if (device) {
                    def numAreas = device.countEdges(Direction.IN, "HasResource")

                    if (numAreas > 0)
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "Device [" + resourceName + "] is already part of an area!",
                                "Remove the device from the area in question")

                    vertex.addEdge("HasResource", device)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + resourceName + "] was not found!",
                            "The device does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }
    }

    protected LinkedHashMap getExpandedVertex(OrientVertex vertex) {
        def parentEdge = vertex.getEdges(Direction.IN, "HasArea").getAt(0)
        def parentName = ""
        if (parentEdge)
            parentName = parentEdge.getVertex(Direction.OUT).getProperty("name")

        def deviceEdges = vertex.getEdges(Direction.OUT, "HasResource")
        def deviceNames = []
        if (deviceEdges)
            deviceEdges.each {
                def deviceName = it.getVertex(Direction.IN).getProperty("name")
                deviceNames.add(deviceName)
            }

        return ["parentName": parentName, "devices": deviceNames]
    }
}