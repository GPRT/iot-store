package databaseInterfacer

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument

class DeviceInterfacer extends VertexInterfacer {
    def DeviceInterfacer(factory) {
        super(factory, "Resource", ["name", "domainData", "networkId", "areaName", "groupNames"])
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
                400,
                "Duplicated device found!",
                "The provided device already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid device properties!",
                "The valid ones are " + this.fields)
    }

    protected final LinkedHashMap generateVertexProperties(HashMap data) {
        def deviceName = data.name
        def networkId = data.networkId
        def domainData =  data.domainData

        return ["name": deviceName,
                "networkId": networkId,
                "domainData": domainData]
    }

    protected void generateVertexRelations(OrientVertex vertex, HashMap data) {
        def areaName = data.areaName
        def groupNames = data.groupNames.unique()

        if (String.isInstance(areaName) && !areaName.isEmpty()) {
            OrientVertex area = getVerticesByIndex("name", areaName, "Area").getAt(0)
            if (area) {
                area.addEdge("HasResource", vertex)
            } else {
                throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                        404,
                        "Area [" + areaName + "] was not found!",
                        "The area does not exist")
            }
        } else {
            invalidVertexProperties()
        }

        for (groupName in groupNames) {
            if (String.isInstance(groupName) && !groupName.isEmpty()) {
                OrientVertex group = getVerticesByIndex("name", groupName, "Group").getAt(0)
                if (group) {
                    group.addEdge("GroupsResource", vertex)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                            404,
                            "Group [" + groupName + "] was not found!",
                            "The group does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }

        def measurements = new ODocument("Measurements")

        measurements.field('year',new LinkedHashMap())
        measurements.save()

        vertex.setProperty('measurements',measurements.getIdentity())
    }

    protected LinkedHashMap getExpandedVertex(OrientVertex vertex) {
        def areaEdge = vertex.getEdges(Direction.IN, "HasResource").getAt(0)
        def parentName = ""
        if (areaEdge)
            parentName = areaEdge.getVertex(Direction.OUT).getProperty("name")

        def groupsEdge = vertex.getEdges(Direction.IN, "GroupsResource")
        def groupNames = []
        for (groupEdge in groupsEdge) {
            def groupName = groupEdge.getVertex(Direction.OUT).getProperty("name")
            groupNames.add(groupName)
        }

        return ["areaName": parentName, "groupNames": groupNames]
    }
}