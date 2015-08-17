package databaseInterfacer

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument

class DeviceInterfacer extends VertexInterfacer {
    def DeviceInterfacer() {
        super("Resource",
                ["domainData": "domainData",
                 "networkId": "networkId"],
                ["areaName": "ifnull(in(\"HasResource\").name[0], \"\") as areaName",
                 "groupNames": "ifnull(in(\"GroupsResource\").networkId,[]) as groupNames"])
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
                "The valid ones are " + this.getExpandedNames())
    }

    protected final LinkedHashMap generateVertexProperties(HashMap data) {
        def networkId = data.networkId
        def domainData =  data.domainData

        return ["networkId": networkId,
                "domainData": domainData]
    }

    protected void generateVertexRelations(OrientGraph graph, OrientVertex vertex, HashMap data) {
        def areaName = data.areaName
        def groupNames = data.groupNames.unique()

        if (String.isInstance(areaName) && !areaName.isEmpty()) {
            OrientVertex area = getByIndex("name", areaName, "Area").getAt(0)
            if (area) {
                area.addEdge("HasResource", vertex)
            } else {
                throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                        404,
                        "Area [" + areaName + "] was not found!",
                        "The area does not exist")
            }
        }

        for (groupName in groupNames) {
            if (String.isInstance(groupName) && !groupName.isEmpty()) {
                OrientVertex group = getByIndex("name", groupName, "Group").getAt(0)
                if (group) {
                    group.addEdge("GroupsResource", vertex)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                            404,
                            "Group [" + groupName + "] was not found!",
                            "The group does not exist")
                }
            }
        }

        def measurements = new ODocument("Measurements")
        measurements.field('year',new LinkedHashMap())
        measurements.save()

        vertex.setProperty('measurements', measurements.getIdentity())
    }
}