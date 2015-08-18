package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

class GroupInterfacer extends VertexInterfacer {
    def GroupInterfacer() {
        super("Group",
                ["name": "name",
                 "domainData": "domainData"],
                ["devices": "ifnull(out(\"GroupsResource\").networkId,[]) as devices"])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                404,
                "Group [" + id + "] was not found!",
                "The group does not exist")
    }

    void vertexNotFoundByIndex(String name) {
        throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                404,
                "Group called [" + name + "] was not found!",
                "The group does not exist")
    }

    void duplicatedVertex() {
        throw new ResponseErrorException(ResponseErrorCode.DUPLICATES_FOUND,
                400,
                "Duplicated group found!",
                "The provided group already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid group properties!",
                "The valid ones are " + this.getExpandedNames())
    }

    protected final LinkedHashMap generateVertexProperties(ODatabaseDocumentTx db,
                                                           HashMap data,
                                                           HashMap optionalData = [:]) {
        def areaName = data.name
        def domainData =  data.domainData

        return ["name": areaName,
                "domainData": domainData]
    }

    protected void generateVertexRelations(ODatabaseDocumentTx db,
                                           OrientVertex vertex,
                                           HashMap data,
                                           HashMap optionalData = [:]) {
        def deviceNames = data.devices.unique()

        for (deviceName in deviceNames) {
            if (String.isInstance(deviceName) && !deviceName.isEmpty()) {
                OrientVertex device = getVerticesByIndex(db, "networkId", deviceName, "Resource").getAt(0)
                if (device) {
                    vertex.addEdge("GroupsResource", device)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + deviceName + "] was not found!",
                            "The device does not exist")
                }
            }
        }
    }
}