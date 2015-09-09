package org.impress.storage.databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException

class GroupInterfacer extends VertexInterfacer {
    def GroupInterfacer() {
        super("Group",
                ["name": "name",
                 "domainData": "domainData"],
                ["devices": "ifnull(out(\"GroupsResource\"),[]) as devices"])
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
        def groupName = data.name
        def domainData =  data.domainData

        return ["name": groupName,
                "domainData": domainData]
    }

    protected void generateVertexRelations(ODatabaseDocumentTx db,
                                           OrientVertex vertex,
                                           HashMap data,
                                           HashMap optionalData = [:]) {
        def deviceUrls = data.devices.unique()

        for (deviceUrl in deviceUrls) {
            if (String.isInstance(deviceUrl) && !deviceUrl.isEmpty()) {
                OrientVertex device = getVertexByUrl(db, deviceUrl)
                if (device) {
                    if (device.getLabel() != 'Resource')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + deviceUrl + "] is not a valid id for a device!",
                                "Choose an id for a device instead")

                    vertex.addEdge("GroupsResource", device)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + deviceUrl + "] was not found!",
                            "The device does not exist")
                }
            }
        }
    }
}