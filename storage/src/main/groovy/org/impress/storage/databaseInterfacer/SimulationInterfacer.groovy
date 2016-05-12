package org.impress.storage.databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException

class SimulationInterfacer extends VertexInterfacer {
    def SimulationInterfacer() {
        super("Simulation",
                ["name"             : "name",
                 "domainData"       : "domainData",
                 "fakeAreaDevices": "fakeAreaDevices",
                 "fakeGroupDevices": "fakeGroupDevices"],
                ["areas" : "out(\"SimulatesArea\") as areas",
                 "groups": "out(\"SimulatesGroup\") as groups",
                 "devices": "out(\"IncludesResource\") as devices"])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                404,
                "Simulation [" + id + "] was not found!",
                "The simulation does not exist")
    }

    void duplicatedVertex() {
        throw new ResponseErrorException(ResponseErrorCode.DUPLICATES_FOUND,
                400,
                "Duplicated simulation found!",
                "The provided simulation already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid simulation properties!",
                "The valid ones are " + this.getExpandedNames())
    }

    protected final LinkedHashMap generateVertexProperties(ODatabaseDocumentTx db,
                                                           HashMap data,
                                                           HashMap optionalData = [:]) {
        def simulationName = data.name
        def domainData = data.domainData
        def fakeAreaResourcesMaps = data.fakeAreaDevices
        def fakeGroupResourcesMaps = data.fakeGroupDevices

        def fakeAreaResources = []

        for (HashMap fakeAreaResourceMap in fakeAreaResourcesMaps) {
            if (HashMap.isInstance(fakeAreaResourceMap) && !fakeAreaResourceMap.isEmpty()) {
                def tempAreaUrl = fakeAreaResourceMap.isInArea

                OrientVertex area = getVertexByUrl(db, tempAreaUrl)
                if (area) {
                    if (area.getLabel() != 'Area')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + tempAreaUrl + "] is not a valid id for an area!",
                                "Choose an id for an area instead")

                    def fakeAreaResource = ["domainData": fakeAreaResourceMap.domainData,
                                            "isInArea"  : area]
                    fakeAreaResources.add(fakeAreaResource)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + tempAreaUrl + "] was not found!",
                            "The area does not exist")
                }
            }
        }

        def fakeGroupResources = []

        for (HashMap fakeGroupResourceMap in fakeGroupResourcesMaps) {
            if (HashMap.isInstance(fakeGroupResourceMap) && !fakeGroupResourceMap.isEmpty()) {
                def tempGroupUrl = fakeGroupResourceMap.isInGroup

                OrientVertex group = getVertexByUrl(db, tempGroupUrl)
                if (group) {
                    if (group.getLabel() != 'Group')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + tempGroupUrl + "] is not a valid id for a group!",
                                "Choose an id for a group instead")

                    def fakeGroupResource = ["domainData": fakeGroupResourceMap.domainData,
                                             "isInGroup" : group]
                    fakeGroupResources.add(fakeGroupResource)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Group [" + tempGroupUrl + "] was not found!",
                            "The group does not exist")
                }
            }
        }

        return ["name"             : simulationName,
                "domainData"       : domainData,
                "fakeAreaDevices": fakeAreaResources,
                "fakeGroupDevices": fakeGroupResources]
    }

    protected void generateVertexRelations(ODatabaseDocumentTx db,
                                           OrientVertex vertex,
                                           HashMap data,
                                           HashMap optionalData = [:]) {
        def deviceUrls = data.devices.unique()
        def areaUrls = data.areas.unique()
        def groupUrls = data.groups.unique()

        for (deviceUrl in deviceUrls) {
            if (String.isInstance(deviceUrl) && !deviceUrl.isEmpty()) {
                OrientVertex device = getVertexByUrl(db, deviceUrl)
                if (device) {
                    if (device.getLabel() != 'Resource')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + deviceUrl + "] is not a valid id for a device!",
                                "Choose an id for a device instead")

                    vertex.addEdge("IncludesResource", device)
                    vertex.save()
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + deviceUrl + "] was not found!",
                            "The device does not exist")
                }
            }
        }

        for (areaUrl in areaUrls) {
            if (String.isInstance(areaUrl) && !areaUrl.isEmpty()) {
                OrientVertex area = getVertexByUrl(db, areaUrl)
                if (area) {
                    if (area.getLabel() != 'Area')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + areaUrl + "] is not a valid id for an area!",
                                "Choose an id for an area instead")

                    vertex.addEdge("SimulatesArea", area)
                    vertex.save()
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + areaUrl + "] was not found!",
                            "The area does not exist")
                }
            }
        }

        for (groupUrl in groupUrls) {
            if (String.isInstance(groupUrl) && !groupUrl.isEmpty()) {
                OrientVertex group = getVertexByUrl(db, groupUrl)
                if (group) {
                    if (group.getLabel() != 'Group')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + groupUrl + "] is not a valid id for a group!",
                                "Choose an id for a group instead")

                    vertex.addEdge("SimulatesGroup", group)
                    vertex.save()
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Group [" + groupUrl + "] was not found!",
                            "The group does not exist")
                }
            }
        }
    }
}
