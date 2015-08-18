package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

class SimulationInterfacer extends VertexInterfacer {
    def SimulationInterfacer() {
        super("Simulation",
                ["name"             : "name",
                 "domainData"       : "domainData",
                 "fakeAreaResources": "fakeAreaResources",
                 "fakeGroupResources": "fakeGroupResources"],
                ["areas" : "out(\"SimulatesArea\").name as areas",
                 "groups": "out(\"SimulatesGroup\").name as groups",
                 "ignoredResources": "out(\"ExcludesResource\").networkId as ignoredResources"])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                404,
                "Simulation [" + id + "] was not found!",
                "The simulation does not exist")
    }

    void vertexNotFoundByIndex(String name) {
        throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                404,
                "Simulation called [" + name + "] was not found!",
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
        def areaName = data.name
        def domainData = data.domainData
        def fakeAreaResourcesMaps = data.fakeAreaResources
        def fakeGroupResourcesMaps = data.fakeGroupResources

        def fakeAreaResources = []

        for (HashMap fakeAreaResourceMap in fakeAreaResourcesMaps) {
            if (HashMap.isInstance(fakeAreaResourceMap) && !fakeAreaResourceMap.isEmpty()) {
                def tempAreaName = fakeAreaResourceMap.isInArea
                OrientVertex area = getVerticesByIndex(db, "name", tempAreaName, "Area").getAt(0)
                if (area) {
                    def fakeAreaResource = ["domainData": fakeAreaResourceMap.domainData,
                                            "isInArea"  : fakeAreaResourceMap.isInArea]
                    fakeAreaResources.add(fakeAreaResource)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + tempAreaName + "] was not found!",
                            "The area does not exist")
                }
            }
        }

        def fakeGroupResources = []

        for (HashMap fakeGroupResourceMap in fakeGroupResourcesMaps) {
            if (HashMap.isInstance(fakeGroupResourceMap) && !fakeGroupResourceMap.isEmpty()) {
                def tempGroupName = fakeGroupResourceMap.isInGroup
                OrientVertex group = getVerticesByIndex(db, "name", tempGroupName, "Group").getAt(0)
                if (group) {
                    def fakeGroupResource = ["domainData": fakeGroupResourceMap.domainData,
                                             "isInGroup" : fakeGroupResourceMap.isInGroup]
                    fakeGroupResources.add(fakeGroupResource)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Group [" + tempGroupName + "] was not found!",
                            "The group does not exist")
                }
            }
        }

        return ["name"             : areaName,
                "domainData"       : domainData,
                "fakeAreaResources": fakeAreaResources,
                "fakeGroupResources": fakeGroupResources]
    }

    protected void generateVertexRelations(ODatabaseDocumentTx db,
                                           OrientVertex vertex,
                                           HashMap data,
                                           HashMap optionalData = [:]) {
        def ignoredResourceNames = data.ignoredResources.unique()
        def areaNames = data.areas.unique()
        def groupNames = data.groups.unique()

        for (ignoredResourceName in ignoredResourceNames) {
            if (String.isInstance(ignoredResourceName) && !ignoredResourceName.isEmpty()) {
                OrientVertex device = getVerticesByIndex(db, "networkId", ignoredResourceName, "Resource").getAt(0)
                if (device) {
                    vertex.addEdge("ExcludesResource", device)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + ignoredResourceName + "] was not found!",
                            "The device does not exist")
                }
            }
        }

        for (areaName in areaNames) {
            if (String.isInstance(areaName) && !areaName.isEmpty()) {
                OrientVertex area = getVerticesByIndex(db, "name", areaName, "Area").getAt(0)
                if (area) {
                    vertex.addEdge("SimulatesArea", area)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + areaName + "] was not found!",
                            "The area does not exist")
                }
            }
        }

        for (groupName in groupNames) {
            if (String.isInstance(groupName) && !groupName.isEmpty()) {
                OrientVertex group = getByIndex(graph, "name", groupName, "Group").getAt(0)
                if (group) {
                    vertex.addEdge("SimulatesGroup", group)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Group [" + groupName + "] was not found!",
                            "The group does not exist")
                }
            }
        }
    }
}
