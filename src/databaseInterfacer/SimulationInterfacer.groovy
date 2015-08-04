package databaseInterfacer

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

class SimulationInterfacer extends ClassInterfacer {
    def SimulationInterfacer(factory) {
        super(factory, "Simulation",
                ["name", "domainData", "areas", "groups", "ignoredResources", "fakeAreaResources", "fakeGroupResources"])
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
                404,
                "Duplicated simulation found!",
                "The provided simulation already exist")
    }

    void invalidVertexProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                404,
                "Invalid simulation properties!",
                "The valid ones are " + this.fields)
    }

    protected final LinkedHashMap generateVertexProperties(HashMap data) {
        def areaName = data.name
        def domainData =  data.domainData
        def fakeAreaResourcesMaps = data.fakeAreaResources
        def fakeGroupResourcesMaps = data.fakeGroupResources

        def fakeAreaResources = []

        for (HashMap fakeAreaResourceMap in fakeAreaResourcesMaps) {
            if (HashMap.isInstance(fakeAreaResourceMap) && !fakeAreaResourceMap.isEmpty()) {
                def tempAreaName = fakeAreaResourceMap.isInArea
                OrientVertex area = getVerticesByIndex("name", tempAreaName, "Area").getAt(0)
                if (area) {
                    def fakeAreaResource = ["domainData":fakeAreaResourceMap.domainData,
                                             "isInArea":fakeAreaResourceMap.isInArea]
                    fakeAreaResources.add(fakeAreaResource)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + tempAreaName + "] was not found!",
                            "The area does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }

        def fakeGroupResources = []

        for (HashMap fakeGroupResourceMap in fakeGroupResourcesMaps) {
            if (HashMap.isInstance(fakeGroupResourceMap) && !fakeGroupResourceMap.isEmpty()) {
                def tempGroupName = fakeGroupResourceMap.isInGroup
                OrientVertex group = getVerticesByIndex("name", tempGroupName, "Group").getAt(0)
                if (group) {
                    def fakeGroupResource = ["domainData":fakeGroupResourceMap.domainData,
                                             "isInGroup":fakeGroupResourceMap.isInGroup]
                    fakeGroupResources.add(fakeGroupResource)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Group [" + tempGroupName + "] was not found!",
                            "The group does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }

        return ["name": areaName,
                "domainData": domainData,
                "fakeAreaResources": fakeAreaResources,
                "fakeGroupResources": fakeGroupResources]
    }

    protected void generateVertexRelations(OrientVertex vertex, HashMap data) {
        def ignoredResourceNames = data.ignoredResources
        def areaNames = data.areas
        def groupNames = data.groups

        for (ignoredResourceName in ignoredResourceNames) {
            if (String.isInstance(ignoredResourceName) && !ignoredResourceName.isEmpty()) {
                OrientVertex device = getVerticesByIndex("name", ignoredResourceName, "Resource").getAt(0)
                if (device) {
                    vertex.addEdge("ExcludesResource", device)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + ignoredResourceName + "] was not found!",
                            "The device does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }

        for (areaName in areaNames) {
            if (String.isInstance(areaName) && !areaName.isEmpty()) {
                OrientVertex area = getVerticesByIndex("name", areaName, "Area").getAt(0)
                if (area) {
                    vertex.addEdge("SimulatesArea", area)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + areaName + "] was not found!",
                            "The area does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }

        for (groupName in groupNames) {
            if (String.isInstance(groupName) && !groupName.isEmpty()) {
                OrientVertex group = getVerticesByIndex("name", groupName, "Group").getAt(0)
                if (group) {
                    vertex.addEdge("SimulatesGroup", group)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Group [" + groupName + "] was not found!",
                            "The group does not exist")
                }
            } else {
                invalidVertexProperties()
            }
        }
    }

    protected LinkedHashMap getExpandedVertex(OrientVertex vertex) {
        def deviceNames = []

        def areaEdges = vertex.getEdges(Direction.OUT, "SimulatesArea")
        def areaNames = []
        if (areaEdges)
            areaEdges.each {
                def area = it.getVertex(Direction.IN)

                def areaDeviceEdges = vertex.getEdges(Direction.OUT, "HasResource")
                if (areaDeviceEdges)
                    areaDeviceEdges.each {
                        def deviceName = it.getVertex(Direction.IN).getProperty("name")
                        deviceNames.add(deviceName)
                    }

                def areaName = area.getProperty("name")
                areaNames.add(areaName)
            }

        def groupEdges = vertex.getEdges(Direction.OUT, "SimulatesGroup")
        def groupNames = []
        if (groupEdges)
            groupEdges.each {
                def group = it.getVertex(Direction.IN)

                def groupDeviceEdges = group.getEdges(Direction.OUT, "GroupsResource")
                if (groupDeviceEdges)
                    groupDeviceEdges.each {
                        def deviceName = it.getVertex(Direction.IN).getProperty("name")
                        deviceNames.add(deviceName)
                    }

                def groupName = group.getProperty("name")
                groupNames.add(groupName)
            }

        def ignoredResourceEdges = vertex.getEdges(Direction.OUT, "ExcludesResource")
        def ignoredResourceNames = []
        if (ignoredResourceEdges)
            ignoredResourceEdges.each {
                def deviceName = it.getVertex(Direction.IN).getProperty("name")
                ignoredResourceNames.add(deviceName)
            }

        return ["devices": deviceNames, "ignoredResources": ignoredResourceNames,
                "areas": areaNames, "groups": groupNames]
    }
}