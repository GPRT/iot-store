package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
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
                ["area": "ifnull(in(\"HasResource\")[0], \"\") as area",
                 "groups": "ifnull(in(\"GroupsResource\"),[]) as groups"])
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

    protected final LinkedHashMap generateVertexProperties(ODatabaseDocumentTx db,
                                                           HashMap data,
                                                           HashMap optionalData = [:]) {
        def networkId = data.networkId
        def domainData =  data.domainData

        return ["networkId": networkId,
                "domainData": domainData]
    }

    protected void generateVertexRelations(ODatabaseDocumentTx db,
                                           OrientVertex vertex,
                                           HashMap data,
                                           HashMap optionalData = [:]) {
        def areaUrl = data.area
        def groupUrls = data.groups.unique()

        if (String.isInstance(areaUrl) && !areaUrl.isEmpty()) {
            OrientVertex area = getVertexByUrl(db, areaUrl)
            if (area) {
                if (area.getLabel() != 'Area')
                    throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                            400,
                            "[" + areaUrl + "] is not a valid id for an area!",
                            "Choose an id for an area instead")

                area.addEdge("HasResource", vertex)
            } else {
                throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                        404,
                        "Area [" + areaUrl + "] was not found!",
                        "The area does not exist")
            }
        }

        for (groupUrl in groupUrls) {
            if (String.isInstance(groupUrl) && !groupUrl.isEmpty()) {
                OrientVertex group = getVertexByUrl(db, areaUrl)
                if (group) {
                    if (group.getLabel() != 'Group')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + groupUrl + "] is not a valid id for a group!",
                                "Choose an id for a group instead")

                    group.addEdge("GroupsResource", vertex)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                            404,
                            "Group [" + groupUrl + "] was not found!",
                            "The group does not exist")
                }
            }
        }

        if (!vertex.getProperty('measurements')) {
            ODocument measurements = new ODocument("Measurements")
            measurements.field('year', new LinkedHashMap())
            measurements.save()
            vertex.setProperty('measurements', measurements.getIdentity())
        }
    }
}