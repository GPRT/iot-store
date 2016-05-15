package org.impress.storage.databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException

class AreaInterfacer extends VertexInterfacer {
    def AreaInterfacer() {
        super("Area",
                ["name": "name",
                 "domainData": "domainData"],
                ["parentArea": "ifnull(in(\"HasArea\")[0],\"\") as parentArea",
                 "areas": "ifnull(out(\"HasArea\"),[]) as areas",
                 "devices": "ifnull(out(\"HasResource\"),[]) as devices"])
    }

    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                404,
                "Area [" + id + "] was not found!",
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

        def parentAreaUrl = data.parentArea

        if (parentAreaUrl && !parentAreaUrl.isEmpty()) {
            OrientVertex parent = getVertexByUrl(db, parentAreaUrl)

            if (parent) {
                if (parent.getLabel() != 'Area')
                    throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                            400,
                            "[" + parentAreaUrl + "] is not a valid id for an area!",
                            "Choose an id for an area instead")

                parent.addEdge("HasArea", vertex)
                parent.save()
            }
            else {
                throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                        404,
                        "Area [" + parentAreaUrl + "] was not found!",
                        "The area does not exist")
            }
        }

        def areaUrls = data.areas.unique()

        for (areaUrl in areaUrls) {
            if (String.isInstance(areaUrl) && !areaUrl.isEmpty()) {
                OrientVertex area = getVertexByUrl(db, areaUrl)
                if (area) {
                    if (area.getLabel() != 'Area')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + areaUrl + "] is not a valid id for an area!",
                                "Choose an id for an area instead")

                    vertex.addEdge("HasArea", area)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Area [" + areaUrl + "] was not found!",
                            "The area does not exist")
                }
            }
        }

        def resourceUrls = data.devices.unique()

        for (resourceUrl in resourceUrls) {
            if (String.isInstance(resourceUrl) && !resourceUrl.isEmpty()) {
                OrientVertex device = getVertexByUrl(db, resourceUrl)

                if (device) {
                    def numAreas = device.countEdges(Direction.IN, "HasResource")

                    if (numAreas > 0)
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "Device [" + resourceUrl + "] is already part of an area!",
                                "Remove the device from the area in question")

                    if (device.getLabel() != 'Resource')
                        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                                400,
                                "[" + resourceUrl + "] is not a valid id for a device!",
                                "Choose an id for a device instead")

                    vertex.addEdge("HasResource", device)
                } else {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device [" + resourceUrl + "] was not found!",
                            "The device does not exist")
                }
            }
        }
    }

    protected final LinkedHashMap recordToMap(ODatabaseDocumentTx db, ORecord record) {
        def result = this.orientTransformer.fromODocument(record)

        if (result.containsKey('devices')) {
            result['inheritedAreas'] = []
            result['inheritedDevices'] = []

            def rid = record.field('id').getIdentity()
            def osql = "select unionall(areas) as areas, unionall(devices) as devices from " +
                    "(select out('HasArea') as areas, out('HasResource') as devices from " +
                    "(traverse out('HasArea') from (select out('HasArea') from ${rid})))"
            db.command(new OSQLSynchQuery(osql)).execute().collect {
                def records = this.orientTransformer.fromODocument(it)

                result['inheritedAreas'] = records['areas']
                result['inheritedDevices'] = records['devices']
            }
        }

        return result
    }
}
