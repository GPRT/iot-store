package org.impress.storage.databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OValidationException
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.impress.storage.exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.id.ORecordId
import org.impress.storage.utils.SearchHelpers
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.utils.Endpoints
import org.impress.storage.utils.Granularity

import javax.xml.bind.DatatypeConverter

class MeasurementInterfacer extends DocumentInterfacer {

    def MeasurementInterfacer() {
        super("Sample",
                ["timestamp"          : "timestamp",
                 "measurementVariable": "measurementVariable",
                 "value"              : "value"],
                [:])
    }


    void vertexNotFoundById(Long id) {
        throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                404,
                "Measurement Variable [" + id + "] was not found!",
                "The variable does not exist")
    }

    void invalidDocumentProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid measurement properties!",
                "The valid ones are " + this.getExpandedNames())
    }

    void documentNotFoundById() {
        throw new ResponseErrorException(ResponseErrorCode.MEASUREMENT_NOT_FOUND,
                404,
                "Measurement [" + id + "] was not found!",
                "The measurement does not exist")
    }

    protected LinkedHashMap create(ODatabaseDocumentTx db,
                                   HashMap data,
                                   HashMap optionalData = [:]){

        if (!(this.getFieldNames() == data.keySet()))
            invalidDocumentProperties()

        if (data.isEmpty())
            invalidDocumentProperties()

        try {
            db.begin()
            def properties = generateDocumentProperties(db, data, optionalData)

            if (null in properties.values())
                invalidDocumentProperties()

            def document = new ODocument('Sample')
            document.field('value', properties['value'])
            document.field('timestamp', properties['timestamp'])

            generateDocumentRelations(db, document, data, optionalData)
            db.commit()

            def json = this.orientTransformer.fromODocument(document)
            json['timestamp'] = data.timestamp
            json
        }
        catch(OValidationException e) {
            invalidDocumentProperties()
        }
    }

    protected Iterable<LinkedHashMap> getFromArea(ODatabaseDocumentTx db,
                                                  Map params = [:],
                                                  Map optionalParams = [:],
                                                  String className = this.className) {
        OrientGraph graph = new OrientGraph(db)
        def area
        def areaId = optionalParams.id
        def areas = []
        def devices = []

        if (areaId >= 0) {
            def clusterId = this.getClusterId(db, 'Area')
            def rid = new ORecordId(clusterId, areaId.toLong())
            area = graph.getVertex(rid)
            if (!area) {
                throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                        404,
                        "Area with id [" + areaId + "] not found!",
                        "The area does not exist")
            }
        }

        areas.add(area)
        while (areas) {
            def deeperAreas = []
            areas.each {
                it.getVertices(Direction.OUT).each {
                    if (it.getLabel() == 'Area')
                        deeperAreas.add(it)
                    else if (it.getLabel() == 'Resource')
                        devices.add(it)
                }
            }
            areas = deeperAreas
        }

        this.getFromDeviceCollection(db,
                params,optionalParams,
                className,devices).sort { a,b -> b.timestamp <=> a.timestamp }
    }

    protected Iterable<LinkedHashMap> getFromGroup(ODatabaseDocumentTx db,
                                                   Map params = [:],
                                                   Map optionalParams = [:],
                                                   String className = this.className) {
        OrientGraph graph = new OrientGraph(db)
        def group
        def groupId = optionalParams.id
        def groups = []
        def devices = []

        if (groupId >= 0) {
            def clusterId = this.getClusterId(db, 'Group')
            def rid = new ORecordId(clusterId, groupId.toLong())
            group = graph.getVertex(rid)
            if (!group) {
                throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                        404,
                        "Group with id [" + groupId + "] not found!",
                        "The group does not exist")
            }
        }

        groups.add(group)
        while (groups) {
            def deeperGroups = []
            groups.each {
                it.getVertices(Direction.OUT).each {
                    if (it.getLabel() == 'Group')
                        deeperGroups.add(it)
                    else if (it.getLabel() == 'Resource')
                        devices.add(it)
                }
            }
            groups = deeperGroups
        }

        this.getFromDeviceCollection(db,
                params,optionalParams,
                className,devices).sort { a,b -> b.timestamp <=> a.timestamp }
    }

    protected Iterable<LinkedHashMap> getFromDeviceCollection(ODatabaseDocumentTx db,
                                                              Map params = [:],
                                                              Map optionalParams = [:],
                                                              String className = this.className,
                                                              ArrayList devices) {
        def mean = { it.sum()/it.size() }
        def granularity = Granularity.valueOf(params.granularity.toString())

        def measurementPipe = devices.collect {
            this.get(db, params,
                    [id: it.getId().getClusterPosition(),
                     variableId:optionalParams.variableId],
                     className)
        }
        if(granularity < Granularity.SAMPLES) {
            measurementPipe.sum().groupBy({ it.timestamp })
                    .collect {

                def sumResult = it.value.collect{ it.value.sum }.sum()
                def meanResult = it.value.collect { it.value.mean }

                [value:[sum      : sumResult,
                        mean     : meanResult],
                 timestamp: it.key]
            }
        }
        else {
            measurementPipe.sum()
        }
    }

    protected Iterable<LinkedHashMap> getVariablesFromArea(ODatabaseDocumentTx db,
                                                           Map params = [:],
                                                           Map optionalParams = [:],
                                                           String className = this.className) {
        OrientGraph graph = new OrientGraph(db)
        def area
        def areaId = optionalParams.id
        def areas = []
        def devices = []

        if (areaId >= 0) {
            def clusterId = this.getClusterId(db, 'Area')
            def rid = new ORecordId(clusterId, areaId.toLong())
            area = graph.getVertex(rid)
            if (!area) {
                throw new ResponseErrorException(ResponseErrorCode.AREA_NOT_FOUND,
                        404,
                        "Area with id [" + areaId + "] not found!",
                        "The area does not exist")
            }
        }

        areas.add(area)
        while (areas) {
            def deeperAreas = []
            areas.each {
                it.getVertices(Direction.OUT).each {
                    if (it.getLabel() == 'Area')
                        deeperAreas.add(it)
                    else if (it.getLabel() == 'Resource')
                        devices.add(it)
                }
            }
            areas = deeperAreas
        }

        devices.collect{
            this.getVariables(db,params,[id:it.getIdentity().clusterPosition], this.className)
        }.sum().unique()
    }

    protected Iterable<LinkedHashMap> getVariablesFromGroup(ODatabaseDocumentTx db,
                                                           Map params = [:],
                                                           Map optionalParams = [:],
                                                           String className = this.className) {

        OrientGraph graph = new OrientGraph(db)
        def group
        def groupId = optionalParams.id
        def groups = []
        def devices = []

        if (groupId >= 0) {
            def clusterId = this.getClusterId(db, 'Group')
            def rid = new ORecordId(clusterId, groupId.toLong())
            group = graph.getVertex(rid)
            if (!group) {
                throw new ResponseErrorException(ResponseErrorCode.GROUP_NOT_FOUND,
                        404,
                        "Group with id [" + groupId + "] not found!",
                        "The group does not exist")
            }
        }

        groups.add(group)
        while (groups) {
            def deeperGroups = []
            groups.each {
                it.getVertices(Direction.OUT).each {
                    if (it.getLabel() == 'Group')
                        deeperGroups.add(it)
                    else if (it.getLabel() == 'Resource')
                        devices.add(it)
                }
            }
            groups = deeperGroups
        }

        devices.collect{
            this.getVariables(db,params,[id:it.getIdentity().clusterPosition], this.className)
        }.sum().unique()
    }

    protected Iterable<LinkedHashMap> getVariables(ODatabaseDocumentTx db,
                                                   Map params = [:],
                                                   Map optionalParams = [:],
                                                   String className = this.className) {

        OrientGraph graph = new OrientGraph(db)
        OrientVertex parent
        def networkId = optionalParams.id
        def variableId = optionalParams.variableId

        if (networkId >= 0) {
            def clusterId = this.getClusterId(db, 'Resource')
            def rid = new ORecordId(clusterId, networkId.toLong())
            parent = graph.getVertex(rid)
            if (!parent) {
                throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                        404,
                        "Device with id [" + networkId + "] not found!",
                        "The device does not exist")
            }
        }

        parent.getVertices(Direction.OUT, "CanMeasure").collect {
            def devId = it.getIdentity().getClusterPosition()
            if (variableId == null || devId == variableId) {
                def variableMap = this.orientTransformer.fromOVertex(it)
                variableMap.put("id", (Endpoints.ridToUrl(it.getIdentity())))
                variableMap
            }
            else null
        } - [null]
    }

    protected Iterable<LinkedHashMap> get(ODatabaseDocumentTx db,
                                          Map params = [:],
                                          Map optionalParams = [:],
                                          String className = this.className) {

        OrientGraph graph = new OrientGraph(db)
        OrientVertex parent
        OrientVertex variable
        Date beginTimestamp = params.beginTimestamp
        Date endTimestamp = params.endTimestamp
        String granularity = params.granularity
        def pageField = params.pageField
        def pageLimitField = params.pageLimitField
        def networkId = optionalParams.id
        def variableId = optionalParams.variableId

        if (beginTimestamp >= endTimestamp) {
            throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                    400,
                    "Invalid timestamps [" + beginTimestamp + ',' + endTimestamp + "]!",
                    "endTimestamp must be greater than beginTimestamp")
        }

        if (networkId >= 0) {
            def clusterId = this.getClusterId(db, 'Resource')
            def rid = new ORecordId(clusterId, networkId.toLong())
            parent = graph.getVertex(rid)
            if (!parent) {
                throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                        404,
                        "Device with id [" + networkId + "] not found!",
                        "The device does not exist")
            }
        }

        if (variableId >= 0) {
            def clusterId = this.getClusterId(db, 'MeasurementVariable')
            def rid = new ORecordId(clusterId, variableId.toLong())
            variable = graph.getVertex(rid)
            if (!variable) {
                throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                        404,
                        "Variable [" + variableId + "] not found!",
                        "The variable does not exist")
            }
        }

        ODocument measurements = parent.getProperty('measurements').getRecord()
        List pageRange = [(pageLimitField*pageField),(pageLimitField*pageField)+pageLimitField]

       return SearchHelpers.DFSMeasurementFinder(measurements,
                                                 beginTimestamp,
                                                 endTimestamp,
                                                 granularity,
                                                 pageRange,
                                                 variable.getIdentity())
    }

    protected final LinkedHashMap generateDocumentProperties(ODatabaseDocumentTx db,
                                                             HashMap data,
                                                             HashMap optionalData = [:]) {
        def dateConverter = new DatatypeConverter()
        def timestamp = data.timestamp
        def value = data.value
        def measurementVariableUrl = data.measurementVariable
        def measurementVariableVertex

        try {
            value = value.toFloat()
        }
        catch (NumberFormatException|MissingMethodException e1) {
            throw new ResponseErrorException(ResponseErrorCode.INVALID_MEASUREMENT_VALUE,
                    400,
                    "[" + value + "] cannot be converted to Float!",
                    "Choose a Float compatible data type.")
        }

        if (measurementVariableUrl && !measurementVariableUrl.isEmpty()) {
            measurementVariableVertex = getVertexByUrl(db, measurementVariableUrl)

            if (measurementVariableVertex) {
                if (measurementVariableVertex.getLabel() != 'MeasurementVariable')
                    throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                            400,
                            "[" + measurementVariableUrl + "] is not a valid"
                                    +" id for a measurement variable!",
                            "Choose an id for an area instead")
            }
            else {
                vertexNotFoundById(Endpoints.urlToRid(new URL(measurementVariableUrl)).clusterPosition)
            }
        }

        try {
            timestamp = dateConverter.parseDateTime(timestamp).getTime()
        }
        catch (IllegalArgumentException err2) {
            throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                    400,
                    "Timestamp ["+timestamp+"] is invalid!",
                    'Use the ISO 8601 format. Example: "MM-DD-YYYYThh:mm:ssZ"')
        }

        return ["timestamp"          : timestamp,
                "measurementVariable": measurementVariableVertex.getIdentity(),
                "value"              : value]
    }

    protected void generateDocumentRelations(ODatabaseDocumentTx db,
                                             ODocument record,
                                             HashMap data,
                                             HashMap optionalData = [:]) {

        OrientGraph graph = new OrientGraph(db)
        def dateConverter = new DatatypeConverter()
        def date = dateConverter.parseDateTime(data.timestamp).getTime()
        def variableRid = Endpoints.urlToRid(new URL(data.measurementVariable))
        def networkId = optionalData.networkId
        def leftBranch = []
        def rightBranch = []

        def clusterId = this.getClusterId(db, 'Resource')
        def rid = new ORecordId(clusterId, networkId.toLong())
        def parent = graph.getVertex(rid)

        if (!parent) {
            throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                    404,
                    "Device with id [" + networkId + "] not found!",
                    "The device does not exist")
        }

        def variableVertex = graph.getVertex(variableRid)
        def resourceVertices = parent.getVertices(Direction.OUT,"CanMeasure").toList()

        if (!(variableVertex in resourceVertices)) {

            def edge = graph.addEdge(null,parent,variableVertex,"CanMeasure")
            graph.commit()
            def parentDoc = db.getRecord(parent)
            def resourceList = parentDoc.field("out_CanMeasure")
            if(resourceList)
                parentDoc.field("out_CanMeasure",resourceList+[edge])
            else
                parentDoc.field("out_CanMeasure",[edge])
            parentDoc.save()
            db.commit()
        }

        def dateBuilder = { granularity ->

                def granularityValue = Granularity.valueOf(granularity.toUpperCase()+'S')
                def newDate = new Date('01/01/01 00:00:00')
                if (granularityValue >= Granularity.YEARS)
                    newDate.year = date.year
                if (granularityValue >= Granularity.MONTHS)
                    newDate.month = date.month
                if (granularityValue >= Granularity.DAYS)
                    newDate.date = date.date
                if (granularityValue >= Granularity.HOURS)
                    newDate.hours = date.hours
                if (granularityValue >= Granularity.MINUTES)
                    newDate.minutes = date.minutes
                newDate
        }

        def initiateAggregationNode = { String mapName,
                                        String granularity,
                                        newDataStructure ->

                def newLog = new ODocument('Log')
                        .field('sum', new LinkedHashMap())
                        .field('mean', new LinkedHashMap())
                        .field('timestamp', dateBuilder(granularity))

                new ODocument(granularity)
                        .field(mapName, newDataStructure)
                        .field('log',newLog)
                        .save()
        }

        def returnValidRecord = { ODocument lastRecord,
                                  String currentGranularity,
                                  String nextGranularity,
                                  currentDate ->

                def currentMap = lastRecord.field(currentGranularity.toLowerCase())
                def currentRecord = currentMap.getAt(currentDate)

                db.begin()
                if (currentRecord == null) {
                    ODocument newRecord = initiateAggregationNode(
                            nextGranularity.toLowerCase(),
                            currentGranularity,
                            new LinkedHashMap()
                    )

                    if (rightBranch.size()==0) {
                        def lastMeasurement = currentMap.sort({
                                                    a, b -> b.key.toInteger() <=> a.key.toInteger()
                                                }).find({ it.key.toInteger() < currentDate })
                        if (lastMeasurement)
                            rightBranch.add(lastMeasurement.value.getRecord())
                    }
                    currentMap.put(currentDate, newRecord)
                    currentRecord = newRecord
                    newRecord.save()
                    leftBranch.add(newRecord)
                }
                db.commit()
                currentRecord
        }

        def measurementsRecord = parent.getProperty('measurements').getRecord()
        def yearRecord = returnValidRecord(measurementsRecord,'Year','Month',date.year+1900)
        def monthRecord = returnValidRecord(yearRecord,'Month','Day',date.month+1)
        def dayRecord = returnValidRecord(monthRecord,'Day','Hour',date.date)
        def hourRecord = returnValidRecord(dayRecord,'Hour','Minute',date.hours)
        def minuteRecord = returnValidRecord(hourRecord,'Minute','Sample',date.minutes)
        def sampleMap = minuteRecord.field('sample')

        if (!sampleMap[variableRid.toString()]){

            def variableEntry = new ODocument('Samples')
                    .field('variable',variableRid)
                    .field('samples',[record])
                    .save()
            db.commit()

            sampleMap.put(variableRid.toString(),variableEntry)
        }
        else {

            def samplesList = sampleMap[variableRid.toString()].field('samples')
            samplesList.add(0,record)
            sampleMap[variableRid].field('samples',samplesList)
            sampleMap[variableRid].save()
        }

        if (rightBranch.size()>0) {

            leftBranch = leftBranch.reverse()
            def dates = ['year','month','day','hour','minute']
            def rightDate = rightBranch.first()
            def granularity

            if (leftBranch.size()>1) {
                (0..(leftBranch.size() - 2)).each {
                    granularity = rightDate.toMap().keySet().find({ key -> key in dates })
                    rightDate = rightDate.field(granularity)
                                .sort({ a, b -> b.key <=> a.key })
                                .entrySet().first().value.getRecord()
                    rightBranch.add(rightDate)
                }
            }

            rightBranch = rightBranch.reverse()
            leftBranch.eachWithIndex {
                leftMeasurement ,i ->
                    leftMeasurement.field('lastMeasurement',rightBranch[i])
                    leftMeasurement.save()
            }
        }

        [minuteRecord,hourRecord,
         dayRecord,monthRecord,
         yearRecord,measurementsRecord].each {
            it.save()
        }
    }
}