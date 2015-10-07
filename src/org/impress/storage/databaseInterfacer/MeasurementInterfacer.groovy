package org.impress.storage.databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OValidationException
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.id.ORecordId
import org.impress.storage.utils.Endpoints
import org.impress.storage.utils.SearchHelpers

class MeasurementInterfacer extends DocumentInterfacer {
    enum Granularity {
        YEARS, MONTHS, DAYS, HOURS, MINUTES, SAMPLES
    }

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

            return this.orientTransformer.fromODocument(document)
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
                def sumMerge = [:]
                it.value.sum.collect {
                    it.collect { k, v -> ["$k": v] }
                }
                .sum().collect {
                    def key = (it.keySet()[0]).toString()
                    if (sumMerge[key] == null)
                        sumMerge[key] = 0.0
                    sumMerge[key] += it.values()[0]
                }
                sumMerge
                def meanMerge = [:]
                it.value.mean.collect {
                    it.collect { k, v -> ["$k": v] }
                }
                .sum().collect {
                    def key = (it.keySet()[0]).toString()
                    if (meanMerge[key] == null)
                        meanMerge[key] = []
                    meanMerge[key].add(it.values()[0])
                }
                meanMerge.each { k, v -> meanMerge[k] = mean(v) }

                [sum      : sumMerge,
                 mean     : meanMerge,
                 timestamp: it.key]
            }
        }
        else {
            measurementPipe.sum()
        }
    }

    protected Iterable<LinkedHashMap> getVariables(ODatabaseDocumentTx db,
                                                   Map params = [:],
                                                   Map optionalParams = [:],
                                                   String className = this.className) {
        OrientGraph graph = new OrientGraph(db)
        OrientVertex parent
        def networkId = optionalParams.id

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

        parent.getVertices(Direction.OUT,"CanMeasure").collect{
            this.orientTransformer.fromOVertex(it)
        }
    }
    protected Iterable<LinkedHashMap> get(ODatabaseDocumentTx db,
                                          Map params = [:],
                                          Map optionalParams = [:],
                                          String className = this.className) {
        OrientGraph graph = new OrientGraph(db)
        OrientVertex parent
        OrientVertex variable
        def beginTimestamp = params.beginTimestamp
        def endTimestamp = params.endTimestamp
        def granularity = params.granularity
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

        def measurements = parent.getProperty('measurements').getRecord()
        def pageRange = [(pageLimitField*pageField),(pageLimitField*pageField)+pageLimitField]
        def results = SearchHelpers.DFSMeasurementFinder(measurements,beginTimestamp,endTimestamp,
                                                         granularity.toString(),pageRange,
                                                         variable.getIdentity())
        return results
    }

    protected final LinkedHashMap generateDocumentProperties(ODatabaseDocumentTx db,
                                                             HashMap data,
                                                             HashMap optionalData = [:]) {
        OrientGraph graph = new OrientGraph(db)
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
                vertexNotFoundById(rid.clusterPosition)
            }
        }

        try {
            timestamp = new Date(timestamp)
        }
        catch (IllegalArgumentException err2) {
            throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                    400,
                    "Timestamp [" + timestamp + "] is invalid!",
                    'Possible format "MM/DD/YYYY hh:mm:ss"')
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
        def timestamp = data.timestamp
        def variableRid = Endpoints.urlToRid(new URL(data.measurementVariable))
        def date = new Date(timestamp)
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
        if (!(variableVertex in resourceVertices))
            parent.addEdge("CanMeasure",variableVertex)

        def dateBuilder = {
            granularity ->
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

        def initiateAggregationNode = {
            ODocument newRecord, String mapName,
            newDataStructure, granularity ->
                def newLog = new ODocument('Log')
                newLog.field('sum', new LinkedHashMap())
                newLog.field('mean', new LinkedHashMap())
                newLog.field('timestamp', dateBuilder(granularity))
                newLog.save()
                db.commit()
                newRecord.field(mapName, newDataStructure)
                newRecord.field('log',newLog)
                newRecord.save()
        }

        def returnValidRecord = {
            lastRecord, currentGranularity, nextGranularity, currentDate ->

                def currentMap = lastRecord.field(currentGranularity.toLowerCase())
                def currentRecord = currentMap.getAt(currentDate)

                db.begin()
                if (currentRecord == null) {
                    def newRecord = new ODocument(currentGranularity)
                    initiateAggregationNode(newRecord, nextGranularity.toLowerCase(),
                            new LinkedHashMap(), currentGranularity)

                    if (rightBranch.size()==0) {
                        def lastMeasurement = currentMap
                                .sort({ a, b -> b.key <=> a.key })
                                .find({ it.key.toInteger() < currentDate })
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
            variableEntry.field('variable',variableRid)
            variableEntry.field('samples',[record])
            variableEntry.save()
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
        db.commit()
    }
}