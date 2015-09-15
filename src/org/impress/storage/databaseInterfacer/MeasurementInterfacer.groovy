package org.impress.storage.databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.id.ORecordId
import org.impress.storage.utils.Endpoints

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
            this.get(db, params, [id: it.getId().getClusterPosition()], className)
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

    protected Iterable<LinkedHashMap> get(ODatabaseDocumentTx db,
                                          Map params = [:],
                                          Map optionalParams = [:],
                                          String className = this.className) {
        OrientGraph graph = new OrientGraph(db)
        OrientVertex parent
        def beginTimestamp = params.beginTimestamp
        def endTimestamp = params.endTimestamp
        def granularity = params.granularity
        def measurementVariables = params.measurementVariables
        def pageField = params.pageField
        def pageLimitField = params.pageLimitField
        def networkId = optionalParams.id
        def measurementRange = [begin:(pageField*pageLimitField),
                                end:(pageField*pageLimitField + pageLimitField)]
        def variablesURLS = []
        def variablesRids = []

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

        for (variableURL in measurementVariables) {
            def variableRid
            try {
                variableRid = Endpoints.urlToRid(new URL(variableURL))
            }
            catch (NullPointerException e){
                throw new ResponseErrorException(ResponseErrorCode.INVALID_MEASUREMENT_VARIABLE,
                400,
                "MeasurementVariables is invalid",
                "URL ["+variableURL+"] is malformed.")
            }
            catch (MalformedURLException e){
                throw new ResponseErrorException(ResponseErrorCode.INVALID_MEASUREMENT_VARIABLE,
                        400,
                        "MeasurementVariables is invalid",
                        "URL ["+variableURL+"] is malformed.")
            }

            if (variableRid.getRecord()) {
                variablesURLS.add(variableURL.toString())
                variablesRids.add(variableRid.toString())
            } else {
                throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                        404,
                        "Measurement variable [" + variableURL + "] was not found!",
                        "The measurement variable does not exist")
            }
        }

        def findSubSet = {
            measurementSet, beginRange, endRange, granularityLevel, rangeSize ->
                def range = new ArrayList<ODocument>()
                def validateAndAdd = {
                    setIndex, mapIndex ->
                    if (measurementSet[setIndex].field(granularityLevel)[mapIndex])
                        range.add(measurementSet[setIndex].field(granularityLevel)[mapIndex])
                }

                if (measurementSet.size() == 1){
                    (beginRange..rangeSize).collect {
                        validateAndAdd(0,it)
                    }
                }
                else if (measurementSet.size() >= 2){
                    (beginRange..rangeSize).collect {
                        validateAndAdd(0,it)
                    }
                    if (measurementSet.size() > 2) {
                        (1..measurementSet.size() - 2).collect {
                            setIter ->
                                (0..rangeSize).collect {
                                    validateAndAdd(setIter,it)
                                }
                        }
                    }
                    (1..endRange).collect {
                        validateAndAdd(measurementSet.size()-1,it)
                    }
                }
                return range
        }

        def granularityValue = Granularity.valueOf(granularity.toString())
        def begin = beginTimestamp
        def end = endTimestamp
        def results = []
        def measurements = parent.getProperty('measurements').getRecord()

        def yearRange = (begin.year + 1900..end.year + 1900)
        def yearMap = measurements.field('year')
        ArrayList<ODocument> years = yearRange.collect {
            yearMap.getAt(it)
        } - [null]

        if (years.size() >= 1) {
            if (begin.year+1900 < yearMap.keySet()[0].toInteger())
                begin = new Date("01/01/2000 00:00:00")
            if (end.year+1900 < yearMap.keySet().last().toInteger())
                end = new Date("12/31/2000 23:59:59")
        }

        if (granularityValue >= Granularity.MONTHS) {
            def months = findSubSet(years, begin.month + 1, end.month + 1, 'month', 12) - [null]
            if (granularityValue >= Granularity.DAYS) {
                def days = findSubSet(months, begin.date, end.date, 'day', 31) - [null]
                if (granularityValue >= Granularity.HOURS) {
                    def hours = findSubSet(days, begin.hours, end.hours, 'hour', 23) - [null]
                    if (granularityValue >= Granularity.MINUTES) {
                        def minutes = findSubSet(hours, begin.minutes, end.minutes, 'minute', 59) - [null]
                        if (granularityValue >= Granularity.SAMPLES) {
                            minutes.each {
                                min ->
                                    min.field('sample').each {
                                        results.add(it)
                                    }
                            }
                        }
                        else
                            results = minutes
                    } else
                        results = hours
                } else
                    results = days
            } else
                results = months
        } else
            results = years

        def pageIndex = -1
        if (granularityValue > Granularity.MINUTES) {
            def json
            def samples = []
            results.collect {
                result ->
                    pageIndex+=1
                    if (pageIndex >= measurementRange.begin
                            && pageIndex <= measurementRange.end) {
                        json = this.orientTransformer.fromODocument(result)
                        if(variablesURLS.isEmpty() ||
                                json.measurementVariable.toString() in variablesURLS)
                            samples.add(json)
                        else
                            pageIndex -= 1
                    }
            }
            samples.sort { a,b -> b.timestamp <=> a.timestamp }
        }
        else {
            results.collect {
                result ->
                    pageIndex += 1
                    if (pageIndex >= measurementRange.begin
                            && pageIndex <= measurementRange.end) {
                        def resultMap = [sum      : [:],
                                         mean     : [:],
                                         timestamp: result.field('log').field('timestamp')]

                        ['sum', 'mean'].collect {
                            def variables = [:]
                            result.field('log').field(it).collect {
                                key, value ->
                                    if (variablesRids.isEmpty() || (key in variablesRids)) {
                                        if (!variables.getAt(key))
                                            variables.put(key, Endpoints.ridToUrl(new ORecordId(key)))
                                        resultMap.getAt(it).put(
                                                variables.getAt(key),
                                                value.getRecord().field('value'))
                                    }
                            }
                        }
                        if (resultMap.sum)
                            resultMap
                        else {
                            pageIndex -= 1
                            null
                        }
                    }
            } - [null]
        }
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
                    if (currentGranularity == 'Minute')
                        initiateAggregationNode(newRecord, nextGranularity.toLowerCase(),
                                new ArrayList<ODocument>(), currentGranularity)
                    else
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
        minuteRecord.field('sample').add(record)

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