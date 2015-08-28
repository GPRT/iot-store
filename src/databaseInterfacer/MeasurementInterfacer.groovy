package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.id.ORecordId
import java.util.Date

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

    protected Iterable<LinkedHashMap> get(ODatabaseDocumentTx db,
                                          Map params = [:],
                                          Map optionalParams = [:],
                                          String className = this.className) {
        def parent = null
        OrientGraph graph = new OrientGraph(db)

        def beginTimestamp = params.beginTimestamp
        def endTimestamp = params.endTimestamp
        def granularity = params.granularity
        def networkId = optionalParams.networkId

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

        def findSubSet = {
            measurementSet, beginRange, endRange, granularityLevel, rangeSize ->
                def range = new ArrayList<ODocument>()
                if (measurementSet.size() > 1) {
                    (beginRange..rangeSize).collect {
                        if (measurementSet.first().field(granularityLevel).getAt(it))
                            range.add(measurementSet.first().field(granularityLevel).getAt(it))
                    }
                    if (measurementSet.size() > 2) {
                        (1..measurementSet.size() - 2).collect {
                            setIter ->
                                (0..rangeSize).collect {
                                    if (measurementSet[setIter].field(granularityLevel).getAt(it))
                                        range.add(measurementSet[setIter].field(granularityLevel).getAt(it))
                                }
                        }
                    }
                    (1..endRange).collect {
                        if (measurementSet.last().field(granularityLevel).getAt(it))
                            range.add(measurementSet.last().field(granularityLevel).getAt(it))
                    }
                } else {
                    (beginRange..endRange).collect {
                        if (measurementSet.first().field(granularityLevel).getAt(it)) {
                            range.add(measurementSet.first().field(granularityLevel).getAt(it))
                        }
                    }
                }
                return range
        }

        try {
            db.begin()
            def granularityValue = Granularity.valueOf(granularity.toString())
            def begin = beginTimestamp
            def end = endTimestamp
            def results = []
            def measurements = parent.getProperty('measurements').getRecord()

            ArrayList<ODocument> years = (begin.year + 1900..end.year + 1900).collect {
                measurements.field('year').getAt(it)
            }

            if (granularityValue >= Granularity.MONTHS) {
                def months = findSubSet(years, begin.month + 1, end.month + 1, 'month', 12)
                if (granularityValue >= Granularity.DAYS) {
                    def days = findSubSet(months, begin.date, end.date, 'day', 31)
                    if (granularityValue >= Granularity.HOURS) {
                        def hours = findSubSet(days, begin.hours, end.hours, 'hour', 23)
                        if (granularityValue >= Granularity.MINUTES) {
                            def minutes = findSubSet(hours, begin.minutes, end.minutes, 'minute', 59)
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

            if (granularityValue > Granularity.MINUTES) {
                results.collect {
                    this.orientTransformer.fromODocument(it)
                }
            }
            else{
                results.collect {
                    result ->
                        def resultMap = [sum:[:],
                                         mean:[:],
                                         timestamp:result.field('log').field('timestamp')]

                        ['sum','mean'].collect {
                            result.field('log').field(it).collect {
                                key,value ->
                                    resultMap.getAt(it).put(key,
                                            this.orientTransformer.fromODocument(value.getRecord()))
                            }
                        }
                        resultMap
                }
            }
        }
        finally {
            db.close()
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

        def initiateAggregationNode = {
            ODocument newRecord, String mapName, newDataStructure ->
                def newLog = new ODocument('Log')
                newLog.field('sum', new LinkedHashMap())
                newLog.field('mean', new LinkedHashMap())
                newLog.field('timestamp', date)
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
                        initiateAggregationNode(newRecord, nextGranularity.toLowerCase(), new ArrayList<ODocument>())
                    else
                        initiateAggregationNode(newRecord, nextGranularity.toLowerCase(), new LinkedHashMap())

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