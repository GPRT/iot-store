package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.id.ORecordId
import java.util.Date

class MeasurementInterfacer extends DocumentInterfacer {
    enum Granularity {
        YEARS, MONTHS, DAYS, HOURS, MINUTES
    }

    def MeasurementInterfacer() {
        super("Sample",
                ["timestamp"          : "timestamp",
                 "measurementVariable": "measurementVariable",
                 "value"              : "value"],
                [:])
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
                def months = findSubSet(years,
                        begin.month + 1, end.month + 1, 'month', 12)
                if (granularityValue >= Granularity.DAYS) {
                    def days = findSubSet(months,
                            begin.date, end.date, 'day', 31)
                    if (granularityValue >= Granularity.HOURS) {
                        def hours = findSubSet(days,
                                begin.hours, end.hours, 'hour', 23)
                        if (granularityValue >= Granularity.MINUTES) {
                            def minutes = findSubSet(hours,
                                    begin.minutes, end.minutes, 'minute', 59)
                            minutes.each {
                                min ->
                                    min.field('sample').each {
                                        results.add(it)
                                    }
                            }
                        } else
                            results = hours
                    } else
                        results = days
                } else
                    results = months
            } else
                results = years

            results.collect {
                def result = this.orientTransformer.fromODocument(it)
                result.put('measurementVariable', it.field('measurementVariable').field('name'))
                result
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
        def measurementVariable = data.measurementVariable
        def measurementVariableVertex = null

        try {
            measurementVariableVertex = graph.getVertices('MeasurementVariable.name',
                    measurementVariable).last()
            timestamp = new Date(timestamp)
        }
        catch (NoSuchElementException err1) {
            throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                    404,
                    "Variable [" + measurementVariable + "] was not found!",
                    "The area does not exist")
        }
        catch (IllegalArgumentException err2) {
            throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                    400,
                    "Timestamp [" + timestamp + "] is invalid!",
                    'Possible format "MM/DD/YYYY hh:mm:ss"')
        }

        return ["timestamp"          : timestamp,
                "measurementVariable": measurementVariableVertex.getId(),
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
        def parent = null

        def clusterId = this.getClusterId(db, 'Resource')
        def rid = new ORecordId(clusterId, networkId.toLong())
        parent = graph.getVertex(rid)
        if (!parent) {
            throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                    404,
                    "Device with id [" + networkId + "] not found!",
                    "The device does not exist")
        }

        def measurementsRecord = parent.getProperty('measurements').getRecord()
        def yearMap = measurementsRecord.field('year')
        def yearRecord = yearMap.getAt(date.year + 1900)

        db.begin()
        if (yearRecord == null) {
            def newYearRecord = new ODocument('Year')
            newYearRecord.field('month', new LinkedHashMap())
            newYearRecord.save()
            measurementsRecord.field('year').put(date.year + 1900, newYearRecord)
            yearRecord = newYearRecord
        }

        def monthMap = yearRecord.field('month')
        def monthRecord = monthMap.getAt(date.month + 1)

        if (monthRecord == null) {
            def newMonthRecord = new ODocument('Month')
            newMonthRecord.field('day', new LinkedHashMap())
            newMonthRecord.save()
            monthMap.put(date.month + 1, newMonthRecord)
            monthRecord = newMonthRecord
        }

        def dayMap = monthRecord.field('day')
        def dayRecord = dayMap.getAt(date.date)

        if (dayRecord == null) {
            def newDayRecord = new ODocument('Day')
            newDayRecord.field('hour', new LinkedHashMap())
            newDayRecord.save()
            dayMap.put(date.date, newDayRecord)
            dayRecord = newDayRecord
        }

        def hourMap = dayRecord.field('hour')
        def hourRecord = hourMap.getAt(date.hours)

        if (hourRecord == null) {
            def newHourRecord = new ODocument('Hour')
            newHourRecord.field('minute', new LinkedHashMap())
            newHourRecord.save()
            hourMap.put(date.hours, newHourRecord)
            hourRecord = newHourRecord
        }

        def minuteMap = hourRecord.field('minute')
        def minuteRecord = minuteMap.getAt(date.minutes)

        if (minuteRecord == null) {
            def newMinuteRecord = new ODocument('Minute')
            newMinuteRecord.field('sample', new ArrayList<ODocument>())
            newMinuteRecord.save()
            minuteMap.put(date.minutes, newMinuteRecord)
            minuteRecord = newMinuteRecord
        }
        minuteRecord.field('sample').add(record)
        minuteRecord.save()
        hourRecord.save()
        dayRecord.save()
        monthRecord.save()
        yearRecord.save()
        measurementsRecord.save()

        db.commit()
    }
}