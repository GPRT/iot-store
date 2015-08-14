package databaseInterfacer

import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.id.ORecordId
import java.util.Date

class MeasurementInterfacer extends DocumentInterfacer {
    enum Granularity {
        YEARS,MONTHS,DAYS,HOURS,MINUTES
    }

    def MeasurementInterfacer(factory) {
        super(factory, "Sample",
                ["timestamp": "timestamp",
                 "measurementVariable": "measurementVariable",
                 "value": "value"],
                [:])
    }

    void invalidDocumentProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid measurement properties!",
                "The valid ones are " + this.fields)

    }

    void documentNotFoundById() {
        throw new ResponseErrorException(ResponseErrorCode.MEASUREMENT_NOT_FOUND,
                404,
                "Measurement [" + id + "] was not found!",
                "The measurement does not exist")
    }

    protected Iterable<LinkedHashMap> get(Set fieldNames, Set filterFields=[], Set sortFields=[],
                                          int pageField=0, int pageLimitField=10, String className=this.className,
                                          networkId=null, beginTimestamp=null,
                                          endTimestamp=null, granularity=null) {
        def graph = factory.getNoTx()
        def db = graph.getRawGraph()
        def parent = null

        if(beginTimestamp >= endTimestamp){
            throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                    404,
                    "Invalid timestamps [" + beginTimestamp +',' + endTimestamp + "]!",
                    "endTimestamp must be greater than beginTimestamp")
        }

        if (networkId){
            def clusterId = this.getClusterId('Resource')
            def rid = new ORecordId(clusterId, networkId.toLong())
            parent = graph.getVertex(rid)
            if(!parent) {
                throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                        404,
                        "Device with id [" + networkId + "] not found!",
                        "The device does not exist")
            }
        }

        def findSubSet = {
            set,beginRange,endRange,gran,rangeSize ->
                def range = new ArrayList<ODocument>()
                if(set.size() > 1) {
                    (beginRange..rangeSize).collect{
                        if(set.first().field(gran).getAt(it))
                            range.add(set.first().field(gran).getAt(it))
                    }
                    if(set.size() > 2) {
                        (1..set.size()-2).collect {
                            setIter ->
                            (0..rangeSize).collect {
                                if(set[setIter].field(gran).getAt(it))
                                    range.add(set[setIter].field(gran).getAt(it))
                            }
                        }
                    }
                    (1..endRange).collect {
                        if(set.last().field(gran).getAt(it))
                            range.add(set.last().field(gran).getAt(it))
                    }
                }
                else{
                    (beginRange..endRange).collect {
                        if(set.first().field(gran).getAt(it)) {
                            range.add(set.first().field(gran).getAt(it))
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
            def result = []
            def measurements = parent.getProperty('measurements').getRecord()

            ArrayList<ODocument> years = (begin.year+1900..end.year+1900).collect{
                measurements.field('year').getAt(it)
            }

            if(granularityValue >= Granularity.MONTHS) {
                def months = findSubSet(years,
                        begin.month + 1, end.month + 1, 'month',12)
                if (granularityValue >= Granularity.DAYS) {
                    def days = findSubSet(months,
                            begin.date, end.date,'day',31)
                    if (granularityValue >= Granularity.HOURS) {
                        def hours = findSubSet(days,
                                begin.hours, end.hours, 'hour', 23)
                        if (granularityValue >= Granularity.MINUTES) {
                            def minutes = findSubSet(hours,
                                    begin.minutes, end.minutes, 'minute', 59)
                            minutes.each{
                                min -> min.field('sample').each {
                                    result.add(it)
                                }
                            }
                        }
                        else result = hours
                    }
                    else result = days
                }
                else result = months
            }
            else result = years

            result.collect {
                this.orientTransformer.fromODocument(it)
            }
        }
        finally {
            db.close()
        }
    }

    protected final LinkedHashMap generateDocumentProperties(HashMap data) {
        def timestamp = data.timestamp
        def value = data.value
        def measurementVariable = data.measurementVariable
        def graph = factory.getTx()
        def measurementVariableVertex = null

        try {
            measurementVariableVertex = graph.getVertices('MeasurementVariable.name',
                                                            measurementVariable).last()
            timestamp = new Date(timestamp)
        }
        catch(NoSuchElementException err1){
            throw new ResponseErrorException(ResponseErrorCode.VARIABLE_NOT_FOUND,
                    404,
                    "Variable [" + measurementVariable + "] was not found!",
                    "The area does not exist")
        }
        catch(IllegalArgumentException err2){
            throw new ResponseErrorException(ResponseErrorCode.INVALID_TIMESTAMP,
                    400,
                    "Timestamp ["+timestamp+"] is invalid!",
                    'Possible format "MM/DD/YYYY hh:mm:ss"')
        }
        finally{
            graph.shutdown()
        }

        return ["timestamp": timestamp,
                "measurementVariable": measurementVariableVertex.getId(),
                "value":value]
    }

    protected void generateDocumentRelations(ODocument record, HashMap data, OrientVertex parent) {
        def timestamp = data.timestamp
        def date = new Date(timestamp)

        def measurementsRecord = parent.getProperty('measurements').getRecord()
        def yearMap = measurementsRecord.field('year')
        def yearRecord = yearMap.getAt(date.year + 1900)
        def db = factory.getTx().getRawGraph()

        try {
            db.begin()
            if (yearRecord == null) {
                def newYearRecord = new ODocument('Year')
                newYearRecord.field('month', new LinkedHashMap())
                newYearRecord.save()
                measurementsRecord.field('year').put(date.year + 1900, newYearRecord)
                yearRecord = newYearRecord
            }

            def monthMap = yearRecord.field('month')
            def monthRecord = monthMap.getAt(date.month+1)

            if (monthRecord == null) {
                def newMonthRecord = new ODocument('Month')
                newMonthRecord.field('day', new LinkedHashMap())
                newMonthRecord.save()
                monthMap.put(date.month+1, newMonthRecord)
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
                newMinuteRecord.field('sample',new ArrayList<ODocument>())
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
        finally{
            db.close()
        }

    }
}
