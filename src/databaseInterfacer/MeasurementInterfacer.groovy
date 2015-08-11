package databaseInterfacer

import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.OTrackedList
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.record.impl.ODocument
import org.codehaus.groovy.runtime.ArrayUtil

import java.util.Date

class MeasurementInterfacer extends DocumentInterfacer {

    def MeasurementInterfacer(factory) {
        super(factory, "Sample", ["networkId","timestamp","measurementVariable","value"])
    }

    void invalidDocumentProperties() {
        throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                400,
                "Invalid measurement properties!",
                "The valid ones are " + this.fields)

    }

    protected final void updateMeasurementTree(ODocument record, OrientVertex resource, Date date){

        def measurementsRecord = resource.getProperty('measurements').getRecord()
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
                measurementsRecord.save()
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
            db.commit()
        }
        finally{
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

    protected void generateDocumentRelations(ODocument record, HashMap data) {
        def graph = factory.getTx()
        def networkId = data.networkId
        def timestamp = data.timestamp
        try {
            def resource = graph.getVertices('Resource.networkId', data.networkId).last()
            updateMeasurementTree(record, resource, new Date(timestamp))
        }
        catch(NoSuchElementException e){
            throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                    404,
                    "Device [" + networkId + "] was not found!",
                    "The device does not exist")
        }
        finally {
            graph.shutdown()
        }
    }
}
