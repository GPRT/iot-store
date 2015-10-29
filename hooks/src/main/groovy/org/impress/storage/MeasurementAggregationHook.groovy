package org.impress.storage

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract
import com.orientechnologies.orient.core.hook.ORecordHook
import com.orientechnologies.orient.core.record.impl.ODocument
import java.util.Date

public class MeasurementAggregationHook extends ODocumentHookAbstract implements ORecordHook {
    def granularityHierarchy = ['Year':'month','Month':'day','Day':'hour','Hour':'minute']
    def mean = { it.sum()/it.size() }

    public MeasurementAggregationHook() {
        setIncludeClasses("Year","Month","Day","Hour","Minute")
    }

    @Override
    public ORecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return ORecordHook.DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterUpdate( ODocument document ) {
        def className = document.getClassName()
        def db = ODatabaseRecordThreadLocal.INSTANCE.get();

        if(document.field('lastMeasurement')) {
            def lastMeasurement = document.field('lastMeasurement')
            def lastLog = lastMeasurement.field('log')
            def lastLogSum = lastLog.field('sum')
            def lastLogMean = lastLog.field('mean')
            def measurementVariables
            def newAggregation = [:]

            if (className == "Minute") {
                lastMeasurement.field('sample').each{
                     variable, samples ->
                         def samplesList = samples.getRecord().field('samples')
                         def newSum = new ODocument('Sample')
                         newSum.field('value', samplesList.sum{ it.field('value') })
                         newSum.save()
                         db.commit()
                         lastLogSum.put(variable,newSum)
                         def newMean = new ODocument('Sample')
                         newMean.field('value', this.mean(samplesList.collect{it.field('value')}))
                         newMean.save()
                         lastLogMean.put(variable, newMean)
                         db.commit()
                }
            }
            else {
                newAggregation.put('sum',[:])
                newAggregation.put('mean',[:])
                def granularity = granularityHierarchy.find( {k,v -> k == className} ).value

                measurementVariables = lastMeasurement.field(granularity)
                        .entrySet().first().value.getRecord()
                        .field('log').field('sum').keySet()

                measurementVariables.collect {
                    variableName ->
                        newAggregation.put(
                                'sum',
                                (lastMeasurement.field(granularity).collect {
                                    it.value.collect{
                                        it.getRecord().field('log').field('sum').getAt(variableName)
                                    }
                                }.sum() - [null]).sum{it.field('value')}
                        )
                        newAggregation.put(
                                'mean',
                                this.mean((lastMeasurement.field(granularity).collect {
                                    it.value.collect{
                                        it.getRecord().field('log').field('mean').getAt(variableName)
                                    }
                                }.sum() - [null]).collect{it.field('value')})
                        )
                }
                measurementVariables.each{
                    variableName ->
                        ['sum':lastLogSum,'mean':lastLogMean].each {
                            function,map ->
                                def newSample = new ODocument('Sample')
                                newSample.field('value', newAggregation.getAt(function))
                                newSample.save()
                                map.put(variableName, newSample)
                                db.commit()
                        }
                }
            }
            lastLog.field('sum',lastLogSum)
            lastLog.field('mean',lastLogMean)
            lastLog.save()
            db.commit()
        }
    }

}
