package hooks

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

    public void onRecordAfterUpdate( ODocument document ) {
        def className = document.getClassName()

        if(document.field('lastMeasurement')) {
            def lastMeasurement = document.field('lastMeasurement')
            def lastLog = lastMeasurement.field('log')
            def lastLogSum = lastLog.field('sum')
            def lastLogMean = lastLog.field('mean')
            def measurementVariables
            def newAggregation = [:]

            if (className == "Minute") {
                measurementVariables = lastMeasurement.field('sample').collect {
                    sample ->
                        def variable = sample.field('measurementVariable')
                        def variableId = variable.getIdentity().toString()
                        if(newAggregation.getAt(variableId).is(null))
                            newAggregation.put(variableId,[])
                        newAggregation.getAt(variableId).add(sample.field('value'))
                        variable
                }
                measurementVariables.each{
                    variable ->
                        def variableId = variable.getIdentity().toString()
                        def newSum = new ODocument('Sample')
                        newSum.field('value', newAggregation[variableId].sum())
                        newSum.save()
                        lastLogSum.put(variableId,newSum)
                        def newMean = new ODocument('Sample')
                        newMean.field('value', this.mean(newAggregation[variableId]))
                        newMean.save()
                        lastLogMean.put(variableId, newMean)
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
                        }
                }
            }
            lastLog.field('sum',lastLogSum)
            lastLog.field('mean',lastLogMean)
            lastLog.save()
        }
    }

}
