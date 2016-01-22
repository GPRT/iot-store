package org.impress.storage.scripts

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientGraph

ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/iot").open("root","root")

try {
    def granularityHierarchy = ['Year': 'month', 'Month': 'day', 'Day': 'hour', 'Hour': 'minute']
    def mean = { it.sum() / it.size() }

    db.browseClass("HOUR").each{
        ODocument document ->

        if(!document.field('log').field('sum').isEmpty()) {
            def className = document.getClassName()

            def lastLog = document.field('log')
            def lastLogSum = lastLog.field('sum')
            def lastLogMean = lastLog.field('mean')
            def measurementVariables
            def newAggregation = [:]

            newAggregation.put('sum', [:])
            newAggregation.put('mean', [:])
            def granularity = granularityHierarchy.find({ k, v -> k == className }).value

            measurementVariables = document.field(granularity)
                    .entrySet().first().value.getRecord()
                    .field('log').field('sum').keySet()

            measurementVariables.collect {
                variableName ->
                    newAggregation['sum'].put(
                            variableName,
                            (document.field(granularity).collect {
                                it.value.collect {
                                    it.getRecord().field('log').field('sum').getAt(variableName)
                                }
                            }.sum() - [null]).sum {
                                it.field('value')
                            }
                    )
                    newAggregation['mean'].put(
                            variableName,
                            mean((document.field(granularity).collect {
                                it.value.collect {
                                    it.getRecord().field('log').field('mean').getAt(variableName)
                                }
                            }.sum() - [null]).collect {
                                it.field('value')
                            })
                    )
                    ['sum': lastLogSum, 'mean': lastLogMean].each {
                        function, map ->
                            def newSample = new ODocument('Sample')
                            newSample.field('value', newAggregation[function][variableName])
                            newSample.save()
                            map.put(variableName, newSample)
                            db.commit()
                    }

            }
            lastLog.field('sum', lastLogSum)
            lastLog.field('mean', lastLogMean)
            lastLog.save()
            document.save()
            db.commit()
        }
    }
}
finally{
    db.close()
}