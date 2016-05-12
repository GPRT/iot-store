package org.impress.storage.utils

import com.orientechnologies.orient.core.db.record.ORecordLazyMap
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import groovy.transform.CompileStatic
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException
import org.impress.storage.utils.Granularity

class SearchHelpers {

    static final Iterable<LinkedHashMap> BFSMeasurementFinder(ODocument measurements,
                                                              Date beginTimestamp,
                                                              Date endTimestamp,
                                                              String granularity) {

       throw new  ResponseErrorException(
               ResponseErrorCode.NOT_IMPLEMENTED,
               404,
               "",
               "This method needs to be implemented!"
       )
    }

    static final Iterable<LinkedHashMap> DFSMeasurementFinder(ODocument measurements,
                                                              Date beginTimestamp,
                                                              Date endTimestamp,
                                                              String granularity,
                                                              List pageRange,
                                                              ORecordId variableRid) {

        def granularityValue = Granularity.valueOf(granularity)
        def results = []
        def resultsSize = 0
        def pageIndex = 0
        def pageSize = pageRange[1] - pageRange[0]
        def yearMap = measurements.field('year')

        def granularityHierarchy = ['Year': 'Month',
                                    'Month': 'Day',
                                    'Day':'Hour',
                                    'Hour':'Minute',
                                    'Minute':'Sample']

        def endDateMap = ['Year'  : endTimestamp.year + 1900,
                          'Month' : endTimestamp.month + 1,
                          'Day'   : endTimestamp.date,
                          'Hour'  : endTimestamp.hours,
                          'Minute': endTimestamp.minutes]

        def (reachedFirst, reachedLast) = [false,false]

        Closure findSubSet
        findSubSet = { node, String granularityBelow ->

                if ((resultsSize < pageSize) && !reachedLast) {

                    Granularity lowerGranValue = Granularity.valueOf((granularityBelow + 's').toUpperCase())
                    def measurementPipe = node.getRecord().field(granularityBelow.toLowerCase())

                    if (granularityBelow != 'Sample')
                        measurementPipe = measurementPipe.sort { a, b ->
                            b.key.toInteger() <=> a.key.toInteger()
                        }
                    else
                        measurementPipe = measurementPipe[variableRid].field('samples')

                    measurementPipe.each { nodeInfo ->

                        if (granularityBelow != 'Sample') {

                            if ((reachedFirst || nodeInfo.key.toInteger() <=
                                    endDateMap[granularityBelow]) &&
                                    !reachedLast) {

                                ODocument lowerNode = nodeInfo.value.getRecord()

                                if (lowerGranValue < granularityValue)
                                    findSubSet(lowerNode, granularityHierarchy[granularityBelow])
                                else {

                                    reachedFirst = true
                                    if (pageIndex >= pageRange[0] && pageIndex < pageRange[1]) {

                                        ODocument log = lowerNode.field('log').getRecord()
                                        if (log.field('sum')) {
                                            HashMap resultMap = [:]

                                            ODocument sum = log.field('sum')[variableRid]
                                            ODocument mean = log.field('mean')[variableRid]

                                            if (sum == null || mean == null)
                                                return null

                                            resultMap.put("value",
                                                    ["sum": sum.field('value'),
                                                     "mean":mean.field('value')])

                                            resultsSize += 1
                                            pageIndex += 1

                                            Date timestamp = log.field('timestamp')
                                            if(timestamp <= beginTimestamp)
                                                reachedLast = true

                                            resultMap.put('timestamp',timestamp.format("yyyy-MM-dd'T'HH:mm:ssX"))
                                            results.add(resultMap)
                                        }
                                        else null
                                    }
                                    else pageIndex += 1
                                }
                            }
                        }
                        else {

                            reachedFirst = true
                            if (pageIndex >= pageRange[0] && pageIndex < pageRange[1]) {

                                ODocument sampleRecord = nodeInfo.getRecord()
                                Date timestamp = sampleRecord.field('timestamp')

                                if (timestamp <= beginTimestamp)
                                    reachedLast = true

                                resultsSize += 1
                                pageIndex += 1

                                results.add([value    : sampleRecord.field('value'),
                                             timestamp: timestamp.format("yyyy-MM-dd'T'HH:mm:ssX")])


                            }
                            else pageIndex += 1
                        }
                    }
                }
                else return
        }

        yearMap.each { index, year ->

            int endYear = endTimestamp.year+1900

            if (index.toInteger() < endYear)
                endTimestamp = new Date('23:59:59 12/31/'+(endYear).toString())

            if (index.toInteger() <= endYear)
                findSubSet(year,'Month')
        }

        return results
    }
}
