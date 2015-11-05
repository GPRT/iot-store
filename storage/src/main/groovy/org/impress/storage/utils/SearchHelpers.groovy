package org.impress.storage.utils

import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import org.impress.storage.utils.Granularity.GranularityValues

class SearchHelpers {

    static final Iterable<LinkedHashMap> BFSMeasurementFinder(ODocument measurements,
                                                         Date beginTimestamp,
                                                         Date endTimestamp,
                                                         String granularity) {
        def granularityValue = GranularityValues.valueOf(granularity)
        def begin = beginTimestamp
        def end = endTimestamp
        def results = []

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

        if (granularityValue >= InputValidator.Granularity.MONTHS) {
            def months = findSubSet(years, begin.month + 1, end.month + 1, 'month', 12) - [null]
            if (granularityValue >= InputValidator.Granularity.DAYS) {
                def days = findSubSet(months, begin.date, end.date, 'day', 31) - [null]
                if (granularityValue >= InputValidator.Granularity.HOURS) {
                    def hours = findSubSet(days, begin.hours, end.hours, 'hour', 23) - [null]
                    if (granularityValue >= InputValidator.Granularity.MINUTES) {
                        def minutes = findSubSet(hours, begin.minutes, end.minutes, 'minute', 59) - [null]
                        if (granularityValue >= InputValidator.Granularity.SAMPLES) {
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

        return results
    }

    static final Iterable<LinkedHashMap> DFSMeasurementFinder(ODocument measurements,
                                                         Date beginTimestamp,
                                                         Date endTimestamp,
                                                         String granularity,
                                                         List pageRange,
                                                         ORecordId variableRid) {
        def variableUrl = Endpoints.ridToUrl(variableRid).toString()
        def granularityValue = GranularityValues.valueOf(granularity)
        def begin = beginTimestamp
        def end = endTimestamp
        def results = []
        def resultsSize = 0
        def pageIndex = 0
        def reachedFirst = false
        def reachedLast = false
        def pageSize = pageRange[1] - pageRange[0]

        def yearRange = (begin.year + 1900..end.year + 1900)
        def yearMap = measurements.field('year')

        ArrayList<ODocument> years = yearRange.collect {
            yearMap.getAt(it)
        } - [null]

        def granularityHierarchy = ['Year':'Month','Month':'Day','Day':'Hour','Hour':'Minute','Minute':'Sample']
        def granularityToDate = {
            date ->
                ['Year'  : date.year + 1900,
                 'Month' : date.month + 1,
                 'Day'   : date.date,
                 'Hour'  : date.hours,
                 'Minute': date.minutes]
        }

        def granularityToEndDate = granularityToDate(end)
        def granularityToBeginDate = granularityToDate(begin)

        def findSubSet = {
            node, lowerGranularity, func ->
                if(resultsSize < pageSize && reachedLast == false) {
                    def lowerGranValue = GranularityValues.valueOf((lowerGranularity + 's').toUpperCase())
                    def measurementPipe = node.getRecord().field(lowerGranularity.toLowerCase())
                    if (lowerGranularity != 'Sample')
                        measurementPipe = measurementPipe.sort { a, b -> b.key.toInteger() <=> a.key.toInteger() }
                    else
                        measurementPipe = measurementPipe[variableRid].field('samples')

                    measurementPipe.each {
                        nodeInfo ->
                                if (lowerGranularity.toString() != 'Sample') {

                                    if ((reachedFirst == true || nodeInfo.key.toInteger() <=
                                            granularityToEndDate[lowerGranularity]) && !reachedLast) {
                                        def lowerRid = nodeInfo.value
                                        def lowerNode = lowerRid.getRecord()

                                        if (lowerGranValue < granularityValue)
                                            func(lowerNode, granularityHierarchy[lowerGranularity], func)
                                        else {
                                            reachedFirst = true
                                            if (pageIndex >= pageRange[0] && pageIndex < pageRange[1]) {
                                                def log = lowerNode.field('log').getRecord()
                                                if (log.field('sum')) {
                                                    def sumMap = [:]
                                                    def meanMap = [:]

                                                    def sum = log.field('sum')[variableRid]
                                                    sumMap.put(variableUrl, sum.getRecord().field('value'))

                                                    def mean = log.field('mean')[variableRid]
                                                    meanMap.put(variableUrl, mean.getRecord().field('value'))

                                                    resultsSize += 1
                                                    pageIndex += 1

                                                    def timestamp = log.field('timestamp')
                                                    if(timestamp <= begin)
                                                        reachedLast = true

                                                    results.add([sum      : sumMap,
                                                                 mean     : meanMap,
                                                                 timestamp: timestamp.format("yyyy-MM-dd'T'HH:mm:ssX")])
                                                } else null
                                            } else pageIndex += 1
                                        }
                                    }
                                } else {
                                    reachedFirst = true
                                    if (pageIndex >= pageRange[0] && pageIndex < pageRange[1]) {
                                        def sampleRecord = nodeInfo.getRecord()
                                        def timestamp = sampleRecord.field('timestamp')
                                        if(timestamp <= begin)
                                            reachedLast = true

                                        resultsSize += 1
                                        pageIndex += 1

                                        results.add([value    : sampleRecord.field('value'),
                                                     timestamp: timestamp.format("yyyy-MM-dd'T'HH:mm:ssX")])


                                    } else pageIndex += 1
                                }

                    }
                }
                else return
        }

        yearMap.collect{ index, year ->
            if(index.toInteger() < end.year+1900)
                end = new Date('23:59:59 12/31/'+(end.year+1900).toString())

            if(index.toInteger() <= end.year+1900)
                findSubSet(year,'Month',findSubSet)
        }

        return results
    }
}
