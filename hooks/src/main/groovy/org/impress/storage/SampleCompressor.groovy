package org.impress.storage

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract
import com.orientechnologies.orient.core.hook.ORecordHook
import com.orientechnologies.orient.core.record.impl.ODocument

public class SampleCompressor extends ODocumentHookAbstract implements ORecordHook {

    public SampleCompressor() {
        setIncludeClasses("Month")
    }

    @Override
    public ORecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return ORecordHook.DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterUpdate( ODocument document ) {
        def measurementClasses = ['year','month','day','hour','minute']
        def db = ODatabaseRecordThreadLocal.INSTANCE.get();

        def compressSamples = { node,func ->
            String granularity

            for(fieldName in node.fieldNames())
                if (fieldName in measurementClasses)
                    granularity = fieldName

            node.field(granularity).each {
                key, doc ->
                    ODocument docRecord = doc.getRecord()
                    Date timestamp = docRecord.field('log').field('timestamp')
                    if (granularity == 'minute') {

                        docRecord.field('sample').collect{
                            variable, samples ->
                                ODocument samplesRecord = samples.getRecord()
                                List samplesList = samplesRecord.field('samples')
                                Double meanValue = samplesList.sum{
                                    it.field('value')
                                } / samplesList.size()

                                samplesList.clear()
                                samplesList.add( (new ODocument('Sample'))
                                        .field('value',meanValue)
                                        .field('timestamp',timestamp) )
                                samplesRecord.field('samples',samplesList)
                                samplesRecord.save()
                        }
                        docRecord.save()
                        db.commit()
                    }
                    else
                        func(docRecord, func)
            }
        }

        if(document.field('lastMeasurement') != null){
            ODocument nextMonth = document
            int monthsPassed = 0

            while(monthsPassed < 3) {
                nextMonth = nextMonth.field('lastMeasurement')
                if(!nextMonth)
                    break
                monthsPassed += 1
            }

            if(nextMonth)
                compressSamples(nextMonth, compressSamples)
        }
    }
}
