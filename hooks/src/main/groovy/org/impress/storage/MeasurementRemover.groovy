package org.impress.storage

import com.orientechnologies.orient.core.db.ODatabase
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract
import com.orientechnologies.orient.core.hook.ORecordHook
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument

public class MeasurementRemover extends ODocumentHookAbstract implements ORecordHook {

    public MeasurementRemover() {
        setIncludeClasses("Resource")
    }

    @Override
    public ORecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return ORecordHook.DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterDelete(ODocument document) {
        def db = ODatabaseRecordThreadLocal.INSTANCE.get();
        def mmntClasses = ['year','month','day','hour','minute','sample']
        ODocument root = document.field('measurements')

        if (!root)
            return

        def removeChildren = {
            node,func ->

                if(!node) return

                def granularity
                for(fieldName in node.fieldNames())
                    if (fieldName in mmntClasses)
                        granularity = fieldName

                node.field(granularity).each {
                    key, doc ->
                        ODocument docRecord = doc.getRecord()
                        if (granularity == 'sample') {
                            docRecord.getRecord().delete()
                        }
                        else {
                            func(docRecord, func)
                            ODocument log = docRecord.field('log')
                            [log.field('sum'),log.field('mean')].each{
                                it.each{ variable, sumOrMean -> sumOrMean.getRecord().delete() }
                            }
                            log.delete()
                            docRecord.delete()
                        }
                }
        }

        removeChildren(root,removeChildren)
        root.delete()
    }
}
