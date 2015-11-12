package org.impress.storage

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract
import com.orientechnologies.orient.core.hook.ORecordHook
import com.orientechnologies.orient.core.record.impl.ODocument

public class MeasurementRemovalHook extends ODocumentHookAbstract implements ORecordHook {

    public MeasurementRemovalHook() {
        setIncludeClasses("Resource")
    }

    @Override
    public ORecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return ORecordHook.DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterDelete( ODocument document ) {
        def db = ODatabaseRecordThreadLocal.INSTANCE.get();
        def mmntClasses = ['year','month','day','hour','minute','sample']
        ODocument root = document.field('measurements')

        def removeChildren = {
            node,func ->
                def granularity
                for(fieldName in node.fieldNames())
                    if (fieldName in mmntClasses)
                        granularity = fieldName

                node.field(granularity).each {
                    key, doc ->
                        def docRecord = doc.getRecord()
                        if (granularity == 'sample') {
                            docRecord.field('sample').each{
                                variable, sample ->
                                    sample.getRecord().delete()
                                    db.commit()
                            }
                            docRecord.delete()
                            db.commit()
                        }
                        else {
                            func(docRecord, func)
                            docRecord.field('log').delete()
                            docRecord.delete()
                            db.commit()
                        }
                }
                db.commit()
        }

        removeChildren(root,removeChildren)
        document.removeField('measurements')
        root.delete()
        db.commit()
    }
}