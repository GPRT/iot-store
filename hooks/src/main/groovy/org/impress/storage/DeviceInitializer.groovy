package org.impress.storage

import com.orientechnologies.orient.core.hook.ODocumentHookAbstract
import com.orientechnologies.orient.core.hook.ORecordHook
import com.orientechnologies.orient.core.record.impl.ODocument

public class DeviceInitializer extends ODocumentHookAbstract implements ORecordHook {

    public DeviceInitializer() {
        setIncludeClasses("Resource")
    }

    @Override
    public ORecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return ORecordHook.DISTRIBUTED_EXECUTION_MODE.BOTH;
    }
    @Override
    public void onRecordAfterCreate( ODocument document ) {
        if (!document.field('measurements')) {
            document.field(
                'measurements',
                new ODocument('measurements').
                    field('year', new LinkedHashMap())
            )
            document.save()
        }
    }
}
