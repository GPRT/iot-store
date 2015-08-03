package utils

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.ORecord
import com.tinkerpop.blueprints.impls.orient.OrientVertex

class OrientTransformer {
    public Map fromORecord(ORecord record) {
        def result = ['id':record.identity.clusterPosition]
        record.each{ fillMap(it, result) }
        return result
    }

    public Map fromOVertex(OrientVertex vertex) {
        def result = ['id':vertex.identity.clusterPosition]
        vertex.record.each { fillMap(it, result) }
        return result
    }

    private void fillMap(field, result) {
        def value = null

        switch (field.value.class) {
            case ORidBag:
                value = null
                break
            case ORecordId:
                value = value.clusterPosition
                break
            case ORecord:
                value = fromORecord(field.value)
                break
            case OrientVertex:
                value = fromOVertex(field.value)
                break
            default:
                value = field.value
        }

        if (field.key[0] != '_')
            result.put(field.key, value)
    }
}
