package utils

import com.orientechnologies.orient.core.db.record.ORecordLazyList
import com.orientechnologies.orient.core.db.record.OTrackedList
import com.orientechnologies.orient.core.db.record.OTrackedMap
import com.orientechnologies.orient.core.db.record.OTrackedSet
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientVertex

class OrientTransformer {
    public Map fromODocument(ODocument document) {
        def result = [:]
        document.each { fillMap(it, result) }
        return result
    }

    public Map fromORecord(ORecord record) {
        def result = [:]
        record.each { fillMap(it, result) }
        return result
    }

    public Map fromOVertex(OrientVertex vertex) {
        def result = [:]
        vertex.record.each { fillMap(it, result) }
        return result
    }

    public List fromORecordLazyList(ORecordLazyList list) {
        def result = []
        list.each {
            fillList(it.getIdentity(), result)
        }
        return result
    }

    public List fromOTrackedSet(OTrackedSet set) {
        def result = []
        set.toList().each {
            fillList(it, result)
        }
        return result
    }

    public List fromOTrackedList(OTrackedList list) {
        def result = []
        list.each { fillList(it, result) }
        return result
    }

    public Map fromOTrackedMap(OTrackedMap map) {
        def result = [:]
        map.each { fillMap(it, result) }
        return result
    }

    private getValue(Object field) {
        def value = null
        def type = field.getClass()

        switch (type) {
            case ORidBag:
                value = null
                break
            case OTrackedSet:
                value = fromOTrackedSet(field)
                break
            case OTrackedMap:
                value = fromOTrackedMap(field)
                break
            case ORecordLazyList:
                value = fromORecordLazyList(field)
                break
            case OTrackedList:
                value = fromOTrackedList(field)
                break
            case ODocument:
                value = fromODocument(field)
                break
            case ORecordId:
                value = Endpoints.ridToUrl(field)
                break
            case ORecord:
                value = fromORecord(field)
                break
            case OrientVertex:
                value = fromOVertex(field)
                break
            default:
                value = field
        }

        return value
    }

    private void fillList(field, result) {
        def value = getValue(field)

        result.add(value)
    }

    private void fillMap(field, result) {
        def value = getValue(field.value)

        if (field.key[0] != '_')
            result.put(field.key, value)
    }
}
