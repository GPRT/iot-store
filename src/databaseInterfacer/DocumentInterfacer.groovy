package databaseInterfacer

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.exception.*
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.DocumentExceptionThrower
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

abstract class DocumentInterfacer extends ClassInterfacer implements DocumentExceptionThrower {
    def DocumentInterfacer(Object className, Object fields, links) {
        super(className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateDocumentProperties(ODatabaseDocumentTx db, HashMap data, HashMap optionalData)
    abstract protected void generateDocumentRelations(ODatabaseDocumentTx db, ODocument document, HashMap data, HashMap optionalData)

    protected final LinkedHashMap create(ODatabaseDocumentTx db,
                                         HashMap data,
                                         HashMap optionalData = [:]) {
        if (!(this.getFieldNames() == data.keySet()))
            invalidDocumentProperties()

        if (data.isEmpty())
            invalidDocumentProperties()

        ODocument document = null

        try {
            db.begin()
            def properties = generateDocumentProperties(db, data, optionalData)

            if (null in properties.values())
                invalidDocumentProperties()

            document = new ODocument(this.className)
            properties.each{
                key, value ->
                    document.field(key,value)
            }
            document.save()
            generateDocumentRelations(db, document, data, optionalData)
            db.commit()

            return this.orientTransformer.fromODocument(document)
        }
        catch(OValidationException e) {
            document.delete()
            invalidDocumentProperties()
        }
    }

    protected final LinkedHashMap delete(ODatabaseDocumentTx db, Long id, String className=this.className) {
        db.begin()
        def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
        def rid = new ORecordId(clusterId, id.toLong())
        ODocument document = db.getRecord(rid)

        if (!document)
            documentNotFoundById(id)

        db.delete(document)
        db.commit()
        return [:]
    }
}
