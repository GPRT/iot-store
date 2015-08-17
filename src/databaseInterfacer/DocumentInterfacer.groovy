package databaseInterfacer

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.exception.*
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.DocumentExceptionThrower
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

abstract class DocumentInterfacer extends ClassInterfacer implements DocumentExceptionThrower {

    def DocumentInterfacer(Object factory, Object className, Object fields, links) {
        super(factory, className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateDocumentProperties(HashMap data)
    abstract protected void generateDocumentRelations(ODocument document, HashMap data, OrientVertex parent)

    protected final LinkedHashMap create(HashMap data, Long id=null) {
        if (!(this.getFieldNames() == data.keySet()))
            invalidDocumentProperties()

        if (data.isEmpty())
            invalidDocumentProperties()

        def graph = factory.getTx()
        def db = graph.getRawGraph()
        ODocument document = null

        try {
            def parent = null
            db.begin()
            if (id>=0){
                def clusterId = this.getClusterId('Resource')
                def rid = new ORecordId(clusterId, id.toLong())
                parent = graph.getVertex(rid)

                if(!parent) {
                    throw new ResponseErrorException(ResponseErrorCode.DEVICE_NOT_FOUND,
                            404,
                            "Device with id [" + id + "] was not found!",
                            "The device does not exist")
                }
            }

            def properties = generateDocumentProperties(data)

            if (null in properties.values())
                invalidDocumentProperties()

            document = new ODocument(this.className)
            properties.each{
                key,value ->
                    document.field(key,value)
            }
            document.save()
            generateDocumentRelations(document, data, parent)
            db.commit()

            return this.orientTransformer.fromODocument(document)
        }
        catch(OValidationException e) {
            document.delete()
            invalidDocumentProperties()
        }
        finally {
            db.close()
        }
    }

    protected Iterable<LinkedHashMap> get(fieldNames, filterFields=[], sortFields=[],
                                                pageField=0, pageLimitField=10,
                                                String className=this.className) {
        def db = factory.getNoTx().getRawGraph()
        def osql = generateQuery(fieldNames, filterFields, sortFields, pageField, pageLimitField, className)

        def query = new OSQLSynchQuery(osql)
        try {
            db.begin()
            db.command(query).execute().collect {
                this.orientTransformer.fromODocument(it)
            }
            db.commit()
        }
        finally {
            db.close()
        }
    }

    protected final LinkedHashMap delete(Long id, String className=this.className) {
        def db = factory.getTx().getRawGraph()

        try {
            db.begin()
            def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
            def rid = new ORecordId(clusterId, id.toLong())
            ODocument document = db.getRecord(rid)

            if (!document)
                documentNotFoundById(id)

            db.delete(document)
            db.commit()
            return [:]
        } finally {
            db.close()
        }
    }
}
