package databaseInterfacer

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.exception.*
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import exceptions.DocumentExceptionThrower
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException

abstract class DocumentInterfacer extends ClassInterfacer implements DocumentExceptionThrower {
    def DocumentInterfacer(Object className, Object fields, links) {
        super(className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateDocumentProperties(OrientGraph graph, HashMap data)
    abstract protected void generateDocumentRelations(OrientGraph graph, ODocument document, HashMap data, parent)

    protected final LinkedHashMap create(OrientGraph graph, HashMap data) {
        if (!(this.getFieldNames() == data.keySet()))
            invalidDocumentProperties()

        if (data.isEmpty())
            invalidDocumentProperties()

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
            generateDocumentRelations(graph, document, data, parent)
            db.commit()

            return this.orientTransformer.fromODocument(document)
        }
        catch(OValidationException e) {
            document.delete()
            invalidDocumentProperties()
        }
        }
    }

    protected Iterable<LinkedHashMap> get(OrientGraph graph,
                                                fieldNames, filterFields=[], sortFields=[],
                                                pageField=0, pageLimitField=10,
                                                String className=this.className) {
        def db = graph.getRawGraph()
        def osql = generateQuery(fieldNames, filterFields, sortFields, pageField, pageLimitField, className)

        def query = new OSQLSynchQuery(osql)
        try {
            db.begin()
            db.command(query).execute().collect {
                this.orientTransformer.fromODocument(it)
            }
            db.commit()
        }
    }

    protected final LinkedHashMap delete(OrientGraph graph, Long id, String className=this.className) {
        def db = graph.getRawGraph()
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
        }
    }
}
