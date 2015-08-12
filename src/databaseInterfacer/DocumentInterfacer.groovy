package databaseInterfacer

import com.orientechnologies.orient.core.exception.*
import com.orientechnologies.orient.core.record.impl.ODocument
import exceptions.DocumentExceptionThrower

abstract class DocumentInterfacer extends ClassInterfacer implements DocumentExceptionThrower {

    def DocumentInterfacer(Object factory, Object className, Object fields, links) {
        super(factory, className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateDocumentProperties(HashMap data)
    abstract protected void generateDocumentRelations(ODocument document, HashMap data)

    protected final LinkedHashMap createDocument(HashMap data) {
        if (!(this.fields == data.keySet()))
            invalidDocumentProperties()

        if (data.isEmpty())
            invalidDocumentProperties()

        def db = factory.getTx().getRawGraph()
        ODocument document = null

        try {
            db.begin()
            def properties = generateDocumentProperties(data)

            if (null in properties.values())
                invalidDocumentProperties()

            document = new ODocument(this.className)
            properties.each{
                key,value ->
                    document.field(key,value)
            }
            document.save()
            generateDocumentRelations(document, data)
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
}
