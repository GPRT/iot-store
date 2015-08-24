package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import exceptions.VertexExceptionThrower

abstract class VertexInterfacer extends ClassInterfacer implements VertexExceptionThrower {

    def VertexInterfacer(className, fields, links) {
        super(className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateVertexProperties(ODatabaseDocumentTx db,
                                                              HashMap data,
                                                              HashMap optionalData)
    abstract protected void generateVertexRelations(ODatabaseDocumentTx db,
                                                    OrientVertex vertex,
                                                    HashMap data,
                                                    HashMap optionalData)

    protected final LinkedHashMap create(ODatabaseDocumentTx db,
                                         HashMap data,
                                         HashMap optionalData = [:]) {
        if (!(this.getExpandedNames() == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        Long id = -1
        OrientGraphNoTx graph = new OrientGraphNoTx(db)
        OrientVertex vertex = null

        try {
            def properties = generateVertexProperties(db, data, optionalData)

            if (null in properties.values())
                invalidVertexProperties()

            vertex = graph.addVertex("class:" + this.className, properties)
            generateVertexRelations(db, vertex, data, optionalData)
            graph.commit()
            return this.getDocumentByRid(db, vertex.getIdentity()).collect() {
                this.orientTransformer.fromODocument(it)
            }
        } catch(OValidationException e) {
            invalidVertexProperties()
        } catch(ORecordDuplicatedException e) {
            duplicatedVertex()
        } catch(ResponseErrorException e) {
            if (vertex)
                graph.removeVertex(vertex)

            throw e
        }
    }

    protected final LinkedHashMap delete(ODatabaseDocumentTx db, Long id, String className=this.className) {
        def clusterId = this.getClusterId(db, className)
        def rid = new ORecordId(clusterId, id.toLong())
        OrientGraph graph = new OrientGraph(db)
        OrientVertex vertex = graph.getVertex(rid)

        if (!vertex)
            vertexNotFoundById(id)

        graph.removeVertex(vertex)
        return [:]
    }

    protected final Iterable<LinkedHashMap> get(ODatabaseDocumentTx db,
                                                HashMap params = [:],
                                                String className = this.className) {
        def results = super.getDocuments(db, params, className)

        return results.collect() {
            this.orientTransformer.fromODocument(it)
        }
    }

    protected final LinkedHashMap setById(ODatabaseDocumentTx db,
                                          Long id,
                                          HashMap data,
                                          String className=this.className) {
        if (!(this.getExpandedNames() == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        def clusterId = this.getClusterId(db, className)
        def rid = new ORecordId(clusterId, id)

        try {
            OrientGraph graph = new OrientGraph(db)
            OrientVertex vertex = graph.getVertex(rid)

            if (!vertex)
                vertexNotFoundById(id)

            def properties = generateVertexProperties(db, data)

            if (null in properties.values())
                invalidVertexProperties()

            vertex.setProperties(properties)

            for (edge in vertex.getEdges(Direction.BOTH)) {
                graph.removeEdge(edge)
            }

            generateVertexRelations(db, vertex, data)
            graph.commit()
        } catch (ORecordDuplicatedException e) {
            duplicatedVertex()
        } catch (NullPointerException e) {
            vertexNotFoundById(id)
        }

        return this.getDocumentByRid(db, rid).collect() {
            this.orientTransformer.fromODocument(it)
        }
    }

    protected final Iterable<LinkedHashMap> getById(ODatabaseDocumentTx db,
                                                    Long id,
                                                    Set fieldNames,
                                                    String className=this.className) {
        def clusterId = this.getClusterId(db, className)
        def rid = new ORecordId(clusterId, id)

        def vertex = this.getDocumentByRid(db, rid, fieldNames)

        if (!vertex.getAt(0))
            vertexNotFoundById(id)

        return vertex.collect {
            this.orientTransformer.fromODocument(vertex)
        }
    }
}
