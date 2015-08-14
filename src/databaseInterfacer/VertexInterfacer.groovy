package databaseInterfacer

import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.VertexExceptionThrower

abstract class VertexInterfacer extends ClassInterfacer implements VertexExceptionThrower {

    def VertexInterfacer(factory, className, fields, links) {
        super(factory, className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateVertexProperties(HashMap data)
    abstract protected void generateVertexRelations(OrientVertex vertex, HashMap data)

    protected final LinkedHashMap create(HashMap data) {
        if (!(this.getExpandedNames() == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        def graph = factory.getNoTx()
        Long id = -1

        try {
            OrientVertex vertex = null
            def properties = generateVertexProperties(data)

            if (null in properties.values())
                invalidVertexProperties()

            vertex = graph.addVertex("class:" + this.className, properties)
            generateVertexRelations(vertex, data)
            graph.commit()

            id = vertex.identity.clusterPosition
        } catch(OValidationException e) {
            graph.rollback()
            invalidVertexProperties()
        } catch(ORecordDuplicatedException e) {
            graph.rollback()
            duplicatedVertex()
        } finally {
            graph.shutdown()
        }

        return this.getById(id, this.getExpandedNames())
    }

    protected final LinkedHashMap delete(Long id, String className=this.className) {
        def graph = factory.getTx()

        try {
            def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
            def rid = new ORecordId(clusterId, id.toLong())
            OrientVertex vertex = graph.getVertex(rid)

            if (!vertex)
                vertexNotFoundById(id)

            graph.removeVertex(vertex)
            return [:]
        } finally {
            graph.shutdown()
        }
    }

    protected final Iterable<LinkedHashMap> get(Set fieldNames, Set filterFields=[], Set sortFields=[],
                                                        int pageField=0, int pageLimitField=10,
                                                        String className=this.className) {
        def graph = factory.getNoTx()

        def osql = super.generateQuery(fieldNames, filterFields, sortFields, pageField, pageLimitField, className)

        def query = new OSQLSynchQuery(osql)
        try {
            graph.command(query).execute().collect {
                this.orientTransformer.fromOVertex(it)
            }
        }
        finally {
            graph.shutdown()
        }
    }

    protected final LinkedHashMap setById(Long id, HashMap data, String className=this.className) {
        if (!(this.getExpandedNames() == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
        def rid = new ORecordId(clusterId, id)

        def graph = factory.getNoTx()

        try {
            OrientVertex vertex = graph.getVertex(rid)

            def properties = generateVertexProperties(data)

            if (null in properties.values())
                invalidVertexProperties()

            vertex.setProperties(properties)

            for (edge in vertex.getEdges(Direction.BOTH)) {
                graph.removeEdge(edge)
            }

            generateVertexRelations(vertex, data)
            graph.commit()
        } catch (ORecordDuplicatedException e) {
            graph.rollback()
            duplicatedVertex()
        } catch (NullPointerException e) {
            vertexNotFoundById(id)
        } finally {
            graph.shutdown()
        }

        return this.getById(rid.clusterPosition, this.getExpandedNames())
    }

    protected final Iterable<LinkedHashMap> getById(Long id, Set fieldNames, String className=this.className) {
        def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
        def rid = new ORecordId(clusterId, id)

        return this.get(fieldNames, [].toSet(), [].toSet(), 0, 1, rid.toString())
    }

    protected final Iterable<LinkedHashMap> getByIndex(String fieldName, String fieldValue,
                                                               String className=this.className) {
        def graph = factory.getNoTx()

        try {
            return graph.getVertices(className + "." + fieldName, fieldValue)
        }
        finally {
            graph.shutdown()
        }
    }
}
