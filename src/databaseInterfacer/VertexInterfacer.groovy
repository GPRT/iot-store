package databaseInterfacer

import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.VertexExceptionThrower

abstract class VertexInterfacer extends ClassInterfacer implements VertexExceptionThrower {

    def VertexInterfacer(factory, className, fields) {
        super(factory, className, fields)
    }

    // Helpers
    abstract protected LinkedHashMap generateVertexProperties(HashMap data)
    abstract protected void generateVertexRelations(OrientVertex vertex, HashMap data)
    abstract protected LinkedHashMap getExpandedVertex(OrientVertex vertex)

    protected final LinkedHashMap createVertex(HashMap data) {
        if (!(this.fields == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        def graph = factory.getNoTx()
        OrientVertex vertex = null

        try {
            def properties = generateVertexProperties(data)

            if (null in properties.values())
                invalidVertexProperties()

            vertex = graph.addVertex("class:" + this.className, properties)
            generateVertexRelations(vertex, data)
            graph.commit()

            return this.orientTransformer.fromOVertex(vertex)
        } catch(OValidationException e) {
            graph.rollback()
            invalidVertexProperties()
        } catch(ORecordDuplicatedException e) {
            graph.rollback()
            duplicatedVertex()
        } finally {
            graph.shutdown()
        }
    }

    protected final LinkedHashMap deleteVertex(Long id, String className=this.className) {
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

    protected final Iterable<LinkedHashMap> getExpandedVertices(fieldNames, filterFields=[], sortFields=[],
                                                                pageField=0, pageLimitField=10,
                                                                String className=this.className) {
        def graph = factory.getNoTx()
        def osql = this.generateQuery(fieldNames, filterFields, sortFields, pageField, pageLimitField, className)

        def query = new OSQLSynchQuery(osql)
        try {
            return graph.command(query).execute().collect {
                LinkedHashMap result = this.orientTransformer.fromOVertex(it)
                result.putAll(this.getExpandedVertex(it))
                result
            }
        }
        finally {
            graph.shutdown()
        }
    }

    protected final Iterable<LinkedHashMap> getVertices(fieldNames, filterFields=[], sortFields=[],
                                                        pageField=0, pageLimitField=10,
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

    protected final LinkedHashMap setVertexById(Long id, HashMap data, String className=this.className) {
        if (!(this.fields == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        def graph = factory.getNoTx()

        try {
            def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
            def rid = new ORecordId(clusterId, id.toLong())
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

            return this.orientTransformer.fromOVertex(vertex)
        } catch (ORecordDuplicatedException e) {
            graph.rollback()
            duplicatedVertex()
        } catch (NullPointerException e) {
            vertexNotFoundById(id)
        } finally {
            graph.shutdown()
        }
    }

    protected final LinkedHashMap getVertexById(Long id, String className=this.className) {
        def graph = factory.getNoTx()

        try {
            def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
            def rid = new ORecordId(clusterId, id.toLong())
            OrientVertex vertex = graph.getVertex(rid)
            return this.orientTransformer.fromOVertex(vertex)
        } catch (NullPointerException e) {
            vertexNotFoundById(id)
        } finally {
            graph.shutdown()
        }
    }

    protected final LinkedHashMap getExpandedVertexById(Long id, String className=this.className) {
        def graph = factory.getNoTx()

        try {
            def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
            def rid = new ORecordId(clusterId, id.toLong())
            def vertex = graph.getVertex(rid)
            LinkedHashMap result = this.orientTransformer.fromOVertex(vertex)
            result.putAll(this.getExpandedVertex(vertex))
            return result
        } catch (NullPointerException e) {
            vertexNotFoundById(id)
        } finally {
            graph.shutdown()
        }
    }

    protected final Iterable<LinkedHashMap> getVerticesByIndex(String fieldName, String fieldValue,
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
