package databaseInterfacer

import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import exceptions.VertexExceptionThrower

abstract class VertexInterfacer extends ClassInterfacer implements VertexExceptionThrower {

    def VertexInterfacer(className, fields, links) {
        super(className, fields, links)
    }

    // Helpers
    abstract protected LinkedHashMap generateVertexProperties(HashMap data)
    abstract protected void generateVertexRelations(OrientGraph graph, OrientVertex vertex, HashMap data)

    protected final LinkedHashMap create(OrientGraph graph, HashMap data) {
        if (!(this.getExpandedNames() == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        Long id = -1
        OrientVertex vertex = null

        try {
            def properties = generateVertexProperties(data)

            if (null in properties.values())
                invalidVertexProperties()

            vertex = graph.addVertex("class:" + this.className, properties)
            generateVertexRelations(graph, vertex, data)
            graph.commit()

            id = vertex.identity.clusterPosition

            return this.getVertexById(graph, id, this.getExpandedNames())
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

    protected final LinkedHashMap delete(OrientGraph graph, Long id, String className=this.className) {
        def clusterId = this.getClusterId(graph.getRawGraph(), className)
        def rid = new ORecordId(clusterId, id.toLong())
        OrientVertex vertex = graph.getVertex(rid)

        if (!vertex)
            vertexNotFoundById(id)

        graph.removeVertex(vertex)
        return [:]
    }

    protected final Iterable<LinkedHashMap> get(OrientGraph graph,
                                                        Set fieldNames, Set filterFields=[], Set sortFields=[],
                                                        int pageField=0, int pageLimitField=10,
                                                        String className=this.className) {
        def osql = super.generateQuery(fieldNames, filterFields, sortFields, pageField, pageLimitField, className)

        def query = new OSQLSynchQuery(osql)

        return graph.command(query).execute().collect {
            this.orientTransformer.fromOVertex(it)
        }
    }

    protected final LinkedHashMap setById(OrientGraph graph,
                                                Long id, HashMap data, String className=this.className) {
        if (!(this.getExpandedNames() == data.keySet()))
            invalidVertexProperties()

        if (data.isEmpty())
            invalidVertexProperties()

        def clusterId = this.getClusterId(graph.getRawGraph(), className)
        def rid = new ORecordId(clusterId, id)

        try {
            OrientVertex vertex = graph.getVertex(rid)

            def properties = generateVertexProperties(data)

            if (null in properties.values())
                invalidVertexProperties()

            vertex.setProperties(properties)

            for (edge in vertex.getEdges(Direction.BOTH)) {
                graph.removeEdge(edge)
            }

            generateVertexRelations(graph, vertex, data)
            graph.commit()
        } catch (ORecordDuplicatedException e) {
            duplicatedVertex()
        } catch (NullPointerException e) {
            vertexNotFoundById(id)
        }

        return this.getById(graph, rid.clusterPosition, this.getExpandedNames())
    }

    protected final Iterable<LinkedHashMap> getById(OrientGraph graph,
                                                          Long id, Set fieldNames, String className=this.className) {
        def clusterId = this.getClusterId(graph.getRawGraph(), className)
        def rid = new ORecordId(clusterId, id)
        def vertex = this.get(graph, fieldNames, [].toSet(), [].toSet(), 0, 1, rid.toString())

        if (!vertex.getAt(0))
            vertexNotFoundById(id)

        return vertex
    }

    protected final Iterable<LinkedHashMap> getByIndex(OrientGraph graph,
                                                               String fieldName, String fieldValue,
                                                               String className=this.className) {
        def vertices = graph.getVertices(className + "." + fieldName, fieldValue)

        if (!vertices.getAt(0))
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "No field named [$fieldName] was found with value [$fieldValue] in class [$className]!",
                    "Check the body you sent")

        return vertices
    }
}
