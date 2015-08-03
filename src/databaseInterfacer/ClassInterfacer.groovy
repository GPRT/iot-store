package databaseInterfacer

import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ExceptionThrower
import utils.OrientTransformer

abstract class ClassInterfacer implements ExceptionThrower {
    def OrientGraphFactory factory
    def OrientTransformer orientTransformer = new OrientTransformer()
    def defaultClusterId = -1
    def className = ""
    def fields = []

    def ClassInterfacer(factory, className, fields) {
        this.factory = factory
        this.fields = fields
        this.className = className
        this.defaultClusterId = this.getClusterId(className)
    }

    // Helpers
    abstract protected LinkedHashMap generateVertexProperties(HashMap data)
    abstract protected void generateVertexRelations(OrientVertex vertex, HashMap data)
    abstract protected LinkedHashMap getExpandedVertex(OrientVertex vertex)

    private final Number getClusterId(String className) {
        def db = this.factory.getDatabase()
        try {
            return db.getClusterIdByName(className)
        } finally {
            db.close()
        }
    }

    private final String generateQuery(fieldNames, filterFields=[], sortFields=[], String className=this.className) {
        def osql = "select " + fieldNames.join(", ") + " from " + className

        if (!filterFields.isEmpty())
            osql += " where"

        filterFields.eachWithIndex { value, i ->
            def (fieldName, fieldOperator, constant) = value

            osql += " " + fieldName

            switch (fieldOperator) {
                case 'lt':
                    osql += " <"
                    break
                case 'gt':
                    osql += " >"
                    break
                case 'lte':
                    osql += " <="
                    break
                case 'gte':
                    osql += " >="
                    break
                case 'eq':
                    osql += " ="
                    break
                case 'neq':
                    osql += " <>"
                    break
            }

            osql += " " + constant

            if ((i + 1) < filterFields.size())
                osql += " and"
        }

        if (!sortFields.isEmpty())
            osql += " order by"

        sortFields.eachWithIndex { value, i ->
            def (fieldName, fieldOrder) = value
            osql += " " + fieldName + " " + fieldOrder

            if ((i + 1) < sortFields.size())
                osql += " ,"
        }

        return osql
    }

    protected final LinkedHashMap createVertex(HashMap data) {
        data.each {
            if (!(it.key in this.fields))
                invalidVertexProperties()
        }

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
            def vertex = graph.getVertex(rid)

            if (!vertex)
                vertexNotFoundById(id)

            graph.removeVertex(vertex)
            return [:]
        } finally {
            graph.shutdown()
        }
    }

    protected final Iterable<LinkedHashMap> getExpandedVertices(fieldNames, filterFields=[], sortFields=[],
                                                                String className=this.className) {
        def graph = factory.getNoTx()
        def osql = this.generateQuery(fieldNames, filterFields, sortFields, className)

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
                                                        String className=this.className) {
        def graph = factory.getNoTx()
        def osql = this.generateQuery(fieldNames, filterFields, sortFields, className)

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

    protected final LinkedHashMap getVertexById(Long id, String className=this.className) {
        def graph = factory.getNoTx()

        try {
            def clusterId = (className == this.className) ? this.defaultClusterId : this.getClusterId(className)
            def rid = new ORecordId(clusterId, id.toLong())
            def vertex = graph.getVertex(rid)
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