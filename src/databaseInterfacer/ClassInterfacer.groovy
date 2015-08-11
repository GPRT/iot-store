package databaseInterfacer

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import utils.OrientTransformer

abstract class ClassInterfacer {
    OrientGraphFactory factory
    OrientTransformer orientTransformer = new OrientTransformer()
    Integer defaultClusterId = -1
    String className = ""
    Set fields = []

    def ClassInterfacer(factory, className, fields) {
        this.factory = factory
        this.fields = fields
        this.className = className
        this.defaultClusterId = this.getClusterId(className)
    }

    protected final Number getClusterId(String className) {
        def db = this.factory.getDatabase()
        try {
            return db.getClusterIdByName(className)
        } finally {
            db.close()
        }
    }

    protected final String generateQuery(fieldNames, filterFields=[], sortFields=[], pageField=0, pageLimitField=10,
                                       String className=this.className) {
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

        osql += " skip " + (pageField * pageLimitField) + " limit " + pageLimitField

        return osql
    }
}