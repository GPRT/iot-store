package utils

import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.id.ORecordId

class Endpoints {
    static String mainUrl = ""
    static HashMap classToPath = [:]
    static HashMap clusterToClass = [:]

    static final URL ridToUrl(ORID rid) {
        String currentClass = getClass(rid.getIdentity().clusterId)
        return new URL(this.mainUrl + getPath(currentClass) + '/' + rid.getIdentity().clusterPosition)
    }

    static final ORecordId urlToRid(URL url) {
        List pathParts = url.path.split('/')
        String collection = pathParts[1]
        int id = -1

        try {
            id = pathParts[2].toInteger()
        } catch (NumberFormatException e) {
            throw new MalformedURLException()
        }

        String className = ''
        int cluster = -1

        try {
            className = classToPath.find({ return it.value == collection }).key
            cluster = clusterToClass.find({ return it.value == className }).key
        } catch (NullPointerException e) {
            throw new MalformedURLException()
        }

        return new ORecordId(cluster, id)
    }

    static addClass(String className, String path) {
        classToPath[className.toLowerCase()] = path.toLowerCase()
    }

    static addCluster(int clusterId, String className) {
        clusterToClass[clusterId] = className.toLowerCase()
    }

    static final String getPath(String className) {
        return '/' + classToPath[className.toLowerCase()]
    }

    static final String getClass(int clusterId) {
        return clusterToClass[clusterId]
    }
}
