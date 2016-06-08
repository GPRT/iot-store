package utils

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OSecurityAccessException
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.impress.storage.databaseInterfacer.ClassInterfacer
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException
import org.impress.storage.utils.Endpoints

class SpecSetupBuilder {
    static String login = 'test'
    static String password = '123'
    static OPartitionedDatabasePoolFactory factory = new OPartitionedDatabasePoolFactory()
    static String iotStoreUrl = "http://192.168.0.189:4567"

    public static String getPaths(String className){
        try {
            URL mainUrl = new URL(iotStoreUrl);
            Endpoints.setMainUrl(mainUrl.toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException("[" + iotStoreUrl + "] " +
                    "is not a valid URL for your iotStore.url property");
        }
        Endpoints.addClass("area", "areas");
        Endpoints.addClass("measurementvariable", "variables");
        Endpoints.addClass("resource", "devices");
        Endpoints.addClass("group", "groups");
        Endpoints.addClass("simulation", "simulations");


        return Endpoints.mainUrl+Endpoints.getPath(className)
    }

    public static ODatabaseDocumentTx getDatabase() {
        try {
            return factory.get("remote:localhost/test", login, password).acquire()
        } catch (OSecurityAccessException e) {
            throw new ResponseErrorException(ResponseErrorCode.AUTHENTICATION_ERROR,
                    401,
                    "The basic auth failed!",
                    "Check if your login and password are correct")
        }
    }
}