import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.id.ORecordId

db = new OrientGraph("remote:localhost/iot").getRawGraph()

allowedUsers = [db.browseClass("OUser").find{ it.field("name") == "ufpe"},
                    db.browseClass("OUser").find{ it.field("name") == "ufpe_reader"}]

mmntClasses = ['year','month','day','hour','minute','samples']

modifyAllowOfGranularity = { String className, int backwardsOffset=0 ->

    ODocument lastOfClass = db.browseClass(className).last().toList()[0]

    def clusterPosition = lastOfClass.getIdentity().getClusterPosition()

    def clusterId = lastOfClass.getIdentity().getClusterId() - backwardsOffset

    ODocument document

    while (clusterPosition > 0) {

        document = db.getRecord(new ORecordId(clusterId.toInteger(),clusterPosition.toInteger()))
        if(document) {
            if (className == "samples"){
                document.field("_allow", allowedUsers)
                document.save()
                db.commit()
            }
            else {
                ODocument log = document.field('log')
                if (allowedUsers[0] in log.field("_allow")) {
                    [log.field('sum'), log.field('mean')].each {
                        it.each {
                            variable, sumOrMean ->
                                ODocument rec = sumOrMean.getRecord()
                                rec.field("_allow", allowedUsers)
                                rec.save()
                        }
                    }
                    log.field("_allow", allowedUsers)
                    log.save()
                    document.field("_allow", allowedUsers)
                    document.save()
                    db.commit()
                }
            }
        }
        clusterPosition -= 1
    }
}

mmntClasses.each{ className ->
    println("class: " + className)
    modifyAllowOfGranularity(className)
}
