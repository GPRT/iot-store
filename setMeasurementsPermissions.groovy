import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

db = new OrientGraph("remote:localhost/iot").getRawGraph()

allowedUsers = [db.browseClass("OUser").find{ it.field("name") == "ufpe"},
                    db.browseClass("OUser").find{ it.field("name") == "ufpe_reader"}]

class DevicePermissions {
    public List mmntClasses
    public List usersAllowedList
    private ODatabaseDocumentTx db = new OrientGraph("remote:localhost/iot").getRawGraph()

    def DevicePermissions(List allowedUsers){
        mmntClasses = ['year','month','day','hour','minute','sample']
        usersAllowedList = allowedUsers
    }

    def allowUsers(ODocument document){
            def granularity = null
            for (fieldName in document.fieldNames())
                if (fieldName in this.mmntClasses)
                    granularity = fieldName

            document.field(granularity).each {
                key, doc ->
                    ODocument docRecord = doc.getRecord()
                    if (granularity == 'sample') {
                        docRecord.field("_allow", this.usersAllowedList)
                        docRecord.save()
                        db.commit()
                    } else {
                        allowUsers(docRecord)
                        ODocument log = docRecord.field('log')
                        [log.field('sum'), log.field('mean')].each {
                            it.each {
                                variable, sumOrMean ->
                                    ODocument rec = sumOrMean.getRecord()
                                    rec.field("_allow", this.usersAllowedList)
                                    rec.save()
                            }
                        }
                        log.field("_allow", this.usersAllowedList)
                        log.save()
                        docRecord.field("_allow", this.usersAllowedList)
                        docRecord.save()
                        db.commit()
                    }
            }
            db.commit()
    }
}

devicePermissions = new DevicePermissions(allowedUsers)
devicePermissions.allowUsers(device.field("measurements"))

device = db.browseClass("resource").toList()[37]
