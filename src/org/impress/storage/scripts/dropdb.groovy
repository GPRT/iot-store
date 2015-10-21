package org.impress.storage.scripts

import com.orientechnologies.orient.client.remote.OServerAdmin

admin = new OServerAdmin('remote:localhost/iot');
admin.connect("root", "root");

admin.dropDatabase("iot");
