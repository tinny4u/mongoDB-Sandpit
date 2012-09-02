import com.mongodb.Mongo
import com.mongodb.DB
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
/**
 * Created with IntelliJ IDEA.
 * User: sean
 * Date: 2/09/12
 * Time: 5:32 PM
 * To change this template use File | Settings | File Templates.
 */
class ConnectionAndInsert {

    public static void main(String[] args) {


//        Connection examples
//        Mongo m = new Mongo();
//        // or
//        Mongo m = new Mongo( "localhost" );
//        // or
//        Mongo m = new Mongo( "localhost" , 27017 );
//        // or, to connect to a replica set, supply a seed list of members
//        Mongo m = new Mongo(Arrays.asList(new ServerAddress("localhost", 27017),
//                new ServerAddress("localhost", 27018),
//                new ServerAddress("localhost", 27019)));

        Mongo m = new Mongo('localhost')

        DB db = m.getDB( "mydb" )

        println 'Stats:'
        db.getStats().entrySet().each {
            println "${it.key}, ${it.value}"
        }


        BasicDBObject doc = new BasicDBObject()
        doc.putAll(['name': 'MongoDB',
                    'type': 'database',
                    'count': 1])

        BasicDBObject info = new BasicDBObject()
        info.putAll(['x': 203,
                     'y': 102])


        doc.put("info", info)

        DBCollection coll = db.getCollection("testCollection")

        coll.insert(doc)

        def cols = db.getCollectionNames()

        println "Collections:"
        cols.each {
            println it
        }

    }

}
