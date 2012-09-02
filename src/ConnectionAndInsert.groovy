import com.mongodb.Mongo
import com.mongodb.DB
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.WriteConcern
import com.mongodb.DBCursor

class ConnectionAndInsert {

    //Tutorial - http://www.mongodb.org/display/DOCS/Java+Tutorial

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

        m.setWriteConcern(WriteConcern.SAFE)


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

        println coll.findOne()

        def start = System.currentTimeMillis()

        for (int i = 0; i < 100; i++) {

            coll.insert(new BasicDBObject().append("i", i))
        }

        def finish = System.currentTimeMillis()

        println "Insert time: ${finish - start / 1000} secounds"

        println "Document count: ${coll.getCount()}"


        DBCursor cursor = coll.find()
        try {
            while(cursor.hasNext()) {

                println "${cursor.next()}"

            }
        } finally {
            cursor.close()
        }

        //Basic query
        BasicDBObject query = new BasicDBObject()

        query.put("i", 71)

        cursor = coll.find(query)

        try {
            println "Query result:"
            while(cursor.hasNext()) {

                println "${cursor.next()}"

            }
        } finally {
            cursor.close()
        }

        query = new BasicDBObject()


        query.put("j", new BasicDBObject('$ne', 3))
        query.put("k", new BasicDBObject('$gt', 10))

        cursor = coll.find(query)

        try {
            println "Regular expression result:"
            while(cursor.hasNext()) {
                println cursor.next()
            }
        } finally {
            cursor.close()
        }

        //Up to "Getting A Set of Documents With a Query" - http://www.mongodb.org/display/DOCS/Java+Tutorial


        //drop DB when finished
        m.dropDatabase("mydb")


    }

}
