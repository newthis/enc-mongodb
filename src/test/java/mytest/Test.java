package mytest;
import com.mongodb2.MongoClient;
import com.mongodb2.MongoClientOptions;
import com.mongodb2.ServerAddress;
import com.mongodb2.client.MongoCollection;
import com.mongodb2.client.MongoCursor;
import com.mongodb2.client.MongoDatabase;
import org.bson.Document;
import java.util.ArrayList ;
import org.bson2.codecs.OriginDocumentCodec;
import org.bson2.codecs.configuration.CodecRegistries;
import org.bson2.codecs.configuration.CodecRegistry;


public class Test {

    public static void main(String [] args){
//        Document doc = new Document() ;
//        doc.put("name", "gada") ;
//        System.out.println(doc.toJson().toString()) ;
        //OriginDocumentCodec ncodec = new OriginDocumentCodec() ;
        //MongoClientOptions customClientOptions = new MongoClientOptions.Builder().maxConnectionIdleTime(0).maxConnectionLifeTime(0).maxWaitTime(100000).connectionsPerHost(100).build() ;

        //CodecRegistry codecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(ncodec),MongoClient.getDefaultCodecRegistry());
        //MongoClientOptions customClientOptions  = new MongoClientOptions.Builder().codecRegistry(codecRegistry).maxConnectionIdleTime(0).maxConnectionLifeTime(0).maxWaitTime(100000).connectionsPerHost(100).build() ; ;
        MongoClientOptions customClientOptions  = new MongoClientOptions.Builder().maxConnectionIdleTime(0).maxConnectionLifeTime(0).maxWaitTime(100000).connectionsPerHost(100).build() ; ;
        String url = "192.168.1.109" ;
        int port = 37017 ;
        ServerAddress address = new ServerAddress(url, port ) ;
        MongoClient client = new MongoClient(address, customClientOptions) ;
        MongoDatabase db = client.getDatabase("tulu") ;
        MongoCollection colc = db.getCollection("open") ;
        org.bson.Document dd = new Document() ;
        //org.bson2.Document pst = new org.bson2.Document() ;


        //String usss = "{ \"_id\" : { \"$oid\" : \"59ed68364d6889bea05f95b7\" }, \"project_id\" : 105066, \"project_name\" : \"index\", \"project_userid\" : 38165, \"project_user\" : \"hashpage\", \"full_name\" : null, \"main_language\" : \"JavaScript\", \"created_time\" : \"2009-01-11T04:33:59Z\", \"project_star\" : 5, \"project_fork\" : 1, \"project_watch\" : 0, \"popularity\" : 6, \"popularity_level\" : { \"$numberLong\" : \"1\" }, \"project_url\" : \"https://github.com/hashpage/index\", \"detail_info\" : null, \"is_library\" : 0, \"latest_version\" : null, \"vul_info\" : null, \"vul_details\" : null, \"highest_risk\" : 0, \"versions_total\" : 0, \"is_update\" : 0, \"official_license\" : [], \"versions\" : [{ \"name\" : \"master\", \"files_total\" : 104, \"valid_files_total\" : 13, \"empty_files\" : 0, \"project_size\" : 190.4853515625, \"valid_project_size\" : 58.228515625, \"lines_total\" : { \"$numberLong\" : \"1530\" }, \"pushed_time\" : \"2016-05-08T22:08:32Z\", \"is_download\" : 0, \"is_setup\" : 0, \"is_update\" : 0, \"checked_license\" : [], \"clone_valid_size\" : 59626.0, \"clone_valid_files\" : 13 }] }";
        //pst.put("project_url","https://github.com/wash/rnaz") ;

        ArrayList<String> ghd = new ArrayList<String>() ;
        ghd.add("CVE-2910-232") ;
        ghd.add("CVE-2018-2332") ;
        //org.bson2.Document pst = org.bson2.Document.parse("{\"project_id\":\"fdafa\"}") ;
        org.bson2.Document pst = new org.bson2.Document() ;
        pst.put("names", ghd) ;
        //System.out.println(pst.toJson()) ;
        colc.insertOne(pst);
//        dd.put("name", 3) ;
//        pst.putAll(dd);
//        colc.insertOne(pst);
//        MongoCursor<org.bson2.Document> dc = colc.find().limit(1).iterator() ;
//        int counter = 0 ;
//        while(dc.hasNext()){
//            org.bson2.Document doc = dc.next() ;
//            System.out.println(doc.getString("project_id")) ;
//            counter = counter + 1;
//        }
//        System.out.println(counter) ;
//        HashMap<String,Object> ss = new HashMap<String,Object>();
//        ss.put("a",3) ;
//        ss.put("ab","ee") ;
//        String sga = "{ \"a\" : 3, \"ab\" : \"ee\" }" ;
//        Document dc = Document.parse(sga, ncodec) ;
//        System.out.println(dc.getString("ab")) ;

//        System.out.println(dc.toJson()) ;


//        String url = "192.168.1.109" ;
//        int port = 37017 ;
//        ServerAddress address = new ServerAddress(url, port ) ;
//        //ServerAddress address = new ServerAddress("127.0.0.1", 29102) ;
//        MongoClient mongoClient = new MongoClient(address, customClientOptions) ;
//        MongoDatabase  db = mongoClient.getDatabase("CodeClone3") ;
//        MongoCollection col = db.getCollection("opensource_projects") ;
//        MongoCursor<Document> cursor = col.find().batchSize(200).iterator() ;
//        while(cursor.hasNext()){
//            Document ds = cursor.next() ;
//            System.out.println(ds.getString("project_url")) ;
//
//        }
//        cursor.close();
//        Document doc = new Document() ;
//        double val = 5.623 ;
//        doc.put("project_id", "fagdalwepdaedadgeedagdeae") ;
//        doc.put("value", 234) ;
//        col.insertOne(doc);


//          Document doc = new Document() ;
//
//          doc.put("project_id", "234") ;
//        doc.put("project_id", "fagdalwepdaedadgeedagdeae") ;
//
//        ArrayList<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>() ;
//        HashMap<String,Object> a = new HashMap<String,Object>() ;
//        doc.put("project_id", "fagdalwepdaedadgeedagdeae") ;
//        doc.put("value", 234) ;
//        a.put("name","a1") ;
//        HashMap<String,Object> b = new HashMap<String,Object>() ;
//        b.put("name","a2") ;
//        list.add(a) ;
//        list.add(b) ;
//        doc.put("versions", list) ;
//        col.insertOne(doc);



//        ArrayList<Document> list = new ArrayList<Document>() ;
//        Document doc2 = new Document() ;
//
//        FindIterable ite = col.find(doc2) ;
//        doc2.put("project_id", "fagdalwepdaedadgeedagdeae") ;
//        MongoCursor<Document> cursor = ite.iterator() ;
//        int counter = 0 ;
//        while(cursor.hasNext()){
//            Document dc = cursor.next() ;
//            counter = counter + 1 ;
//
//            System.out.println(dc.getString("project_id")) ;
//            ArrayList<Document> li = (ArrayList<Document>)dc.get("versions") ;
//            for(Document dt:li){
//                System.out.println((String)dt.get("name")) ;
//
//            }
////            System.out.println(dc.toJson()) ;
//
//        }
//        System.out.println(counter) ;
//
//        cursor.close() ;




    }
}
