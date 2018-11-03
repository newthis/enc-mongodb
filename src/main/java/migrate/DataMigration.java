package migrate;

import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList ;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Scanner ;

public class DataMigration {
    ExecutorService pool = Executors.newFixedThreadPool(200) ;
    public static int migrateData(com.mongodb.MongoClient fclient, String sourceDb, String sourceCol, com.mongodb2.MongoClient tclient, String targetDb, String targetCol, ArrayList<org.bson.Document> conditions, CountDownLatch ltch){



            int len = conditions.size() ;

            for(int i = 0 ; i < len ; i++){
                org.bson.Document sourceRange = conditions.get(i) ;

                String name = "thread_" + (i+1) ;
                MigrationTask task = new MigrationTask(name, fclient, sourceDb, sourceCol, tclient  , targetDb, targetCol, sourceRange, ltch) ;
                Thread target= new Thread(task) ;
                try{
                    target.start() ;
                }catch(Exception ite){
                    ite.printStackTrace();

                }



            }
            return 1 ;








    }

    public static ArrayList<org.bson.Document> loadQueryConditionFromFile(){
        ArrayList<org.bson.Document> lst = new ArrayList<org.bson.Document>() ;


       try{
           BufferedReader br = new BufferedReader(new FileReader("./conds.txt"));
           String contentLine = br.readLine();
           while (contentLine != null) {
               System.out.println(contentLine);

               org.bson.Document li = org.bson.Document.parse(contentLine) ;
               if(li != null){
                   lst.add(li) ;
               }
               contentLine = br.readLine();


           }
           br.close();
           return lst ;
       }catch(IOException e){

           e.printStackTrace();
           return lst ;
       }


    }


    public static void main(String [] args){

        System.out.println("please input the parameter: ") ;
        Scanner scaner = new Scanner(System.in) ;
        String line = scaner.nextLine() ;
        String [] temps = line.trim().split(" ") ;
        if(temps.length == 6){
            String [] srs = temps[0].trim().split(":") ;
            String ip = srs[0] ;
            int port = Integer.valueOf(srs[1]) ;
            String sourceDb = temps[1].trim() ;
            String sourceCol = temps[2].trim() ;

            String [] sss = temps[3].trim().split(":") ;
            String ip2 = sss[0] ;
            int port2 = Integer.valueOf(sss[1]) ;

            String targetDb = temps[4].trim() ;
            String targetCol = temps[5].trim() ;
            com.mongodb.ServerAddress ff = new com.mongodb.ServerAddress(ip, port) ;
            com.mongodb2.ServerAddress tt = new com.mongodb2.ServerAddress(ip2, port2) ;
            ArrayList<org.bson.Document> rss = loadQueryConditionFromFile() ;
            int len = rss.size() ;
            CountDownLatch cdl = new CountDownLatch(len) ;


            com.mongodb.MongoClient fclient = null ;
            com.mongodb2.MongoClient tclient = null ;
            com.mongodb2.MongoClientOptions customClientOptions2  = new com.mongodb2.MongoClientOptions.Builder().maxConnectionIdleTime(0).maxConnectionLifeTime(0).maxWaitTime(100000).connectionsPerHost(100).build() ;
            com.mongodb.MongoClientOptions customClientOptions  = new com.mongodb.MongoClientOptions.Builder().maxConnectionIdleTime(0).maxConnectionLifeTime(0).maxWaitTime(100000).connectionsPerHost(100).build() ;
            fclient = new com.mongodb.MongoClient(ff, customClientOptions) ;
            tclient = new com.mongodb2.MongoClient(tt, customClientOptions2) ;

            migrateData(fclient, sourceDb, sourceCol, tclient, targetDb, targetCol, rss, cdl) ;
            try{
                cdl.await();

                System.out.println("migration success") ;
            }catch(InterruptedException e){
                e.printStackTrace();
                System.out.println("migration failure") ;


            }finally{
                if(fclient !=null){
                    fclient.close();

                }
                if(tclient != null){
                    tclient.close();
                }

            }
        }else{
            System.out.println("parameter error!") ;

        }






//        try{
//            cdl.await() ;
//
//        }catch(Exception e){
//            e.printStackTrace();
//        }






    }

    static class MigrationTask implements Runnable{

        String sourceDb ;
        String sourceCol ;

        String targetDb;
        String targetCol ;


        org.bson.Document sourceRange ;
        com.mongodb.MongoClient fclient ;
        com.mongodb2.MongoClient tclient ;
        CountDownLatch latch ;
        String name ;

        MigrationTask(String name, com.mongodb.MongoClient fclient, String sourceDb, String sourceCol, com.mongodb2.MongoClient tclient  , String targetDb, String targetCol, org.bson.Document sourceRange, CountDownLatch latch){
            this.name = name ;
            this.sourceDb = sourceDb;
            this.sourceCol = sourceCol ;
            this.targetDb = targetDb ;
            this.targetCol = targetCol ;
            this.sourceRange = sourceRange ;
            this.fclient = fclient ;
            this.tclient = tclient ;
            this.latch = latch ;

        }
        public void run(){
            com.mongodb.client.MongoCollection ss = fclient.getDatabase(this.sourceDb).getCollection(sourceCol) ;

            com.mongodb2.client.MongoCollection tt = tclient.getDatabase(this.targetDb).getCollection(this.targetCol) ;
            com.mongodb.client.MongoCursor<org.bson.Document> mcr = null ;
            if(this.sourceRange != null){
                mcr = ss.find(this.sourceRange).batchSize(200).iterator() ;
            }else{
                mcr = ss.find().batchSize(200).iterator() ;

            }

            ArrayList<org.bson2.Document> list = new ArrayList<org.bson2.Document> () ;
            System.out.println("migration task "+this.name + " begin") ;
            while(mcr.hasNext()){
                org.bson.Document sty = mcr.next() ;
                org.bson.types.ObjectId oid = (org.bson.types.ObjectId)sty.get("_id") ;
                org.bson2.types.ObjectId nid = new org.bson2.types.ObjectId(oid.toHexString()) ;
                System.out.println(sty.getString("project_url")) ;
                org.bson2.Document nt = new org.bson2.Document();


                sty.remove("_id") ;
                nt.put("_id",nid) ;
                nt.putAll(sty);


//                Iterator<String> ite = sty.keySet().iterator() ;
//                while(ite.hasNext()){
//                    String ky = ite.next() ;
//                    if(!ky.equals("_id")){
//                        nt.put(ky, sty.get(ky)) ;
//
//                    }
//
//                }


                list.add(nt) ;
                if(list.size() > 0 && list.size() % 500 == 0){
                    try{
                        tt.insertMany(list);
                    }catch(Exception e){
                        System.out.println("insert converted document exception") ;
                        e.printStackTrace();
                    }

                    list.clear();
                }


            }


            if(list.size() > 0){
                try{
                    tt.insertMany(list);
                }catch(Exception e){
                    System.out.println("insert converted document exception") ;
                    e.printStackTrace();
                }


            }
            latch.countDown() ;
            System.out.println("migration task "+this.name + " end") ;




        }




    }



}
