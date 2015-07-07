/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /**
  * Original is available at https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/spout/RandomSentenceSpout.java
  */

package com.ayscom.minetur.frankestain_tree.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.ayscom.minetur.complex.XDRS;
import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

//This spout randomly emits sentences
public abstract class FrankestainAdapterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Document> queue;
    private final AtomicBoolean opened = new AtomicBoolean(false);

    //private  MongoCollection collection = null;
    private MongoDatabase mongoDB;
    private final Document query;
    private  MongoCollection collection = null;
    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final String mongoCollectionName;

    public FrankestainAdapterSpout(String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName, Document query) {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
        this.mongoCollectionName = mongoCollectionName;
        this.query = query;
       // this.collection=mongo.getCollection( mongoCollectionName );
    }

    class TailableCursorThread extends Thread {
        MongoCollection collection;
        LinkedBlockingQueue<Document> queue;
        String mongoCollectionName;
        MongoDatabase mongoDB;
        Document query;

        public TailableCursorThread(LinkedBlockingQueue<Document> queue, MongoDatabase mongoDB, String mongoCollectionName, Document query) {

            this.queue = queue;
            this.mongoDB = mongoDB;
            this.mongoCollectionName = mongoCollectionName;
            this.query = query;
            this.collection=this.mongoDB.getCollection( mongoCollectionName );
        }

        public void run() {

            while(opened.get()) {
                try {
                    // create the cursor
                   // mongoDB.requestStart();

                   // collection =this.mongoDB.getCollection();
                    int length = (int) this.collection.count();
                  // String[] xdrs;
                    //xdrs=new String[length];
                    int cont = 0;
                    Document allxdrs= new Document();
                    //String xdr_crudo="";

                   // MongoCursor

                    /*
                    final DBCursor cursor = mongoDB.getCollection(mongoCollectionName)
                   // MongoCursor<Document> cursor = collection.find().iterator()
                            .find(query)
                            .sort(new BasicDBObject("$natural", 1))
                            .addOption(Bytes.QUERYOPTION_TAILABLE)
                            .addOption(Bytes.QUERYOPTION_AWAITDATA);*/
                    MongoCursor<Document> cursor = collection.find().iterator();
                    try {

                        ObjectId obj3= new ObjectId();
                        String format= "dd/MM/yyyy HH:mm:ss";
                        while (opened.get() && cursor.hasNext()) {

                                final Document doc = cursor.next();

                                if(doc == null) break;


                                //xdr.makePojoFromBson(cursor.next());
                              //  String xdrcrudo= "";

                            ObjectId obj2= ( ObjectId ) doc.get( "_id" );
                            String protocol= (String) doc.get( "Protocol" );
                            String end_time= (String) doc.get("End Time");
                            String st="End Time";
                            //if(doc.get( "Protocol" )=="Iu-CS/A-Interface"){
                            if(end_time==null){
                                st="End Time Correlation";
                            }

                            XDRS xdr = new XDRS();
                            //setDate( ( String ) b.get( "Start Time" ),format)

                            Document document = new Document("_id", obj2.toString())
                                   // .append("MSISDN", xdr.getMSISDN())
                                    //.append("IMEI", xdr.getIMEI())
                                    //.append("IMSI",xdr.getIMSI())
                                    .append("idGroup", obj3.toString())
                                    .append("xdr_crudo", "Datos crudos xdrs" /*doc*/)
                                    .append("end_time",  xdr.setDate((String) doc.get(st), format))
                                    .append("start_time", xdr.setDate((String) doc.get("Start Time"), format));

                               // xdrs[cont]= document.toJson();

                                ObjectId obj= new ObjectId();
                          if(cont<4) {
                                allxdrs.append(obj.toString(), document);
                                cont++;
                            }
                                //xdr_crudo= xdr_crudo.concat("*").concat(xdrs[cont].getXDRCrudo());
                                // System.out.println(cursor.next().toJson());


                               // queue.put(docum);
                               // queue.put(xdr.bsonFromPojo());
                        }
                        //queue.put(xdrs);
                        queue.put(allxdrs);
                       // XDRS xdrs1 = new XDRS();
                        //xdrs1.generateId();
                       // xdrs1.generateIdUser();

                       /* Document document = new Document("_id",xdrs1.getId())
                                .append("MSISDN", "34633998875")
                                .append("IMEI", "865116010172474")
                                .append("IMSI", "214040101801031")
                                .append("idGroup", xdrs[1].getIdGroup())
                                .append("xdr_crudo", xdr_crudo)
                                //.append("info_user", new Document("id_user", xdrs1.getIdUser()).append("name_user", "Ruben"));
                        queue.put(document);*/


                  /*      if(cont< length) {

                                            //.append("Protocol", protocol)
                                            //.append("Start Time", start_time)
                                            //.append("End Time", end_time)
                                    document.;


                            cont++;
                    }*/


                       // GsonBuilder builder = new GsonBuilder();
                        //Gson gson = builder.create();
                        //XDRS xdr1= new XDRS();
                        //queue.put(xdr1.bsonFromPojoArray(xdrs));

                      /*  while (opened.get() && cursor.hasNext()) {
                            final DBObject doc = cursor.next();

                            if (doc == null) break;

                            queue.put(doc);
                        }*/
                    } finally {
                        try {
                            if (cursor != null) cursor.close();

                        } catch (final Throwable t) { }
                       /* try {

                        } catch (final Throwable t) { }*/
                    }

                    Utils.sleep(500);
                } catch (final MongoException cnf) {
                    // rethrow only if something went wrong while we expect the cursor to be open.
                    if (opened.get()) {
                        throw cnf;
                    }
                } catch (Exception e) { break; }
            }
        };
    }





    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.queue = new LinkedBlockingQueue<>(1000);
        try {
            this.mongoDB = new MongoClient(this.mongoHost, this.mongoPort).getDatabase(this.mongoDbName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TailableCursorThread listener = new TailableCursorThread((LinkedBlockingQueue<Document>) this.queue, this.mongoDB, this.mongoCollectionName, this.query);
        this.opened.set(true);
        listener.start();
    }

    @Override
    public void close() {
        this.opened.set(false);
    }

    @Override
    public void ack(Object msgId) {
        // TODO Auto-generated method stub
        System.out.println("OK this group had was processed: "+msgId);
    }

    @Override
    public void fail(Object msgId) {
        // TODO Auto-generated method stub
        System.out.println("FAIL this group didn't was processed:"+msgId);
    }

    public abstract List<Object> dbObjectToStormTuple(Document message);


    @Override
    public void nextTuple() {

       Document doc = this.queue.poll();
        Gson gson= new Gson();
        String docs = gson.toJson(doc);

        if(doc == null) {
            Utils.sleep(50);
        }else{

            ObjectId obj= new ObjectId();
            this.collector.emit("xdrs_spout", new Values(obj.toString(), docs ),obj.toString());
        }
    }

    }





    /*
  //Collector used to emit output
  SpoutOutputCollector _collector;
  //Used to generate a random number
  Random _rand;
  XDRS[] xdrs;
  MongoComplexArrays complex;
  private boolean completed = false;

  //Open is called when an instance of the class is created
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
  //Set the instance collector to the one passed in
    _collector = collector;
    //For randomness
    _rand = new Random();
   // complex = new MongoComplexArrays();
  }



  //Emit data to the stream
  @Override
  public void nextTuple() {

    if(completed){
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
            //Do nothing
        }
      return;
    }

    complex = new MongoComplexArrays();
    xdrs= complex.setup();

      try {

          xdrs= complex.setup();

          if(xdrs!=null){

              this._collector.emit(new Values("xdrs"),xdrs[_rand.nextInt(xdrs.length)].bsonFromPojo().toJson());
          }
      }catch (Exception e){
          throw new RuntimeException("Error reading tuple",e);
      }finally{
          completed =true;
      }

  //Sleep for a bit
    Utils.sleep(100);
    //The sentences that will be randomly emitted
    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
    //Randomly pick a sentence
    //
    String sentence = sentences[_rand.nextInt(sentences.length)];

    //xdrs= complex.setup();

    //Emit the sentence
   // _collector.emit(new Values(xdrs[_rand.nextInt(xdrs.length)].getProtocol()));
    _collector.emit(new Values(sentence));



  }


//
  //Ack is not implemented since this is a basic example
  @Override
  public void ack(Object id) {
      System.out.println("OK:"+id);
  }

  //Fail is not implemented since this is a basic example
  @Override
  public void fail(Object id) {
      System.out.println("FAIL:"+id);
  }

  //Declare the output fields. In this case, an sentence
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("xdrs"));
  }

    */

