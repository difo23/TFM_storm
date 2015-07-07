package com.ayscom.minetur.frankestain_tree.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.List;
import java.util.Map;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class FrankestainTree extends BaseRichBolt {
    private OutputCollector collector;
  //Execute is called to process tuples
  @Override
  public void execute(Tuple tuple) {
    //Get the sentence content from the tuple
      //Long user;
      String xdrs_crudos;
      List user=tuple.getValues();;
      Object obj_user= new Object();
      Object doc_xdr = new Object();

      //String doc_xdr;
     // Document xdr;
      int index=0;
   // String sentence = tuple.getString(0);
    //String xdrs = tuple.toString();

      try{
          if(tuple.getSourceStreamId().equals("xdrs_spout")){
              //user= (ObjectId) tuple.getStringByField("user_id").toString();0
              //xdrs= (Document) tuple.getStringByField("xdrs_arrays");1
              //xdr=tuple.getStringByField("xdrs_arrays");
              //user= tuple.getValues();
              //index =;//user.fieldIndex("user_id");
              obj_user= user.get(0);
              doc_xdr=  user.get(1);
              Gson gson= new Gson();
             // doc_xdr2.toString();
              Document obj = gson.fromJson( doc_xdr.toString(), Document.class);
              Collection<Object> objv= obj.values();




              //array = gson.fromJson( objv.toString(), (Class<T>) String.class);

             // Document xdrs= new Document();
            //
            // doc_xdr2= new Document();
              //System.out.println("***************user:"+obj_user);
              //System.out.println("***************xdrs:"+doc_xdr);

             // this.collector.emit("franktree_bolt", tuple, new Values(obj_user, user.get(1)));
             // this.collector.ack(tuple);

              /*//this.collector.emit("franktree_bolt", new Values(obj_user, doc_xdr));
              System.out.println("***************User:" + obj_user.toString());
             // System.out.println("***************xdrs:"+doc_xdr2.toString());
              System.out.println("***************xdrs:" + objv.toArray()[0].toString());
              System.out.println("***************xdrs:" + objv.toArray()[1].toString());
              System.out.println("***************xdrs:"+objv.toArray()[2].toString());
              System.out.println("***************xdrs:"+objv.toArray().length);
              //System.out.println("***************xdrs:"+objv.toArray()[3].toString());
                //
            */
              index=objv.toArray().length;
             // System.out.println("***************xdr_crudo:"+doc_xdr.getString("xdr_crudo").toString());
             // System.out.println("***************IMSI:"+doc_xdr.getString("IMSI").toString());

             /* ***************xdrs:{"5593f60e88cc3164883b4b3d":{"_id":"5565b4ece0f687668b056d73",
             "idGroup":"5593f60e88cc3164883b4b3c","xdr_crudo":"Datos crudos xdrs","end_time":"Apr 27, 2015 2:00:56 PM",
             "start_time":"Apr 27, 2015 2:00:37 PM"},"5593f60e88cc3164883b4b3e":{"_id":"5565b4ece0f687668b056d74","idGroup":
             "5593f60e88cc3164883b4b3c","xdr_crudo":"Datos crudos xdrs","end_time":"Apr 27, 2015 2:00:56 PM","start_time":"Apr 27,
              2015 2:00:37 PM"},"5593f60e88cc3164883b4b3f":{"_id":"5565b4ece0f687668b056d75","idGroup":"5593f60e88cc3164883b4b3c",
              "xdr_crudo":"Datos crudos xdrs","end_time":"Apr 27, 2015 1:43:59 PM","start_time":"Apr 27, 2015 1:43:38 PM"}}
                */
          }else{

          }

      } catch (IllegalArgumentException e){
          // Hacer algo con la Exception

      }
      //logica del bolt1 FrankestainTree

    if(index<1)
    {
        System.out.println("*************** Este grupo posee "+index+" xdrs no se puede corralar. Vale! ***********");
        this.collector.fail(tuple);

    } else{
        System.out.println("*************** Este grupo posee "+index+" xdrs iniciamos frank. Vale! ***********");
        this.collector.emit("franktree_bolt", tuple, new Values(obj_user, user.get(1)));
        this.collector.ack(tuple);
    }





      //Fin de la logica del bolt1

      /* Envio al final del bolt
      this.collector.emit("franktree_bolt", tuple, new Values(obj_user, user.get(1)));
      this.collector.ack(tuple);
      */
      //Logica de franktree bolt

     // System.out.println("FAIL this word didn't was processed:"+doc_xdr.getString("IMEI"));

   // String fields= tuple;
    //An iterator to get each word
   // BreakIterator boundary=BreakIterator.getWordInstance();
    //Give the iterator the sentence
   // boundary.setText(sentence);
    //Find the beginning first word
    //int start=boundary.first();
    //Iterate over each word and emit it to the output stream
    //for (int end=boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
      //get the word
      //String word=sentence.substring(start,end);
      //If a word is whitespace characters, replace it with empty
      //word=word.replaceAll("\\s+","");
      //if it's an actual word, emit it
      //if (!word.equals("")) {
       //this.collector.emit(tuple, new Values(/*xdrs*/));
           if(false){
                    //this.collector.ack(tuple);
               }else{
               // this.collector.fail(tuple);
               }

     // }
    //}
  }

  //Declare that emitted tuples will contain a word field
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream ("franktree_bolt", new Fields("user","xdrs"));
  }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }


   // public void execute(Tuple tuple) {

   // }
}
