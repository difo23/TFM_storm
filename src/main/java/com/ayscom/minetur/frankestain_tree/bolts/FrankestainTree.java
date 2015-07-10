package com.ayscom.minetur.frankestain_tree.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.util.JSON;
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
      Object id_ini= new Object();
      Object id= new Object();
      Object xdrs = new Object();



      int index=0;
      try{
          if(tuple.getSourceStreamId().equals("xdrs_spout")){
              id_ini = user.get(0);
              //id=null;
              xdrs =  user.get(1);

              Gson gson= new Gson();
             // doc_xdr2.toString();
              Document obj = gson.fromJson( xdrs.toString(), Document.class);
              Collection<Object> objv= obj.values();
              index=objv.toArray().length;

              if(index<1)
              {
                  System.out.println("frankestaintree xdrs_spout: *************** Este grupo posee "+index+" xdrs no se puede corralar. Vale! ***********");
                  //this.collector.fail(tuple);
                  this.collector.ack(tuple);

              } else{
                  System.out.println("frankestaintree xdrs_spout: *************** Este grupo posee "+index+" xdrs iniciamos frank. Vale! ***********");
                  this.collector.emit("franktree_bolt", tuple, new Values(id_ini,"INICIO", xdrs));
                  this.collector.ack(tuple);
              }

          }else if(tuple.getSourceStreamId().equals("franksplitsonsder_bolt")){
              id_ini= user.get(0);
              id= user.get(1);
              xdrs = user.get(2);
              String xdrim= (String) user.get(2);
              System.out.println("frankestaintree franksplitsonsder_bolt: *************** Grupo original: "+ id_ini.toString()+" Estoy en franktree recibo xdrs derecha. Vale! *********** "+id);
              BasicDBList dbObject = (BasicDBList) JSON.parse(xdrim);


              if(dbObject.size()<1)
              {
                  System.out.println("frankestaintree franksplitsonsder_bolt:  *************** Este grupo posee "+dbObject.size()+" xdrs no se puede corralar. Vale! ***********");
                  //this.collector.fail(tuple);
                  this.collector.ack(tuple);

              } else{
                  System.out.println("frankestaintree franksplitsonsder_bolt : *************** Este grupo posee "+dbObject.size()+" xdrs iniciamos frank. Vale! ***********");
                  this.collector.emit("franktree_split_bolt", tuple, new Values(id_ini,id, xdrs));
                  this.collector.ack(tuple);
              }

          }else if(tuple.getSourceStreamId().equals("franksplitsonsizq_bolt")){
              id_ini= user.get(0);
              id= user.get(1);
              xdrs = user.get(2);
              System.out.println("frankestaintree franksplitsonsizq_bolt: *************** Grupo original: "+id_ini+" Estoy en franktree recibo xdrs por la izq. Vale! *********** " +id );
              String xdrim= (String) user.get(2);
              BasicDBList dbObject = (BasicDBList) JSON.parse(xdrim);


              if(dbObject.size()<1)
              {
                  System.out.println("frankestaintree franksplitsonsizq_bolt: *************** Este grupo posee "+dbObject.size()+" xdrs no se puede corralar. Vale! ***********");
                  //this.collector.fail(tuple);
                  this.collector.ack(tuple);

              } else{
                  System.out.println("frankestaintree franksplitsonsizq_bolt: *************** Este grupo posee "+dbObject.size()+" xdrs iniciamos frank. Vale! ***********");
                  this.collector.emit("franktree_split_bolt", tuple, new Values(id_ini,id, xdrs));
                  this.collector.ack(tuple);
              }


          }else{
              //System.out.println("*************** Este grupo posee "+index+" xdrs no se puede corralar. Vale! ***********");
              this.collector.fail(tuple);
          }

      } catch (IllegalArgumentException e){
          // Hacer algo con la Exception

      }

           if(false){
                    //this.collector.ack(tuple);
               }else{
               // this.collector.fail(tuple);
               }
  }

  //Declare that emitted tuples will contain a word field
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream ("franktree_bolt", new Fields("user","id_split","xdrs"));
      declarer.declareStream ("franktree_split_bolt", new Fields("user","id_split","xdrs"));
  }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }


   // public void execute(Tuple tuple) {

   // }
}
