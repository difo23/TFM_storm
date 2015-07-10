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
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.Document;

import javax.swing.text.html.HTMLDocument;
import java.util.*;

/**
 * Created by lramirez on 12/06/15.
 */

public class FrankestainSetInterval extends BaseRichBolt {

    private OutputCollector collector;
    @Override
    public void execute(Tuple tuple) {
        //Get the sentence content from the tuple
        //String sentence = tuple.getStringByField("xdrs");
        List user=tuple.getValues();;
        Object obj_user= new Object();
        Object doc_xdr= new Object();
        Object[] xdrs;
        DBObject[] docs = new DBObject[0];
        Date end= new Date();
        Date start=new Date();
        Date str= new Date();

        Object id_ini= new Object();
        Object id= new Object();
        Object xdrs_split = new Object();

        // Document xdr;
        int index;

        try{
            if(tuple.getSourceStreamId().equals("franktree_bolt")){
                obj_user= user.get(0);
                doc_xdr=  user.get(2);
                Gson gson= new Gson();
                // doc_xdr2.toString();
                Document obj = gson.fromJson(doc_xdr.toString(), Document.class);
                obj.get(obj_user);
                Collection<Object> objv= obj.values();
                xdrs= objv.toArray();
                String st= gson.toJson(objv);
                Document doc = new Document();
                doc.append("xdrs",st);
                Iterator it =objv.iterator();
               //doc.get("xdr");

                docs= new DBObject[xdrs.length];
               // System.out.println("*************** Xdrs:  contenido antes del for ");
                Document doc2 = new Document();
                String st1;
                for(int i=0;i<xdrs.length;i++){
                    st1= gson.toJson(it.next());
                    DBObject dbObject= (DBObject) JSON.parse(st1);
                    docs[i]=dbObject;
                }

                end= new Date(docs[0].get("end_time").toString());
                start = new Date(docs[0].get("start_time").toString());

                for(int i=0; i<docs.length;++i){
                    Date date_start=  new Date(docs[i].get("start_time").toString());
                    Date date_end=  new Date(docs[i].get("end_time").toString());
                    if(start.compareTo(date_start)==1) {
                        start =date_start;
                    }
                    if( end.compareTo(date_end)==-1) {
                        end=date_end;
                    }
                }
                Long diferencia = end.getTime()-start.getTime();
                // System.out.println(diferencia);
                Long div= (diferencia/(2))+start.getTime() ;
                // System.out.println(div);
                Date pm = new Date(div);

               // String xdrim= (String) user.get(2);
                //BasicDBObject dbObject = (BasicDBObject ) JSON.parse(xdrim);

                if( pm != null/*index<1*/)
                {
                    System.out.println("Frankestainsetinterval frankSetInt_bolt: *************** Este grupo posee punto medio "+pm.toString()+" iniciamo separacion por PM. Vale! ***********");
                    this.collector.emit("frankSetInt_bolt", tuple, new Values(obj_user, user.get(2), pm));
                    this.collector.ack(tuple);

                } else{
                    System.out.println("Frankestainsetinterval frankSetInt_bolt: *************** Existe algun error al calcular el punto medio. Vale! ***********");
                    this.collector.fail(tuple);
                }


            }else{

            }


            if(tuple.getSourceStreamId().equals("franktree_split_bolt")){
                id_ini= user.get(0);
                id= user.get(1);
                xdrs_split = user.get(2);
                String xdrim= (String) user.get(2);
                System.out.println("Frankestainsetinterval franktree_split_bolt: *************** Grupo original: "+ id_ini+" Estoy en franktreeinterval recibo xdrs. Vale! *********** "+id);
                BasicDBList dbObject = (BasicDBList) JSON.parse(xdrim);
                DBObject dbObt = null;
                DBObject dbObt2 = (DBObject) (dbObject.get(0));
                int count=1;
                start= new Date (dbObt2.get("start_time").toString());
                end= new Date (dbObt2.get("end_time").toString());
                String total= new String();
                total=dbObt2.toString();

               while(count<dbObject.size()) {
                   dbObt = (DBObject) (dbObject.get(count));
                   Date date_start=new Date( dbObt.get("start_time").toString());
                   Date date_end= new Date( dbObt.get("end_time").toString());

                   if(start.compareTo(date_start)==1) {
                       start =date_start;
                   }
                   if( end.compareTo(date_end)==-1) {
                       end=date_end;
                   }

                    total+=","+dbObt.toString();
                    //dbObt = (DBObject) (dbObject.get(count));
                    System.out.println("Frankestainsetinterval franktree_split_bolt: -------Elemento a enviar grupo divido " + dbObt.get("_id")+" date start "+date_start.toString()+" Date end"+date_end.toString());
                   ++count;

                }

                System.out.println("Frankestainsetinterval franktree_split_bolt: -------Elementos a enviar grupo divido " + dbObject.size());
                Long diferencia = end.getTime()-start.getTime();
                // System.out.println(diferencia);
                Long div= (diferencia/(2))+start.getTime() ;
                // System.out.println(div);
                Date pm = new Date(div);
                System.out.println("Frankestainsetinterval franktree_split_bolt: -------Elementos a enviar grupo divido " +pm.toString());

                if( pm != null/*index<1*/)
                {
                    System.out.println("Frankestainsetinterval frankSetInt_split_bolt: *************** Este grupo posee punto medio "+pm.toString()+" iniciamo separacion por PM. Vale! ***********");
                    this.collector.emit("frankSetInt_split_bolt", tuple, new Values(id_ini, xdrs_split , pm));
                    this.collector.ack(tuple);

                } else{
                    System.out.println("Frankestainsetinterval franktree_split_bolt: *************** Existe algun error al calcular el punto medio. Vale! ***********");
                    this.collector.fail(tuple);
                }


               /* str= new Date("Apr 27, 2015 1:42:42 PM");
                System.out.println("*************** Este grupo posee punto medio "+str.toString()+" franktree_split_bolt iniciamo separacion por PM. Vale! ********");

                this.collector.ack(tuple);*/


            }else{

            }

        } catch (IllegalArgumentException e){
            // Hacer algo con la Exception

        }


    }

    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("frankSetInt_bolt", new Fields("user_id", "xdrs", "pm"));
        declarer.declareStream("frankSetInt_split_bolt", new Fields("user_id", "xdrs", "pm"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
    }


}
