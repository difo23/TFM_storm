package com.ayscom.minetur.frankestain_tree.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
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
        DBObject[] docs=new DBObject[0];
        Date end= new Date();
        Date start=new Date();
        // Document xdr;
        int index;


        try{
            if(tuple.getSourceStreamId().equals("franktree_bolt")){

                obj_user= user.get(0);
                doc_xdr=  user.get(1);
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

                    if(start.compareTo(date_start)==1)
                    {
                        start =date_start;
                    }

                    if( end.compareTo(date_end)==-1)
                    {
                        end=date_end;
                    }

                }


            }else{

            }

        } catch (IllegalArgumentException e){
            // Hacer algo con la Exception

        }

      //  System.out.println(end);
       // System.out.println(start);
        Long diferencia = end.getTime()-start.getTime();
       // System.out.println(diferencia);
        Long div= (diferencia/(2))+start.getTime() ;
       // System.out.println(div);
        Date pm= new Date(div);
         System.out.println("dddddddddddddddddddddd pm "+pm);

        index=1;
        if( pm != null/*index<1*/)
        {
            System.out.println("*************** Este grupo posee punto medio "+pm.toString()+" iniciamo separacion por PM. Vale! ***********");
            this.collector.emit("frankSetInt_bolt", tuple, new Values(obj_user, user.get(1), pm));
            this.collector.ack(tuple);

        } else{
            System.out.println("*************** Existe algun error al calcular el punto medio. Vale! ***********");
            this.collector.fail(tuple);
        }


       // sentence.split('\]'):
        //An iterator to get each word
        //BreakIterator boundary=BreakIterator.getWordInstance();
        //Give the iterator the sentence
        /*boundary.setText(sentence);
        //Find the beginning first word
        int start=boundary.first();
        //Iterate over each word and emit it to the output stream
        for (int end=boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
            //get the word
            String word=sentence.substring(start,end);
            //If a word is whitespace characters, replace it with empty
            word=word.replaceAll("\\s+","");
            //if it's an actual word, emit it
            if (!word.equals("")) {
                collector.emit(new Values(word));
            }
        }*/

      // for(int i= 0; array.length-1>i;i++) {
        //    collector.emit(new Values(array[i]));
        //}
       // this.collector.emit(new Values(sentence));
       /* if(false){
           // this.collector.ack(tuple);
        }else{
            //this.collector.fail(tuple);
        }*/

    }

    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("frankSetInt_bolt", new Fields("user_id", "xdrs", "pm"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
    }


}
