package com.ayscom.minetur.frankestain_tree.bolts;

import java.text.BreakIterator;
import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.Document;

/**
 * Created by lramirez on 12/06/15.
 */

public class FrankestainSearchPM extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {

        List user = tuple.getValues();
        Object obj_user = new Object();
        Object doc_xdr = new Object();
        Object[] xdrs;
        DBObject[] docs = new DBObject[0];
        Vector<DBObject> xdrs_derecha = new Vector<DBObject>();
        Vector<DBObject> xdrs_izquierda = new Vector<DBObject>();
        Vector<DBObject> xdrs_pm = new Vector<DBObject>();
        Date pm = new Date();

        try {
            if (tuple.getSourceStreamId().equals("frankSetInt_bolt")) {

                obj_user = user.get(0);
                doc_xdr = user.get(1);
                pm =  (Date) user.get(2);

                Gson gson = new Gson();
                Document obj = gson.fromJson(doc_xdr.toString(), Document.class);
                obj.get(obj_user);
                Collection<Object> objv = obj.values();
                xdrs = objv.toArray();
                Iterator it = objv.iterator();
                String st1;

                for (int i = 0; i < xdrs.length; i++) {
                    st1 = gson.toJson(it.next());
                    DBObject dbObject = (DBObject) JSON.parse(st1);
                    Date end_search= new Date(dbObject.get("end_time").toString());
                    Date start_search = new Date(dbObject.get("start_time").toString());

                    if((start_search.compareTo(pm) < 1 ) && (pm.compareTo(end_search)<1)){
                        xdrs_pm.add( dbObject);
                        System.out.println("++++++++Entra en Arreglo pm " + st1);
                    }else if((start_search.compareTo(pm) == 1 ))
                    {
                        xdrs_derecha.add( dbObject);
                        System.out.println("++++++++Entra en arrgelo de la derecha " + st1);
                    }else if((pm.compareTo(end_search) == 1 )){
                        xdrs_izquierda.add(dbObject);
                        System.out.println("++++++++Entra en arrgelo de la izq " + st1);
                    }

                }

               // System.out.println("++++++++fecha pm " + pm.toString());
               // System.out.println("++++++++fecha arreglo pm " + JSON.serialize(xdrs_derecha));

            } else {

            }

        } catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        if (/*!*/xdrs_pm.isEmpty()) {
            System.out.println("*************** Este grupo posee " + xdrs_pm.size() + " xdrs iniciamos frankim. Vale! ***********");
           // this.collector.emit("franksearchpm_bolt", tuple, new Values(obj_user, user.get(1), JSON.serialize(xdrs_pm)));
            this.collector.emit("franksearchpm_bolt", tuple, new Values(obj_user, user.get(1), JSON.serialize(xdrs_derecha)));
            this.collector.ack(tuple);

        } else {
            System.out.println("*************** No existen xdrs corralados por punto medio. Vale! ***********");
            this.collector.emit("franksearchpm_split_bolt", tuple, new Values(obj_user, user.get(1), JSON.serialize(xdrs_derecha), JSON.serialize(xdrs_izquierda)));
            this.collector.ack(tuple);
        }

    }

    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("franksearchpm_bolt", new Fields("user_id", "xdrs",  "xdrs_pm"));
        declarer.declareStream("franksearchpm_split_bolt", new Fields("user_id", "xdrs", "xdrs_derecha", "xdrs_izquierda"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
    }


}