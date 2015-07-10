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
import com.mongodb.BasicDBList;
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
        Vector<DBObject> xdrs_no_corralados = new Vector<DBObject>();
        Date pm = new Date();

        Object id_ini= new Object();
        Object id= new Object();
        Object xdrs_split = new Object();

        try {
            if (tuple.getSourceStreamId().equals("frankSetInt_bolt")) {

                 obj_user = user.get(0);
                 doc_xdr = user.get(1);
                pm =  (Date) user.get(2);
                String xdrim= (String) user.get(1);



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
                        System.out.println("FrankestainSearchPM frankSetInt_bolt: ++++++++Entra en Arreglo pm " + st1);
                    }else if((start_search.compareTo(pm) == 1 ))
                    {
                        xdrs_derecha.add( dbObject);
                        System.out.println("FrankestainSearchPM frankSetInt_bolt: ++++++++Entra en arrgelo de la derecha " + st1);
                    }else if((pm.compareTo(end_search) == 1 )){
                        xdrs_izquierda.add(dbObject);
                        System.out.println("FrankestainSearchPM frankSetInt_bolt: ++++++++Entra en arrgelo de la izq " + st1);
                    }

                }

               // System.out.println("++++++++fecha pm " + pm.toString());
               // System.out.println("++++++++fecha arreglo pm " + JSON.serialize(xdrs_derecha));

            } else {

            }

            if (tuple.getSourceStreamId().equals("frankSetInt_split_bolt")) {

                obj_user =id_ini= user.get(0);
                xdrs_split = user.get(1);
                pm =  (Date) user.get(2);
                String xdrim= (String) user.get(1);
                System.out.println("FrankestainSearchPM frankSetInt_split_bolt: --------+++++ entro en  frankSetInt_split_bolt este es el pm " + pm.toString());
                System.out.println("FrankestainSearchPM frankSetInt_split_bolt: *************** Grupo original: "+ id_ini+" Estoy en frankpm frankSetInt_split_bolt. Vale! *********** ");
                BasicDBList dbObject = (BasicDBList) JSON.parse(xdrim);

                //DBObject dbObt = null;
                //DBObject dbObt2 = (DBObject) (dbObject.get(0));
                int count=0;
                while ( count < dbObject.size()) {

                    DBObject dbOb = (DBObject) (dbObject.get(count));
                    Date end_search= new Date(dbOb.get("end_time").toString());
                    Date start_search = new Date(dbOb.get("start_time").toString());

                    if((start_search.compareTo(pm) < 1 ) && (pm.compareTo(end_search)<1)){
                        xdrs_pm.add( dbOb);
                        System.out.println("FrankestainSearchPM frankSetInt_split_bolt: ++++++++Entra en Arreglo pm " +dbOb.toString()  );
                    }else if((start_search.compareTo(pm) == 1 ))
                    {
                        xdrs_derecha.add( dbOb);
                        xdrs_no_corralados.add( dbOb);
                        System.out.println("FrankestainSearchPM frankSetInt_split_bolt: ++++++++Entra en arrgelo de la derecha " +dbOb.toString());
                    }else if((pm.compareTo(end_search) == 1 )){
                        xdrs_izquierda.add(dbOb);
                        xdrs_no_corralados.add( dbOb);
                        System.out.println("FrankestainSearchPM frankSetInt_split_bolt: ++++++++Entra en arrgelo de la izq " +dbOb.toString());
                    }
                    ++count;

                }



            }


        } catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        if (!xdrs_pm.isEmpty()) {
            System.out.println("FrankestainSearchPM franksearchpm_bolt: *************** Este grupo posee " + xdrs_pm.size() + " xdrs corralados por pm iniciamos frankim. Vale! ***********");
            this.collector.emit("franksearchpm_bolt", tuple, new Values(obj_user, JSON.serialize(xdrs_no_corralados), JSON.serialize(xdrs_pm)));
            //this.collector.emit("franksearchpm_bolt", tuple, new Values(obj_user, user.get(1), JSON.serialize(xdrs_derecha)));

            this.collector.ack(tuple);

        } else {
            System.out.println("FrankestainSearchPM franksearchpm_split_bolt: *************** No existen xdrs corralados por punto medio. Vale! ***********");
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