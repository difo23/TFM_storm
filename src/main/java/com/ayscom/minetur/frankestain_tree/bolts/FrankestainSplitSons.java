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
import org.bson.types.ObjectId;

/**
 * Created by lramirez on 12/06/15.
 */

public class FrankestainSplitSons extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {

        List user = tuple.getValues();
        Object obj_user = new Object();
        //Object doc_xdr = new Object();
        //Object[] xdrs;
        //DBObject[] docs = new DBObject[0];
        Object xdrs_derecha = new Object();
        Object xdrs_izquierda = new Object();
        //Vector<DBObject> xdrs_pm = new Vector<DBObject>();
        //Date pm = new Date();

        try {
            if (tuple.getSourceStreamId().equals("franksearchpm_split_bolt")) {

                obj_user = user.get(0);
                //doc_xdr = user.get(1);
                xdrs_derecha =   user.get(2);
                xdrs_izquierda =  user.get(3);
                 System.out.println("FrankestaintSplitSons franksearchpm_split_bolt: ++++++++ estoy en splitson " );
                // System.out.println("++++++++der " + xdrs_derecha.toString());
                // System.out.println("++++++++fecha arreglo pm " + JSON.serialize(xdrs_derecha));

            }

            if (tuple.getSourceStreamId().equals("frankim_split_bolt")) {

                obj_user = user.get(0);
                //doc_xdr = user.get(1);
                xdrs_derecha =   user.get(2);
                xdrs_izquierda =  user.get(3);
                System.out.println("FrankestaintSplitSons franksearchpm_split_bolt: ++++++++ estoy en splitson " );
                // System.out.println("++++++++der " + xdrs_derecha.toString());
                // System.out.println("++++++++fecha arreglo pm " + JSON.serialize(xdrs_derecha));
            }

        } catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        if (!(xdrs_derecha==null)) {
            ObjectId id_der= new ObjectId();
            System.out.println("FrankestaintSplitSons franksplitsonsder_bolt: *************** xdrs iniciamos franktree recursivo por la derecha (PM o IM). Vale! ***********");
            this.collector.emit("franksplitsonsder_bolt", tuple, new Values(obj_user, id_der.toString(),  xdrs_derecha));
            this.collector.ack(tuple);

        }else{
            this.collector.fail(tuple);
        }

        if(!(xdrs_izquierda==null)) {
            ObjectId id_izq= new ObjectId();
            System.out.println("FrankestaintSplitSons franksplitsonsizq_bolt: *************** xdrs iniciamos franktree recursivo por la izquierda (PM o IM). Vale! ***********");
            this.collector.emit("franksplitsonsizq_bolt", tuple, new Values(obj_user, id_izq.toString(), xdrs_izquierda));
            this.collector.ack(tuple);
        }else{
            this.collector.fail(tuple);
        }
    }

    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("franksplitsonsder_bolt", new Fields("user_id", "id_der", "xdrs_der"));
        declarer.declareStream("franksplitsonsizq_bolt", new Fields("user_id", "id_izq", "xdrs_izq"));
       // declarer.declareStream("franksearchpm_split_bolt", new Fields("user_id", "xdrs", "xdrs_derecha", "xdrs_izquierda"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
    }

}
