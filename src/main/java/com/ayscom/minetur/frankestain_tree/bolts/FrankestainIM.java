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

public class FrankestainIM extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {
        List user = tuple.getValues();
        Object obj_user = new Object();
        Object doc_xdr = new Object();
        String xdrim = new String();
        String xdrim2 = new String();
        Object[] xdrs;
       // DBObject[] docs = new DBObject[0];
        BasicDBList xdrs_no_im = new BasicDBList();

        BasicDBList xdrs_derecha = new BasicDBList();
        BasicDBList xdrs_izquierda = new BasicDBList();

        //Vector<DBObject> xdrs_izquierda = new Vector<DBObject>();
        BasicDBList xdrs_im = new BasicDBList();
        Date pm = new Date();

        Object id_ini= new Object();
        Object xdr= new Object();
        Object xdrs_split = new Object();

        BasicDBList corraladosim = new BasicDBList();
        BasicDBList nocorraladosim = new BasicDBList();

        try {
            if (tuple.getSourceStreamId().equals("franksearchpm_bolt")) {

                id_ini = user.get(0);
                xdr = user.get(1);
                xdrs_split = user.get(2);
                xdrim = (String) user.get(2);
                xdrim2 = (String) user.get(1);

                System.out.println("FrankestainIM franksearchpm_bolt: --------Estoy en  frankim ");
                System.out.println("FrankestainIM franksearchpm_bolt: --------id_init " + id_ini.toString());
                System.out.println("FrankestainIM franksearchpm_bolt: -------- xdr no corralados pm: " + xdr.toString());
                System.out.println("FrankestainIM franksearchpm_bolt: -------- xdr corralados por pm: " + xdrim);
                BasicDBList corraladospm = (BasicDBList) JSON.parse(xdrim);
                BasicDBList nocorraladosxdrs = (BasicDBList) JSON.parse(xdrim2);

                System.out.println("FrankestainIM franksearchpm_bolt: -------- xdr no corralados pm size " + nocorraladosxdrs.size());

                System.out.println("FrankestainIM franksearchpm_bolt: --------  xdrs no corralados size: (" + nocorraladosxdrs.size() + ") xdrs corralados por pm size : (" + corraladospm.size()+")");
                if (nocorraladosxdrs.size() == 0) {

                    System.out.println("FrankestainIM franksearchpm_bolt: ************  correlados por punto medio proceder a guardar en base de datos: **** " + xdrim);
                    corraladosim= corraladospm;

                } else if (nocorraladosxdrs.size() >= 1) {
                    do {
                        xdrs_im= new BasicDBList();
                        xdrs_no_im= new BasicDBList();
                        xdrs_derecha = new BasicDBList();
                        xdrs_izquierda = new BasicDBList();
                        System.out.println("FrankestainIM franksearchpm_bolt: ************ Estos xdrs no estan correlados por PM proceder a corralar por IM: **** " + nocorraladosxdrs.toString());
                        Vector<BasicDBList> result = new Vector<BasicDBList>();


                        //start metodo result = frankeintervaloim(corraladospm, nocorraladosxdrs);

                        System.out.println("FrankestainIM frankeintervaloim metodo: ************ Estos xdrs no estan correlados por punto medio proceder a corralar por intervalo medio: **** " + nocorraladosxdrs.toString());
                        System.out.println("FrankestainIM frankeintervaloim metodo in : -------- nocorraladospm: " + nocorraladosxdrs.toString());
                        System.out.println("FrankestainIM frankeintervaloim metodo in : -------- corraladospm: " + corraladospm.toString());

                        DBObject dbObt = null;
                        DBObject dbObt2 = (DBObject) (corraladospm.get(0));
                        int count = 0;
                        Date start = new Date(dbObt2.get("start_time").toString());
                        Date end = new Date(dbObt2.get("end_time").toString());

                        while (count < corraladospm.size()) {
                            dbObt = (DBObject) (corraladospm.get(count));
                            Date date_start = new Date(dbObt.get("start_time").toString());
                            Date date_end = new Date(dbObt.get("end_time").toString());

                            if (start.compareTo(date_start) == 1) {
                                start = date_start;
                            }
                            if (end.compareTo(date_end) == -1) {
                                end = date_end;
                            }

                            ++count;
                        }


                        System.out.println("FrankestainIM frankeintervaloim metodo: ************ Encuentro el intervalo del grupo PM: **** TIME END:" + end.toString()+" START time: "+start.toString());

                        count = 0;
                        while (count < nocorraladosxdrs.size()) {

                            dbObt = (DBObject) (nocorraladosxdrs.get(count));
                            Date date_start = new Date(dbObt.get("start_time").toString());
                            Date date_end = new Date(dbObt.get("end_time").toString());

                            if ((end.getTime() >= date_start.getTime()) && (date_start.getTime() >= start.getTime())) {
                                xdrs_im.add(dbObt);
                                System.out.println("FrankestainIM frankeintervaloim metodo: ************ Entra en intervalo medio:"+dbObt.toString() );

                            } else if ((start.getTime() <= date_end.getTime()) && (date_end.getTime() <= end.getTime())) {
                                xdrs_im.add(dbObt);
                                System.out.println("FrankestainIM frankeintervaloim metodo: ************ Entra en intervalo medio:"+dbObt.toString() );
                            } else {
                                xdrs_no_im.add(dbObt);
                                if((start.getTime() > date_end.getTime()))
                                {
                                    xdrs_derecha.add( dbObt);

                                    System.out.println("FrankestainIM frankeintervaloim metodo: ++++++++Entra en arrgelo de la derecha " +dbObt.toString());
                                }else if((end.getTime() < date_start.getTime())){
                                    xdrs_izquierda.add(dbObt);
                                    System.out.println("FrankestainIM frankeintervaloim metodo: ++++++++Entra en arrgelo de la izq " +dbObt.toString());
                                }

                                System.out.println("FrankestainIM frankeintervaloim metodo: ************ NO! Entra en intervalo medio:"+dbObt.toString() );
                            }
                            ++count;
                        }


                        result.add(xdrs_im);
                        result.add(xdrs_no_im);


                        System.out.println("FrankestainIM frankeintervaloim metodo out : -------- resultado: " + result.toString());

                        //end metodo
                        if (xdrs_im.size() > 0) {
                            int cont = 0;
                            while (cont < xdrs_im.size()) {
                                corraladospm.add(corraladospm.size(), xdrs_im.get(cont));
                                ++cont;
                             }
                        }


                                //result = frankeintervaloim(corraladospm, nocorraladosxdrs);
                                corraladosim = corraladospm;
                                nocorraladosxdrs = nocorraladosim = xdrs_no_im;

                        //corraladosim = result.get(0);
                        //nocorraladosim = result.get(1);

                        System.out.println("xxx FrankestainIM franksearchpm_bolt: ************ Repuesta de metodo corraladosim corralados por IM: **** " + corraladosim.toString() + " size:" + corraladosim.size());
                        System.out.println("xxx FrankestainIM franksearchpm_bolt: ************ Repuesta de metodo corraladosim  no corralados por IM: **** " + nocorraladosim.toString() + " size:" + nocorraladosim.size());
                    }while(xdrs_im.size() > 0);


                }




            } else {

            }

        } catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        if (!corraladosim.isEmpty()) {
            System.out.println("FrankestainIM frankIM_bolt: *************** Este grupo posee " + corraladosim.size() + " xdrs iniciamos frankson. Vale! ***********");
            this.collector.emit("frankIM_bolt", tuple, new Values(id_ini, JSON.serialize(nocorraladosim), JSON.serialize(corraladosim)));
            this.collector.emit("frankim_split_bolt", tuple, new Values(id_ini, user.get(1), JSON.serialize(xdrs_derecha), JSON.serialize(xdrs_izquierda)));
            this.collector.ack(tuple);

        } else {
            System.out.println("FrankestainIM frankIM_bolt: *************** No existen xdrs corralados por intervalo medio. Vale! ***********");
            //this.collector.emit("franksearchpm_split_bolt", tuple, new Values(obj_user, user.get(1), JSON.serialize(xdrs_derecha), JSON.serialize(xdrs_izquierda)));
            //this.collector.emit("frankIM_bolt", tuple, new Values(obj_user, JSON.serialize(corraladosim))
            this.collector.ack(tuple);
        }

    }



    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("frankIM_bolt", new Fields("user_id", "xdrs",  "xdrs_im"));
        declarer.declareStream("frankim_split_bolt", new Fields("user_id", "xdrs", "xdrs_derecha", "xdrs_izquierda"));
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
    }
}
