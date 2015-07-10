package com.ayscom.minetur.frankestain_tree.bolts;


import java.text.BreakIterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by lramirez on 12/06/15.
 */

public class FrankestainSaveGroup extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        try {

            if (tuple.getSourceStreamId().equals("frankIM_bolt")) {

            }else{


            }


        }catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }
    }

    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
