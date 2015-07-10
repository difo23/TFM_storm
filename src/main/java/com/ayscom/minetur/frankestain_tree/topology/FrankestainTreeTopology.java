

package com.ayscom.minetur.frankestain_tree.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.ayscom.minetur.frankestain_tree.bolts.*;
import com.ayscom.minetur.frankestain_tree.spouts.FrankestainAdapterSpout;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

import java.util.List;

import static backtype.storm.utils.Utils.tuple;


public class FrankestainTreeTopology {
//
  //Entry point for the topology
  public static void main(String[] args) throws Exception {
  //Used to build the topology
    TopologyBuilder builder = new TopologyBuilder();

      MongoClient mongo = new MongoClient(new ServerAddress("localhost", 27017));
      final CreateCollectionOptions options = new CreateCollectionOptions();
      options.capped(true);
      options.sizeInBytes(10000);

      final MongoCollection coll = mongo.getDatabase("test").getCollection("xdrs_crudos");


      FrankestainAdapterSpout spout = new FrankestainAdapterSpout("localhost", 27017, "test", "xdrs_crudos", new Document()) {

          private static final long serialVersionUID = 1L;

          @Override
          public void declareOutputFields(OutputFieldsDeclarer declarer) {

              declarer.declareStream ("xdrs_spout", new Fields("user_id","xdrs_arrays"));
          }

          @Override
          public List<Object> dbObjectToStormTuple(Document document) {

              return tuple(document);
          }

      };
     // mongo.getDatabase("test").createCollection("xdrs_crudos_reducida", options);
      //mongo.dropDatabase("mongo_storm_tailable_cursor");
      //Add the spout, with a name of 'spout'
      //and parallelism hint of 5 executors


   builder.setSpout("frank_spout", spout);
    //Add the SplitSentence bolt, with a name of 'split'
    //and parallelism hint of 8 executors
    //shufflegrouping subscribes to the spout, and equally distributes
    //tuples (sentences) across instances of the SplitSentence bolt
    builder.setBolt("franktree_bolt_1", new FrankestainTree(),4).shuffleGrouping("frank_spout", "xdrs_spout").shuffleGrouping("frankSpSons_bolt_7","franksplitsonsder_bolt").shuffleGrouping("frankSpSons_bolt_7","franksplitsonsizq_bolt");
    // builder.setBolt("franktree_bolt_1", new FrankestainTree(), 4).fieldsGrouping("frank_spout", "xdrs", new Fields("fieldline"));
    // builder.setBolt("franktree_bolt_1", new FrankestainTree(), 1).shuffleGrouping("frank_spout").shuffleGrouping("frankSpSons_bolt_7");
    // Add the counter, with a name of 'count'
    //builder.setBolt("franktree_bolt_1", new FrankestainTree(), 1).shuffleGrouping("frank_spout").shuffleGrouping("frankSpSons_bolt_7");
    //Add the counter, with a name of 'count'
    //and parallelism hint of 12 executors
    //fieldsgrouping subscribes to the split bolt, and
    //ensures that the same word is sent to the same instance (group by field 'word')
    builder.setBolt("franksetint_bolt_2", new FrankestainSetInterval(),4).shuffleGrouping("franktree_bolt_1", "franktree_bolt").shuffleGrouping("franktree_bolt_1","franktree_split_bolt");

    builder.setBolt("franksearPM_bolt_3", new FrankestainSearchPM(), 4).shuffleGrouping("franksetint_bolt_2", "frankSetInt_bolt").shuffleGrouping("franksetint_bolt_2","frankSetInt_split_bolt");

    builder.setBolt("frankIM_bolt_4", new FrankestainIM(), 4).shuffleGrouping("franksearPM_bolt_3","franksearchpm_bolt");
/*
    builder.setBolt("frankSwit_bolt_5", new FrankestainSwitch(), 1).shuffleGrouping("frankIM_bolt_4");
*/
    builder.setBolt("frankSaGr_bolt_6", new FrankestainSaveGroup(), 4).shuffleGrouping("frankIM_bolt_4","frankIM_bolt");

    builder.setBolt("frankSpSons_bolt_7", new FrankestainSplitSons(), 4).shuffleGrouping("frankIM_bolt_4", "frankim_split_bolt").shuffleGrouping("franksearPM_bolt_3", "franksearchpm_split_bolt");

      //new configuration
    Config conf = new Config();
    conf.setDebug(true);

    //If there are arguments, we are running on a cluster
    if (args != null && args.length > 0) {
      //parallelism hint to set the number of workers
      //conf.setNumWorkers(3);
      //submit the topology
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

      /*
      Manera de ejecutarlo modo remoto con StormSubmitter
      storm jar target/WordCount-1.0-SNAPSHOT.jar com.ayscom.example.WordCountTopology Mytopology_name
      */

    }
    //Otherwise, we are running locally
    else {
      //Cap the maximum number of executors that can be spawned
      //for a component to 3
      //conf.setMaxTaskParallelism(3);
      //LocalCluster is used to run locally
      LocalCluster cluster = new LocalCluster();
      //submit the topology
      cluster.submitTopology("frankLocalTest", conf, builder.createTopology());
       /*
      Manera de ejecutarlo manera local  con LocalCluster, no se envia parametros.
      storm jar target/WordCount-1.0-SNAPSHOT.jar com.ayscom.example.WordCountTopology
      */
      //sleep
      Thread.sleep(9000);
      //shut down the cluster
      cluster.shutdown();
    }
  }
}
