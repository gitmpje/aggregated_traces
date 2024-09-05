BASE <http://example.org/id/aggregated_traces/>
PREFIX : <http://example.org/def/ekg/aggregated_traces/>

INSERT {
  GRAPH <urn:ekg:directlyFollows> {
    ?Relation_DF a :DirectlyFollows , :DirectlyFollows_AggregatedEntity ;
      :source ?Event_t1 ;
      :target ?Event_t2 ;
      :amount ?amount ;
      :class ?AggregatedEntity .
  }
  
  GRAPH <urn:ekg:directlyPrecedes> {
    ?Relation_DP a :DirectlyPrecedes , :DirectlyPrecedes_AggregatedEntity ;
      :source ?Event_t2 ;
      :target ?Event_t1 ;
      :amount ?amount ;
      :class ?AggregatedEntity .
  }
}
WHERE {
  {
    SELECT ?AggregatedEntity ?Event_t2 (max(?time_earlier) AS ?time_prevEvent)
    WHERE {
      ?Event_t2 :entity|:parentEntity|:childEntity|
          (:inputQuantity/:class) ?AggregatedEntity ;
        :timestamp ?time .

      {
        [] :entity|:parentEntity ?AggregatedEntity ;
          :timestamp ?time_earlier .
      } UNION {
        # Only consider child entity for 'split' events
        [] a :Aggregation ;
          :action "DELETE" ;
          :childEntity ?AggregatedEntity ;
          :timestamp ?time_earlier .
      } UNION {
        # Only consider output entity for Transformation events
        [] a :Transformation ;
          :outputQuantity/:class ?AggregatedEntity ;
          :timestamp ?time_earlier .
      }
      FILTER( ?time_earlier < ?time )

      ?AggregatedEntity a :AggregatedEntity .
    }
    GROUP BY ?AggregatedEntity ?Event_t2
  }

  {
    # Aggregation - ADD
    ?Event_t2 a :Aggregation ;
      :action "ADD" .
    {
      ?Event_t2 :parentEntity ?AggregatedEntity .
      ?Event_t1 :timestamp ?time_prevEvent ;
        :quantity|:childQuantity|:outputQuantity [
          :class ?AggregatedEntity ;
          :amount ?amount ;
        ] .
    } UNION {
      ?Event_t2 :childQuantity [
          :class ?AggregatedEntity ;
          :amount ?amount ;
        ] .
      ?Event_t1 :timestamp ?time_prevEvent ;
        :entity|:parentEntity|:childEntity|(:outputQuantity/:class) ?AggregatedEntity  .
    }
  } UNION {
    # Object or Aggregation - DELETE
    {
      ?Event_t2 a :Object ;
        :entity ?AggregatedEntity .
    } UNION {
      ?Event_t2 a :Aggregation ;
        :action "DELETE" ;
        :parentEntity ?AggregatedEntity .
    }
    ?Event_t1 :timestamp ?time_prevEvent ;
      :quantity|:childQuantity|:outputQuantity [
        :class ?AggregatedEntity ;
        :amount ?amount ;
      ] .
  } UNION {
    # Transformation
    ?Event_t2 a :Transformation ;
      :inputQuantity [
        :class ?AggregatedEntity ;
        :amount ?amount ;
      ] .

    ?Event_t1 :timestamp ?time_prevEvent ;
      (:quantity|:childQuantity|:outputQuantity)/:class ?AggregatedEntity .
  }

  BIND ( iri(md5(concat(str(?Event_t1), str(?Event_t2), str(?AggregatedEntity)))) as ?Relation_DF )
  BIND ( iri(md5(concat(str(?Event_t2), str(?Event_t1), str(?AggregatedEntity)))) as ?Relation_DP )
}
