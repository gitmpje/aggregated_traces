BASE <http://example.org/id/aggregated_traces/>
PREFIX : <http://example.org/def/ekg/aggregated_traces/>

INSERT {
  GRAPH <urn:ekg:directlyFollows> {
    ?Relation_DF a :DirectlyFollows , :DirectlyFollows_AggregatedEntity ;
      :source ?Event_1 ;
      :target ?Event_2 ;
      :amount ?amount ;
      :class ?AggregatedEntity .
  }
  
  GRAPH <urn:ekg:directlyPrecedes> {
    ?Relation_DP a :DirectlyPrecedes , :DirectlyPrecedes_AggregatedEntity ;
      :source ?Event_2 ;
      :target ?Event_1 ;
      :amount ?amount ;
      :class ?AggregatedEntity .
  }
}
WHERE {
  {
    SELECT ?AggregatedEntity ?Event_2 (max(?time_earlier) AS ?time_prevEvent)
    WHERE {
      ?Event_2 :entity|:parentEntity|:childEntity ?AggregatedEntity ;
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
      }
      FILTER( ?time_earlier < ?time )

      ?AggregatedEntity a :AggregatedEntity .
    }
    GROUP BY ?AggregatedEntity ?Event_2
  }

  {
    # Aggregation - ADD
    ?Event_2 a :Aggregation ;
      :action "ADD" .
    {
      ?Event_2 :parentEntity ?AggregatedEntity .
      ?Event_1 :timestamp ?time_prevEvent ;
        :quantity|:childQuantity|:outputQuantity [
          :class ?AggregatedEntity ;
          :amount ?amount ;
        ] .
    } UNION {
      ?Event_2 :childQuantity [
          :class ?AggregatedEntity ;
          :amount ?amount ;
        ] .
      ?Event_1 :timestamp ?time_prevEvent ;
        :entity|:parentEntity|:childEntity ?AggregatedEntity  .
    }
  } UNION {
    # Object or Aggregation - DELETE
    {
      ?Event_2 a :Object ;
        :entity ?AggregatedEntity .
    } UNION {
      ?Event_2 a :Aggregation ;
      :action "DELETE" ;
        :parentEntity ?AggregatedEntity .
    }
    ?Event_1 :timestamp ?time_prevEvent ;
      :quantity|:childQuantity|:outputQuantity [
        :class ?AggregatedEntity ;
        :amount ?amount ;
      ] .
  }
  #TODO: Transformation event

  BIND ( iri(md5(concat(str(?Event_1), str(?Event_2), str(?AggregatedEntity)))) as ?Relation_DF )
  BIND ( iri(md5(concat(str(?Event_2), str(?Event_1), str(?AggregatedEntity)))) as ?Relation_DP )
}