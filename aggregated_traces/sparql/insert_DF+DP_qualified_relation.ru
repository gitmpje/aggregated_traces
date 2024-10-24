BASE <http://example.org/id/ekg/aggregated_traces/>
PREFIX : <http://example.org/def/ekg/aggregated_traces/>

INSERT {
  GRAPH <urn:ekg:directlyFollows> {
    ?Relation_DF a :DirectlyFollows , :DirectlyFollows_AggregatedEntity ;
      :source ?Event_t1 ;
      :target ?Event_t2 ;
      :amount ?amount ;
      :class ?AggregatedEntity, ?Product .
  }
  
  GRAPH <urn:ekg:directlyPrecedes> {
    ?Relation_DP a :DirectlyPrecedes , :DirectlyPrecedes_AggregatedEntity ;
      :source ?Event_t2 ;
      :target ?Event_t1 ;
      :amount ?amount ;
      :class ?AggregatedEntity, ?Product .
  }
}
WHERE {
  {
    # Object
    ?Event_t2 a :Object ;
      :directlyPrecedes ?Event_t1 ;
      :entity ?AggregatedEntity .

    {
      # Take quantity from source event
      ?Event_t1 :quantity|:childQuantity|:outputQuantity [
          :class ?AggregatedEntity, ?Product ;
          :amount ?amount ;
        ] .

      MINUS {
        ?Event_t1 a :Aggregation ;
          :action "ADD" .
      }
    } UNION {
      # Take quantity from target event if source is Aggregation - ADD
      ?Event_t1 a :Aggregation ;
        :action "ADD" .

      ?Event_t2 :directlyPrecedes ?Event_t1 ;
        :quantity [
          :class ?AggregatedEntity, ?Product ;
          :amount ?amount ;
        ] .
    }
  } UNION {
    # Aggregation - ADD
    # Take quantity from target event
    ?Event_t2 a :Aggregation ;
      :action "ADD" ;
      :directlyPrecedes ?Event_t1 ;
      :childQuantity [
        :class ?AggregatedEntity, ?Product ;
        :amount ?amount ;
      ] .

    ?Event_t1 :entity|:parentEntity|:childEntity|(:outputQuantity/:class) ?AggregatedEntity  .
  } UNION {
    # Aggregation - DELETE
    ?Event_t2 a :Aggregation ;
      :action "DELETE" ;
      :directlyPrecedes ?Event_t1 ;
      :parentEntity ?AggregatedEntity .

    {
      # Take quantity from source event
      ?Event_t1 :quantity|:childQuantity|:outputQuantity [
          :class ?AggregatedEntity, ?Product ;
          :amount ?amount ;
        ] .

      MINUS {
        ?Event_t1 a :Aggregation ;
          :action "ADD" .
      }
    } UNION {
      # Take sum of quantity from source event if source is Aggregation - ADD
      ?Event_t1 a :Aggregation ;
        :action "ADD" ;
        :parentEntity ?AggregatedEntity .

      {
        SELECT ?Event_t1 ?Product (SUM(?_amount) AS ?amount)
        WHERE {
          ?Event_t1 a :Aggregation ;
            :action "ADD" ;
            :childQuantity _:ChildQuantity .

          _:ChildQuantity :amount ?_amount .
          OPTIONAL {
            _:ChildQuantity :class ?Product .
            ?Product a :Product .
          }
        } GROUP BY ?Event_t1 ?Product
      }
    }
  } UNION {
    # Transformation
    # Take quantity from target event
    ?Event_t2 a :Transformation ;
      :directlyPrecedes ?Event_t1 ;
      :inputQuantity [
        :class ?AggregatedEntity, ?Product ;
        :amount ?amount ;
      ] .

    ?Event_t1 (:quantity|:childQuantity|:outputQuantity)/:class ?AggregatedEntity .
  }

  ?AggregatedEntity a :AggregatedEntity .

  BIND ( iri(md5(concat(str(?Event_t1), str(?Event_t2), str(?AggregatedEntity)))) as ?Relation_DF )
  BIND ( iri(md5(concat(str(?Event_t2), str(?Event_t1), str(?AggregatedEntity)))) as ?Relation_DP )
}
