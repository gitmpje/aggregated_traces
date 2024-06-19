BASE <http://example.org/id/aggregated_traces/>
PREFIX : <http://example.org/def/ekg/aggregated_traces/>
PREFIX prov: <http://www.w3.org/ns/prov#>

INSERT {
  GRAPH <urn:ekg:directlyFollows> {
    ?Relation_DF a :DirectlyFollows , :DirectlyFollows_AggregatedEntity ;
      :source ?Event_1 ;
      :target ?Event_2 ;
      :quantity ?Quantity_DF .
  }
  
  GRAPH <urn:ekg:directlyPrecedes> {
    ?Relation_DP a :DirectlyPrecedes , :DirectlyPrecedes_AggregatedEntity ;
      :source ?Event_2 ;
      :target ?Event_1 ;
      :quantity ?Quantity_DP .
  }
}
WHERE {
  {
    SELECT ?AggregatedEntity ?Event_1 (min(?time_later) AS ?time_nextEvent)
    WHERE {
      ?Event_1 prov:entity ?AggregatedEntity ;
        prov:atTime ?time .

      [] prov:entity ?AggregatedEntity ;
        prov:atTime ?time_later .
      FILTER( ?time_later > ?time )

      ?AggregatedEntity a :AggregatedEntity .
    }
    GROUP BY ?AggregatedEntity ?Event_1
  }

  ?Event_2 prov:atTime ?time_nextEvent ;
    prov:entity ?AggregatedEntity .
  OPTIONAL {
    ?Event_2 :inputQuantity ?InputQuantity_Event_1 .
    ?InputQuantity_Event_1 :fromEntity ?AggregatedEntity .
  }
  OPTIONAL {
    ?Event_1 :outputQuantity ?OutputQuantity_Event_1 .
    ?OutputQuantity_Event_1 :fromEntity ?AggregatedEntity .
  }
  BIND ( coalesce(?InputQuantity_Event_1, ?OutputQuantity_Event_1) as ?Quantity_DF )
  BIND ( coalesce(?OutputQuantity_Event_1, ?InputQuantity_Event_1) as ?Quantity_DP )

  BIND ( iri(md5(concat(str(?Event_1), str(?Event_2)))) as ?Relation_DF )
  BIND ( iri(md5(concat(str(?Event_2), str(?Event_1)))) as ?Relation_DP )
}