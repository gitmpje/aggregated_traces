BASE <http://example.org/id/aggregated_traces/>
PREFIX : <http://example.org/def/ekg/aggregated_traces/>
PREFIX prov: <http://www.w3.org/ns/prov#>

INSERT {
  ?DFRelation a :DirectlyFollows , :DirectlyFollows_AggregatedEntity ;
    :source ?EventSource ;
    :target ?EventTarget ;
    :quantity ?Quantity .
}
WHERE {
  {
    SELECT ?AggregatedEntity ?EventSource (min(?time_later) AS ?dfEventTime)
    WHERE {
      ?EventSource prov:entity ?AggregatedEntity ;
        prov:atTime ?time .

      [] prov:entity ?AggregatedEntity ;
        prov:atTime ?time_later .
      FILTER( ?time_later > ?time )

      ?AggregatedEntity a :AggregatedEntity .
    }
    GROUP BY ?AggregatedEntity ?EventSource
  }

  ?EventTarget prov:atTime ?dfEventTime ;
    prov:entity ?AggregatedEntity .
  OPTIONAL {
    ?EventTarget :inputQuantity ?TargetInputQuantity .
    ?TargetInputQuantity :fromEntity ?AggregatedEntity .
  }
  OPTIONAL {
    ?EventSource :outputQuantity ?SourceOutputQuantity .
    ?SourceOutputQuantity :fromEntity ?AggregatedEntity .
  }
  BIND ( coalesce(?TargetInputQuantity, ?SourceOutputQuantity) as ?Quantity )

  BIND ( iri(md5(concat(str(?EventSource), str(?EventTarget)))) as ?DFRelation )
}