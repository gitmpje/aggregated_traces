PREFIX : <http://example.org/def/ekg/aggregated_traces/>

INSERT {
  GRAPH ?g { ?relation :fraction ?fraction }
}
WHERE {
  VALUES ?type {
    :DirectlyFollows_AggregatedEntity
    :DirectlyPrecedes_AggregatedEntity
  }
  GRAPH ?g {
    ?relation a ?type ;
      :source ?event .
  }

  OPTIONAL {
    # Get total amount outgoing amount for an event
    { SELECT ?event ?type ?product (sum(?amount_out) as ?sum_amount_out) {
      VALUES ?type { :DirectlyFollows_AggregatedEntity :DirectlyPrecedes_AggregatedEntity }
      _:relation a ?type ;
        :source ?event ;
        :amount ?amount_out ;
        :class ?entity .
      ?entity a :AggregatedEntity .

      OPTIONAL {
        _:relation :class ?product .
        ?product a :Product .
      }
    } GROUP BY ?event ?product ?type }

    # Get (outgoing) relations with amount
    { SELECT ?relation ?product (sum(?amount) as ?amount_out) {
      ?relation :amount ?amount ;
        :class ?entity .
      ?entity a :AggregatedEntity .

      OPTIONAL {
        ?relation :class ?product .
        ?product a :Product .
      }
    } GROUP BY ?product ?relation }
  }

  # Calculate fraction for a DF/DP relation as the outgoing amount for this relation divided by the total outgoing amount for a node
  # Take total outgoing amount, as quantity can change during transformation
  BIND( coalesce(?amount_out/?sum_amount_out, 1) as ?fraction)
}
