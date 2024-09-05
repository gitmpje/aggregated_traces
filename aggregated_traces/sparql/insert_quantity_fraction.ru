PREFIX : <http://example.org/def/ekg/aggregated_traces/>

INSERT {
  #TODO: distinguish by type of material (contained in the aggregated entity)
  GRAPH ?g { ?Relation :fraction ?fraction }
}
WHERE {
  VALUES ?type {
    :DirectlyFollows_AggregatedEntity
    :DirectlyPrecedes_AggregatedEntity
  }
  GRAPH ?g {
    ?Relation a ?type ;
      :source ?event .
  }

  OPTIONAL {
    # Get total incoming amount for an event
    { SELECT ?event ?type (sum(?amount_in) as ?sum_amount_in) {
      VALUES ?type { :DirectlyFollows_AggregatedEntity :DirectlyPrecedes_AggregatedEntity }
      [] a ?type ;
        :target ?event ;
        :amount ?amount_in ;
        :class ?entity .
    } GROUP BY ?event ?type }

    # Get total amount outgoing amount for an event
    { SELECT ?event ?type (sum(?amount_out) as ?sum_amount_out) {
      VALUES ?type { :DirectlyFollows_AggregatedEntity :DirectlyPrecedes_AggregatedEntity }
      [] a ?type ;
        :source ?event ;
        :amount ?amount_out ;
        :class ?entity .
    } GROUP BY ?event ?type }

    # Get (outgoing) relations with amount
    { SELECT ?Relation (sum(?amount) as ?amount_out) {
      ?Relation :amount ?amount ;
        :class ?entity .
    } GROUP BY ?Relation }
  }

  # Calculate fraction for a DF/DP relation as the outgoing amount for this relation divided by the total outgoing amount for a node
  # Take total outgoing amount, as quantity can change during transformation
  BIND( coalesce(?amount_out/?sum_amount_out, 1) as ?fraction)
}
