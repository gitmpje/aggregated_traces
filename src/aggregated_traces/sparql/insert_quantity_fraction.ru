PREFIX : <http://example.org/def/ekg/aggregated_traces/>

INSERT {
  # TODO: distinguish by class
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
    # Get total incoming amount for an event (by class)
    { SELECT ?event ?type ?class (sum(xsd:float(strafter(?entity_amount_in, "|"))) as ?sum_amount_in) {
      { SELECT DISTINCT ?event ?type ?class (concat(?entity, "|", str(?amount_in)) as ?entity_amount_in) {
        VALUES ?type { :DirectlyFollows_AggregatedEntity :DirectlyPrecedes_AggregatedEntity }
        [] a ?type;
          :target ?event ;
          :quantity [
            :amount ?amount_in ;
            :class ?class ;
            :fromEntity/rdfs:label ?entity ;
          ] .
      }}
    } GROUP BY ?event ?type ?class }

    # Get total amount outgoing amount for an event (by class)
    { SELECT ?event ?type ?class (sum(xsd:float(strafter(?entity_amount_out, "|"))) as ?sum_amount_out) {
      { SELECT DISTINCT ?event ?type ?class (concat(?entity, "|", str(?amount_out)) as ?entity_amount_out) {
        VALUES ?type { :DirectlyFollows_AggregatedEntity :DirectlyPrecedes_AggregatedEntity }
        [] a ?type;
          :source ?event ;
          :quantity [
            :amount ?amount_out ;
            :class ?class ;
            :fromEntity/rdfs:label ?entity ;
          ] .
      }}
    } GROUP BY ?event ?type ?class }

    # Get (outgoing) relations with amount (by class)
    { SELECT ?Relation ?class (sum(xsd:float(strafter(?entity_amount_out, "|"))) as ?amount_out) {
      { SELECT DISTINCT ?Relation ?class (concat(?entity_label, "|", str(?amount)) as ?entity_amount_out) {
        ?Relation :quantity [
          :amount ?amount ;
          :class ?class ;
          :fromEntity/rdfs:label ?entity_label ;
        ] .
      }}
    } GROUP BY ?Relation ?class }
  }

  # If incoming amount matches outgoing amount, use incoming amount, otherwise use outgoing amount
  BIND( coalesce(?amount_out/if(bound(?sum_amount_in) && ?sum_amount_in=?sum_amount_out, ?sum_amount_in, ?sum_amount_out), 1) as ?fraction)
}