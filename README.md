# Aggregated Traces


## Dependencies
Note that `pygraphviz` requires Graphviz to be installed and discoverable. For installation instructions check https://pygraphviz.github.io/documentation/stable/install.html.

## Method
### Mapping from traceability graph model to Event Knowledge Graph
|                       |                        |                           |      Target Event type     |                               |                          |
|:---------------------:|:----------------------:|:-------------------------:|:--------------------------:|:-----------------------------:|:------------------------:|
|                       |                        |          `Object`         |     `Aggregation` - ADD    |     `Aggregation` - DELETE    |      `Transformation`    |
| **Source Event type** |        `Object`        |    source - `quantity`    |  target - `childQuantity`  |      source - `quantity`      | target - `inputQuantity` |
|                       |   `Aggregation` - ADD  |    target - `quantity`    |  target - `childQuantity`  |    source - `childQuantity`   | target - `inputQuantity` |
|                       | `Aggregation` - DELETE |  source - `childQuantity` |  target - `childQuantity`  |    source - `childQuantity`   | target - `inputQuantity` |
|                       |    `Transformation`    | source - `outputQuantity` |  target - `childQuantity`  |   source - `outputQuantity`   | target - `inputQuantity` |