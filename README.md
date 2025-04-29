# Aggregated Traces

## Dependencies
Note that `pygraphviz` requires Graphviz to be installed and discoverable. For installation instructions check [PyGraphviz - Install](https://pygraphviz.github.io/documentation/stable/install.html).

## Get started
Create a virtual environment.
```
python -m venv .venv
```

Activate virtual environment.
```
source .venv/Scripts/activate
```

Install dependencies. Make sure that Graphviz is installed and discoverable.
```
python -m pip install -e .
```

## Method
### Mapping from traceability graph model to Event Knowledge Graph
|                       |                        |                           |      Target Event type     |                                |                          |
|:---------------------:|:----------------------:|:-------------------------:|:--------------------------:|:------------------------------:|:------------------------:|
|                       |                        |          `Object`         |     `Aggregation` - ADD    |     `Aggregation` - DELETE     |      `Transformation`    |
| **Source Event type** |        `Object`        |    source - `quantity`    |  target - `childQuantity`* |      source - `quantity`       | target - `inputQuantity` |
|                       |   `Aggregation` - ADD  |    target - `quantity`    |  target - `childQuantity`* | sum(source - `childQuantity`)* | target - `inputQuantity` |
|                       | `Aggregation` - DELETE |  source - `childQuantity` |  target - `childQuantity`* |    source - `childQuantity`    | target - `inputQuantity` |
|                       |    `Transformation`    | source - `outputQuantity` |  target - `childQuantity`* |   source - `outputQuantity`    | target - `inputQuantity` |

*Assumes that all quantities that are 'merged' are included in the child quantities.

## Tests

```
python -m pytest
```