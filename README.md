# Graphite-Web

## Overview

Graphite consists of three major components:

1. Graphite-Web, a Django-based web application that renders graphs and dashboards
2. The [Carbon](https://github.com/graphite-project/carbon) metric processing daemons
3. The [Whisper](https://github.com/graphite-project/whisper) time-series database library

## Storage Engine

This particular instance of Graphite uses HBase for storage, rather than Whisper.

How data is accessed from the webapp:
![Reading Data (Simple)](https://github.com/johnseekins/graphite-web/blob/hbase/ReadingData.jpg "Reading Data")

![Reading Data (Detailed)](https://github.com/johnseekins/graphite-web/blob/hbase/ReadingDataDetailed.jpg "Reading Data [Detailed]")

![Searching For Data](https://github.com/johnseekins/graphite-web/blob/hbase/SimpleSearches.jpg "Simple Searching")

![Searching for Complex Data](https://github.com/johnseekins/graphite-web/blob/hbase/ComplexSearch.jpg "Complex Searching")

## Installation, Configuration and Usage

Please refer to the instructions at [readthedocs](http://graphite.readthedocs.io/).

## License

Graphite-Web is licensed under version 2.0 of the Apache License. See the [LICENSE](https://github.com/graphite-project/graphite-web/blob/master/LICENSE) file for details.
