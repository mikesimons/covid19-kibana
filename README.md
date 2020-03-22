# COVID19 Kibana dashboards

Here are a simple set of dashboards to track coronavirus on a per country basis.

Right now province / region data is squashed as it's not reliably available.

## Running

You'll need docker, docker-compose, golang, bash & curl installed on your machine.

Start elasticsearch / kibana and wait for kibana to respond on http://localhost:5601 by running:
```
docker-compose up -d
```

Once kibana says it's up, run:
```
./update.sh
```

This will download the latest data from [covid19api](https://covid19api.com/), process it and populate elasticsearch.

Finally, navigate to http://localhost:5601/app/kibana#/management/kibana/objects and import the "export.ndjson" file.
This will configure the index patterns and a few basic visualizations w/ dashboard.
