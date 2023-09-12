# 🔥 FireAgg

An open-source low-latency crypto aggregator. Combine crypto prices from any CCXT
supported exchanges in real-time.

You can get a sense of performance in [this blog
post](https://www.dataroc.ca/blog/how-crypto-exchanges-perform-under-load).

## Prerequisites

- Docker and docker compose

## Running your own aggregator

- Create a `.env` file by copying the content of `.env.example`. Change the password!
- Create the metrics network: `docker network create metrics`.
- Launch the stack: `docker compose up -d`.
- The first launch is much slower, as the core worker needs to index symbols from the top 13
  exchanges.
- Note that to avoid conflicts with other postgres database that might be running on
  your system, the database is exposed on port 15432 instead of the default 5432 port.

## Customizating which symbols you want to aggregate

You can add and remove containers to the docker compose file. Each worker can index
multiple symbols. When you specify for example `BTC/USD` for a worker, the worker will
connect to the 13 majors exchanges and index all of their trades and mid price changes.

There are no restrictions to how many symbols there can be on a single worker. It's a
matter of distributing the load - there are no mechanism (yet) for worker orchestration.

## Current limitations

- The redis server sometimes takes too long to launch before the workers can connect to
  it. The worker containers might restart.
- The performance tests ran on local hardware seems to point towards a limit of about
  200 messages per second that can be processed. Running BTC and ETH major pairs will
  easily reach that during high market activity. See
  [https://www.dataroc.ca/blog/how-crypto-exchanges-perform-under-load](this blog post
  for more information). We're working towards better performance.

## Roadmap

- Add an API to query the real-time aggregated prices.
- Support arbitrary aggregation models - E.g. allow you do develop private, real-time
  aggregate models and query the results.

## Example analytics queries

(Coming soon)

## Collecting prometheus metrics

Each fireagg worker exposes a prometheus-compatible metrics endpoint on port 9000. You
can collect these metrics inside another docker/docker-compose setup with a prometheus
scrape config as such:

```yml
# ...

scrape_configs:
  # ...

  - job_name: "fireagg-db_insertion"

    scrape_interval: 15s

    static_configs:
      - targets: ["fireagg_db_insertion:9000"]
```

The prometheus services needs to be launched and connect to the metrics docker network.
