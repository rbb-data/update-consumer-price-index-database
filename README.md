# Update consumer price index database

The function `update_database` in `main.py` updates the [consumer price index database](https://console.cloud.google.com/sql/instances/consumer-price-index/overview?project=rbb-data-inflation) when new data is available from [Genesis](https://www-genesis.destatis.de/genesis//online?operation=table&code=61111-0006&bypass=true&levelindex=0&levelid=1657617156882#abreadcrumb). It uses the database's API at https://europe-west3-rbb-data-inflation.cloudfunctions.net/consumer-price-index-api ([GitHub](https://github.com/rbb-data/consumer-price-index-api))

## Development

### Run locally

> **Note**
>
> The function uses secret environment variables. Add `API_SECRET` (secret to post to the consumer price index API) and `GENESIS_PASSWORD` (password for Genesis API) to `.env.local` that is not checked into version control.

Load environment variables from `.env` and `.env.local`:

```bash
export $(cat .env .env.local | xargs)
```

Install dependencies from `requirement.txt` and run the function:

```bash
python dev.py
```

### Deploy

Set cloud project

```bash
gcloud config set project rbb-data-inflation
```

and run:

```
gcloud functions deploy update-consumer-price-index-database \
  --region=europe-west3 \
  --runtime=python310 \
  --entry-point=update_database \
  --trigger-topic=database-update
```
