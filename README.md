# Update consumer price index database

The script `update-database.py` updates the [consumer price index database](https://console.cloud.google.com/sql/instances/consumer-price-index/overview?project=rbb-data-inflation) when new data is available from [Genesis](https://www-genesis.destatis.de/genesis//online?operation=table&code=61111-0006&bypass=true&levelindex=0&levelid=1657617156882#abreadcrumb). It uses the database's API at https://europe-west3-rbb-data-inflation.cloudfunctions.net/consumer-price-index-api ([GitHub](https://github.com/rbb-data/consumer-price-index-api))

Secrets are not stored in version control. To run this script, add `secret_config.py` and add the following lines:

```python
GENESIS_USERNAME = ""
GENESIS_PASSWORD = ""
API_TOKEN = ""
```
