# feefee
Data Service for Darsad app

## Run:
```bash
# build docker image
cd docker
docker build -t feefee .

# start services
cd ..
docker-compose up -d

# stop services
docker-compose down

# remove database data
docker volume rm feefee_postgres_data
```



## Source API:
```bash
# index history
GET https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_code}
    total: 32097828799138957
    weighted: 67130298613737946

# bourse fund assest
GET https://cdn.tsetmc.com/api/ClosingPrice/GetRelatedCompany/68


# bourse fund history
GET https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{fund_id}/0
```