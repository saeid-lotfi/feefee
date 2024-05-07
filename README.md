# feefee
Data Service for Darsad app

Build docker image:
```bash
cd docker
docker build -t feefee .
```

Run container:
```bash
docker-compose up -d
```



## Source API:

Get bourse index history by its type code. 
```
GET https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_code}
```
index_code:
    total: 32097828799138957,
    weighted: 67130298613737946