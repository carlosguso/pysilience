# Bulkhead — Examples

## Python-only

### Limit concurrent database connections

```python
from pysilience import bulkhead, BulkheadRejected

@bulkhead(max_concurrent=5)
def query_database(sql: str) -> list:
    """At most 5 concurrent DB queries."""
    return db.execute(sql)

try:
    result = query_database("SELECT * FROM users")
except BulkheadRejected:
    print("Too many concurrent queries — try again later")
```

### Bulkhead with waiting

```python
from pysilience import bulkhead

@bulkhead(max_concurrent=3, max_wait=5.0)
async def call_slow_service(payload: dict) -> dict:
    """Up to 3 concurrent calls; queue others for up to 5 seconds."""
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://slow-api.example.com", json=payload)
        return resp.json()
```

### Isolating multiple dependencies

```python
from pysilience import Bulkhead, BulkheadConfig

db_bulkhead = Bulkhead(BulkheadConfig(max_concurrent=10), name="database")
cache_bulkhead = Bulkhead(BulkheadConfig(max_concurrent=20), name="cache")
api_bulkhead = Bulkhead(BulkheadConfig(max_concurrent=5, max_wait=2.0), name="external-api")

def get_user(user_id: int) -> dict:
    cached = cache_bulkhead.execute(lambda: redis.get(f"user:{user_id}"))
    if cached:
        return cached
    user = db_bulkhead.execute(lambda: db.query_user(user_id))
    enriched = api_bulkhead.execute(lambda: enrichment_api.enrich(user))
    return enriched
```

---

## FastAPI

### Per-dependency isolation

```python
from fastapi import FastAPI, HTTPException
from pysilience import create_bulkhead, BulkheadConfig, BulkheadRejected

app = FastAPI()

@app.on_event("startup")
def setup_bulkheads():
    create_bulkhead(BulkheadConfig(max_concurrent=10, max_wait=1.0), name="postgres", register=True)
    create_bulkhead(BulkheadConfig(max_concurrent=20), name="redis", register=True)
    create_bulkhead(BulkheadConfig(max_concurrent=5, max_wait=3.0), name="ml-service", register=True)

@app.get("/predict/{model_id}")
async def predict(model_id: str, input_data: dict):
    from pysilience import get_registered
    ml_bh = get_registered("bulkhead", "ml-service")
    try:
        return await ml_bh.execute_async(
            lambda: ml_client.predict(model_id, input_data)
        )
    except BulkheadRejected:
        raise HTTPException(status_code=503, detail="ML service at capacity")
```

### Preventing one slow route from starving others

```python
from fastapi import FastAPI, HTTPException
from pysilience import bulkhead, BulkheadRejected

app = FastAPI()

@bulkhead(max_concurrent=3, max_wait=2.0)
async def generate_report(report_id: str) -> bytes:
    """Heavy computation — limited to 3 concurrent."""
    return await report_engine.generate(report_id)

@app.get("/reports/{report_id}")
async def get_report(report_id: str):
    try:
        return await generate_report(report_id)
    except BulkheadRejected:
        raise HTTPException(status_code=503, detail="Report generation at capacity")
```

---

## Celery

### Limiting concurrent external API calls across workers

```python
from celery import Celery
from pysilience import Bulkhead, BulkheadConfig, BulkheadRejected

app = Celery("tasks", broker="redis://localhost:6379/0")

geocoding_bh = Bulkhead(
    BulkheadConfig(max_concurrent=4, max_wait=10.0),
    name="geocoding",
)

@app.task(bind=True, max_retries=3)
def geocode_address(self, address: str) -> dict:
    try:
        return geocoding_bh.execute(lambda: google_maps.geocode(address))
    except BulkheadRejected:
        self.retry(countdown=15)
```

### Protecting a shared resource in worker pool

```python
from celery import Celery
from pysilience import Bulkhead, BulkheadConfig

app = Celery("tasks", broker="redis://localhost:6379/0")

# Each worker process gets its own bulkhead limiting local concurrency
file_bh = Bulkhead(BulkheadConfig(max_concurrent=2), name="disk-io")

@app.task
def process_upload(file_path: str) -> str:
    return file_bh.execute(lambda: heavy_file_processing(file_path))
```
