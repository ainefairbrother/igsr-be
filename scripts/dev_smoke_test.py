import requests, sys, json
FE   = "http://localhost:8080"
API  = "http://127.0.0.1:8000"
ES   = "http://127.0.0.1:9200"

def ok(resp): 
    print(resp.status_code, resp.url); 
    print(json.dumps(resp.json(), indent=2))

print("ES:")
ok(requests.get(ES))

print("\nAPI /health:")
ok(requests.get(f"{API}/health"))

print("\nFE /api/health:")
ok(requests.get(f"{FE}/api/health"))

print("\nAPI /samples:")
ok(requests.get(f"{API}/samples", params={"size": 1}))