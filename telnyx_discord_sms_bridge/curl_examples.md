# curl examples

## Health check

```bash
curl http://127.0.0.1:8787/health
```

## Send outbound SMS

```bash
curl -X POST http://127.0.0.1:8787/send \
  -H "Content-Type: application/json" \
  -H "X-Bridge-Key: YOUR_LOCAL_BRIDGE_API_KEY" \
  -d "{\"to\":\"+18334882119\",\"text\":\"Testing outbound SMS\",\"from_number\":\"+15419202540\"}"
```

## Test inbound webhook locally

```bash
curl -X POST http://127.0.0.1:8787/webhooks/telnyx \
  -H "Content-Type: application/json" \
  --data-binary @sample_telnyx_payload.json
```
