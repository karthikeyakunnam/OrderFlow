#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
RUN_ID="$(date +%s)"

printf 'Checking gateway...\n'
curl -fsS "$BASE_URL/health"
printf '\n'

printf 'Registering test customer...\n'
REGISTER_RESPONSE=$(curl -fsS -X POST "$BASE_URL/api/v1/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"CartNova Customer\",\"email\":\"customer-${RUN_ID}@example.com\",\"password\":\"Customer@123\"}")
printf '%s\n' "$REGISTER_RESPONSE"
CUSTOMER_TOKEN=$(curl -fsS -X POST "$BASE_URL/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"customer-${RUN_ID}@example.com\",\"password\":\"Customer@123\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')

printf 'Logging in admin...\n'
LOGIN_RESPONSE=$(curl -fsS -X POST "$BASE_URL/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@cartnova.local","password":"password"}')
ADMIN_TOKEN=$(printf '%s' "$LOGIN_RESPONSE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')

printf 'Creating category...\n'
CATEGORY=$(curl -fsS -X POST "$BASE_URL/api/v1/categories" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Electronics-${RUN_ID}\"}")
printf '%s\n' "$CATEGORY"
CATEGORY_ID=$(printf '%s' "$CATEGORY" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id", ""))')

printf 'Creating product...\n'
PRODUCT=$(curl -fsS -X POST "$BASE_URL/api/v1/products" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Wireless Keyboard\",\"description\":\"Mechanical keyboard\",\"price\":2499.00,\"category_id\":$CATEGORY_ID}")
printf '%s\n' "$PRODUCT"
PRODUCT_ID=$(printf '%s' "$PRODUCT" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

printf 'Adding inventory...\n'
curl -fsS -X POST "$BASE_URL/api/v1/inventory" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"product_id\":$PRODUCT_ID,\"quantity\":10}"
printf '\n'

printf 'Creating order...\n'
ORDER=$(curl -fsS -X POST "$BASE_URL/api/v1/orders" \
  -H "Authorization: Bearer $CUSTOMER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"items\":[{\"product_id\":$PRODUCT_ID,\"quantity\":2}]}")
printf '%s\n' "$ORDER"
ORDER_ID=$(printf '%s' "$ORDER" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

printf 'Waiting for inventory reservation...\n'
STATUS="CREATED"
for attempt in $(seq 1 20); do
  STATUS=$(curl -fsS "$BASE_URL/api/v1/orders/$ORDER_ID" \
    -H "Authorization: Bearer $CUSTOMER_TOKEN" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  if [ "$STATUS" != "CREATED" ]; then
    printf 'Order reached %s\n' "$STATUS"
    break
  fi
  sleep 1
done

if [ "$STATUS" = "CREATED" ]; then
  printf 'Order did not leave CREATED state\n' >&2
  exit 1
fi

printf 'CartNova end-to-end smoke test passed.\n'
