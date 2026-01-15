import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '10s', target: 10 }, // Ramp-up to 10 users
    { duration: '30s', target: 50 }, // Stay at 50 users
    { duration: '10s', target: 0 },  // Ramp-down to 0 users
  ],
};

const BASE_URL = 'http://localhost:8080';

export default function () {
  // Test /seed endpoint
  let seedPayload = JSON.stringify({
    productID: 'test-product',
    quantity: 100,
  });
  let seedRes = http.post(`${BASE_URL}/seed`, seedPayload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(seedRes, {
    'SeedStock status is 200': (r) => r.status === 200,
  });

  // Test /buy endpoint
  let buyPayload = JSON.stringify({
    productID: 'test-product',
    quantity: 10,
  });
  let buyRes = http.post(`${BASE_URL}/buy`, buyPayload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(buyRes, {
    'DecrementStock status is 200': (r) => r.status === 200,
  });

  // Test invalid /buy request
  let invalidBuyPayload = JSON.stringify({
    productID: 'invalid-product',
    quantity: 10,
  });
  let invalidBuyRes = http.post(`${BASE_URL}/buy`, invalidBuyPayload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(invalidBuyRes, {
    'Invalid DecrementStock status is 400 or 404': (r) => r.status === 400 || r.status === 404,
  });

  sleep(1); // Simulate user wait time
}