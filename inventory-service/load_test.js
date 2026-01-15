import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '10s', target: 2000 }, // Ramp up to 2000 users
    { duration: '20s', target: 2000 }, // Stay at 2000 users
    { duration: '10s', target: 0 },    // Ramp down to 0
  ],
};

const PRODUCTS = ["iphone15", "samsung_s24", "pixel_8"];

export function setup() {
  let params = {
    headers: { 'Content-Type': 'application/json' },
  };

  // Seed all products
  for (let product of PRODUCTS) {
    let payload = JSON.stringify({
      productID: product,
      quantity: 500,
    });
    let res = http.post('http://localhost:8080/seed', payload, params);
    check(res, {
      [`${product} seeded`]: (r) => r.status === 200,
    });
  }
}

export default function () {
  // Randomly select a product
  let product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];

  let payload = JSON.stringify({
    userID: "user-" + __VU + "-" + __ITER, // Unique ID per iteration
    productID: product,
    quantity: 1,
  });

  let params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  let res = http.post('http://localhost:8080/buy', payload, params);

  check(res, {
    'is status 200': (r) => r.status === 200,
    'is status 410 (Gone)': (r) => r.status === 410,
    'is status 500': (r) => r.status === 500,
  });

  sleep(0.1);
}