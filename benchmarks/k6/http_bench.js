import http from 'k6/http';
import { check } from 'k6';

const baseUrl = __ENV.TARGET_BASE_URL || 'http://basilisk:8084';
const duration = __ENV.BENCH_DURATION || '20s';
const vus = Number(__ENV.BENCH_VUS || 25);

export const options = {
  vus,
  duration,
};

export default function () {
  const response = http.get(`${baseUrl}/bench/ping`);
  check(response, {
    'status is 200': (r) => r.status === 200,
    'body is pong': (r) => r.body === 'pong',
  });
}
