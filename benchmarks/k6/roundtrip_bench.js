import http from 'k6/http';
import { check } from 'k6';

const baseUrl = __ENV.TARGET_BASE_URL || 'http://basilisk:8084';
const duration = __ENV.BENCH_DURATION || '20s';
const vus = Number(__ENV.BENCH_VUS || 25);
const payloadBytes = Number(__ENV.BENCH_PAYLOAD_BYTES || 8192);
const postRatio = Number(__ENV.BENCH_POST_RATIO || 0.7);

const payload = 'x'.repeat(payloadBytes);

export const options = {
  vus,
  duration,
};

export default function () {
  if (Math.random() < postRatio) {
    const response = http.post(`${baseUrl}/bench/echo`, payload, {
      headers: { 'Content-Type': 'application/octet-stream' },
      tags: { request_type: 'post_echo' },
    });

    check(response, {
      'post status is 200': (r) => r.status === 200,
      'post body round-trips fully': (r) => r.body.length === payloadBytes,
    });
    return;
  }

  const response = http.get(`${baseUrl}/bench/ping`, {
    tags: { request_type: 'get_ping' },
  });
  check(response, {
    'get status is 200': (r) => r.status === 200,
    'get body is pong': (r) => r.body === 'pong',
  });
}
