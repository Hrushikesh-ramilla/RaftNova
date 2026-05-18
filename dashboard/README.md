# RaftNova Dashboard

Real-time visualization of the RaftNova cluster. Built with React + TypeScript + D3, deployed on Vercel.

## What it shows
- Live cluster state: which node is leader, current term, commit index
- Raft log: last 20 committed entries streaming in real time
- Topology graph: 5-node pentagon with animated heartbeat pulses
- Write tester: PUT/GET/DELETE directly to the cluster, see latency
- Chaos controls: kill the leader, partition a node, flood with writes — watch the cluster recover

## Setup

1. Deploy the RaftNova backend (see root README)
2. Copy `.env.example` to `.env.local`, set `VITE_API_BASE_URL` to your cluster URL
3. Add `Access-Control-Allow-Origin: *` to Go HTTP handler (required for browser requests)
4. `npm install && npm run dev`

## Deploy to Vercel

Push to GitHub. Import repo in Vercel. Set root directory to `dashboard`. Set `VITE_API_BASE_URL` as environment variable. Done.

## CORS

The Go backend must include this header on all responses for the dashboard to work from a different origin:

```
Access-Control-Allow-Origin: *
```

Without this, all fetch calls from the browser will be blocked by CORS policy.

## Design decisions
- No component library — raw CSS keeps the bundle under 150KB
- D3 only for the topology graph — everything else is React state
- React Query for concurrent polling — one query per node, 1s interval
- 1500ms timeout per node poll — marks node unreachable fast enough to feel live
- Chaos controls are client-side simulations — "kill" stops polling that node, "heal" resumes
