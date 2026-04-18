# ── Stage 1: Build React app ──────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# ── Stage 2: Serve with Nginx ─────────────────────────────────────────────────
FROM nginx:alpine

COPY --from=builder /app/dist /usr/share/nginx/html

# Nginx config — proxies /api to backend container
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3000
