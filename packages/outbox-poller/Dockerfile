FROM node:20-bullseye
WORKDIR /app

COPY package*.json ./
COPY tsconfig.base.json ./
COPY packages/shared/package*.json ./packages/shared/
COPY packages/producer/package*.json ./packages/producer/
COPY packages/outbox-poller/package*.json ./packages/outbox-poller/
COPY packages/consumer/package*.json ./packages/consumer/

RUN npm ci

COPY packages/shared ./packages/shared
COPY packages/outbox-poller ./packages/outbox-poller

RUN npm run build -w @notification/shared
RUN npm run build -w @notification/outbox-poller
RUN npx prisma generate --schema ./packages/outbox-poller/prisma/schema.prisma

CMD ["node", "packages/outbox-poller/dist/index.js"]
