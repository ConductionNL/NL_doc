FROM node:20-bullseye AS build
ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}

# Copy cloned editor source into image context
WORKDIR /work
COPY editor-src /work

# Workspace-aware install and build
RUN set -eux; \
    corepack enable || true; corepack prepare pnpm@latest --activate || true; \
    if [ -f pnpm-lock.yaml ]; then \
      pnpm install --frozen-lockfile; \
      pnpm --filter @nldoc/demo build; \
    else \
      cd packages/demo; \
      if [ -f package-lock.json ]; then npm ci; else npm install; fi; \
      npm run build; \
    fi

FROM node:20-bullseye AS runtime
ENV NODE_ENV=production
WORKDIR /app
# keep workspace to allow next start to resolve deps
COPY --from=build /work /app
ENV PORT=3000
EXPOSE 3000
# Start the built Next.js demo
CMD [ "bash", "-lc", "if [ -f pnpm-lock.yaml ]; then corepack enable || true; pnpm --filter @nldoc/demo start; else cd packages/demo && npm run start; fi" ]


