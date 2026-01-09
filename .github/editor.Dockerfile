FROM node:20-bullseye AS build
ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}
WORKDIR /work

# Copy cloned editor source into image context
COPY editor-src /work

# Install dependencies and build the demo app (Next.js)
RUN corepack enable && corepack prepare pnpm@latest --activate || true
# Prefer pnpm if workspace is configured, fall back to npm
RUN if [ -f pnpm-lock.yaml ]; then \
      pnpm install --frozen-lockfile; \
      pnpm --filter @nldoc/demo build; \
    else \
      npm ci; \
      npm run build -w packages/demo; \
    fi

FROM node:20-bullseye AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /work /app
ENV PORT=3000
EXPOSE 3000
# Start the built Next.js demo
CMD [ "bash", "-lc", "if [ -f pnpm-lock.yaml ]; then pnpm --filter @nldoc/demo start; else npm run start -w packages/demo; fi" ]


