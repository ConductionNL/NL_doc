FROM node:20-bullseye AS build
ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}

# Copy cloned editor source into image context
WORKDIR /work
COPY editor-src /work

# Build the demo app directly, robust to workspace manager
WORKDIR /work/packages/demo
RUN set -eux; \
    corepack enable || true; corepack prepare pnpm@latest --activate || true; \
    if [ -f ../pnpm-lock.yaml ] || [ -f pnpm-lock.yaml ]; then \
      pnpm install --frozen-lockfile; \
      pnpm build; \
    else \
      if [ -f package-lock.json ]; then npm ci; else npm install; fi; \
      npm run build; \
    fi

FROM node:20-bullseye AS runtime
ENV NODE_ENV=production
WORKDIR /app
# copy only the demo app (built) to runtime
COPY --from=build /work/packages/demo /app
ENV PORT=3000
EXPOSE 3000
# Start the built Next.js demo
CMD [ "bash", "-lc", "if [ -f pnpm-lock.yaml ]; then corepack enable || true; pnpm start; else npm run start; fi" ]


