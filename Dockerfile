ARG NODE_VERSION=16
FROM node:$NODE_VERSION


WORKDIR /plugin
RUN chown node:node /plugin

USER node

COPY --chown=node:node package.json ./

RUN npm install --quiet

COPY marker ./marker
COPY --chown=node:node tsconfig.json ./
COPY src ./src

RUN npx tsc

ENTRYPOINT npm run mocha