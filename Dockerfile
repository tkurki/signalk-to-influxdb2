FROM node:16


WORKDIR /plugin
RUN chown node:node /plugin

USER node

COPY --chown=node:node package.json ./

RUN npm install --quiet

COPY --chown=node:node tsconfig.json ./
COPY src ./src

RUN npx tsc

ENTRYPOINT npm run mocha