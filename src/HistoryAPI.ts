import { ZonedDateTime } from "@js-joda/core";

import {
  Request,
  Response,
  Router,
} from "express";
import { SKInflux } from "./influx";


export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  influx: SKInflux,
  selfId: string,
  debug: (k: string) => void
) {

  router.get(
    "/signalk/v1/history/values",
    (req: Request, res: Response) => {
      const {from, to, context} = getFromToContext(req as FromToContextRequest, selfId)
      res.json(getValues(influx, context, from, to, debug, req))
    }
  );
  // router.get(
  //   "/signalk/v1/history/contexts",
  //   asyncHandler(
  //     fromToHandler(
  //       (...args) => getContexts.apply(this, [influx, ...args]),
  //       contextsDebug
  //     )
  //   )
  // );
  // router.get(
  //   "/signalk/v1/history/paths",
  //   asyncHandler(
  //     fromToHandler(
  //       (...args) => getPaths.apply(this, [influx, selfId, ...args]),
  //       pathsDebug
  //     )
  //   )
  // );
}

async function getContexts(
  influx: SKInflux,
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (s: string) => void
): Promise<string[]> {
  return Promise.resolve(['self'])
  // return influx
  //   .then((i) =>
  //     i.query('SHOW TAG VALUES FROM "navigation.position" WITH KEY = "context"')
  //   )
  //   .then((x: any) => x.map((x) => x.value));
}

async function getPaths(
  influx: SKInflux,
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (s: string) => void,
  req: Request
) :Promise<string[]> {
  // const query = `SHOW MEASUREMENTS`;
  // console.log(query);
  // return influx.then((i) => i.query(query)).then((d) => d.map((r:any) => r.name));
  return Promise.resolve(['navigation.speedOverGround'])
}

interface ValuesResult {
  context: string;
  range: {
    from: string;
    to: string;
  };
  values: {
    path: string;
    method: string;
    source?: string;
  }[];
  data: ValuesResultRow[];
}

type ValuesResultRow = any[];

async function getValues(
  influx: SKInflux,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (s: string) => void,
  req: Request
): Promise<ValuesResult | void> {
  const timeResolutionSeconds = req.query.resolution
    ? Number.parseFloat(req.query.resolution as string)
    : (to.toEpochSecond() - from.toEpochSecond()) / 500;

  debug(context);
  const pathExpressions = (req.query.paths as string || "")
    .replace(/[^0-9a-z\.,\:]/gi, "")
    .split(",");
  const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression);
  return undefined
  // const queries = pathSpecs
  //   .map(
  //     ({ aggregateFunction, path }) => `
  //     SELECT
  //       ${aggregateFunction} as value
  //     FROM
  //     "${path}"
  //     WHERE
  //       "context" = '${context}'
  //       AND
  //       time > '${from.toString()}' and time <= '${to.toString()}'
  //     GROUP BY
  //       time(${Number(timeResolutionSeconds * 1000).toFixed(0)}ms)`
  //   )
  //   .map((s) => s.replace(/\n/g, " ").replace(/ +/g, " "));
  // queries.forEach((s) => debug(s));

  // const x: Promise<IResults<any>[]> = Promise.all(
  //   queries.map((q: string) => influx.then((i) => i.query(q)))
  // );

  // return x.then((results: IResults<any>[]) => ({
  //   context,
  //   values: pathSpecs.map(({ path, aggregateMethod }) => ({
  //     path,
  //     method: aggregateMethod,
  //     source: null,
  //   })),
  //   range: { from: from.toString(), to: to.toString() },
  //   data: toDataRows(
  //     results.map((r) => r.groups()),
  //     pathSpecs.map((ps) => ps.extractValue)
  //   ),
  // }));
}

function getContext(contextFromQuery: string, selfId: string) {
  if (
    !contextFromQuery ||
    contextFromQuery === "vessels.self" ||
    contextFromQuery === "self"
  ) {
    return `vessels.${selfId}`;
  }
  return contextFromQuery.replace(/ /gi, "");
}


interface PathSpec {
  path: string;
  aggregateMethod: string;
  aggregateFunction: string;
  extractValue: (x: any) => any;
}

interface WithValue {
  value?: any
}
type ExtractValue = (r:WithValue) => any
const EXTRACT_POSITION = (r: WithValue) => {
  if (r.value) {
    const position = JSON.parse(r.value);
    return [position.longitude, position.latitude];
  }
  return null;
};
const EXTRACT_NUMBER = (r: WithValue) => Number(r.value);

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(":");
  let aggregateMethod = parts[1] || "average";
  let extractValue: ExtractValue = EXTRACT_NUMBER;
  if (parts[0] === "navigation.position") {
    aggregateMethod = "first";
    extractValue = EXTRACT_POSITION;
  }
  return {
    path: parts[0],
    aggregateMethod,
    extractValue,
    aggregateFunction: functionForAggregate[aggregateMethod] as string || "MEAN(value)",
  };
}

const functionForAggregate: {[key:string]: string} = {
  average: "mean()",
  min: "min()",
  max: "max()",
  first: "first()",
};

type FromToHandler<T = any> = (
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (d: string) => void,
  req: Request,
  res: Response
) => Promise<T>;


type FromToContextRequest = Request<unknown, unknown, unknown, {
  from: string
  to: string
  context: string
}>

const getFromToContext = ( {query}: FromToContextRequest, selfId: string) => {
  const from = ZonedDateTime.parse(query["from"]);
  const to = ZonedDateTime.parse(query["to"]);
  return {from, to, context: getContext(query.context, selfId)}
}