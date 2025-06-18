import Database, { Database as BetterSqlite3Database, Statement } from 'better-sqlite3'
import { s2, geojson } from 's2js'
import path from 'path'
import fs from 'fs'
import { getSqDist, simplify } from './simplify'
import { Debug, GeoBounds, LatLngTuple, LatLngZTimestamp, TrackCollection, TrackParams } from './types'
import { Context } from '@signalk/server-api'
const RegionCoverer = geojson.RegionCoverer
import { Polygon } from 'geojson'

interface DbRow {
  timestamp: number
  lat: number
  lon: number
  s2cell: number
}

interface TableInfo {
  name: string
  type: string
}

export class SqliteTrackDb {
  db: BetterSqlite3Database
  insertStmt: Statement
  selfContext: Context
  databases: BetterSqlite3Database[] = []

  constructor(selfId: string, dataDir: string, dbName = 'tracks.db') {
    this.selfContext = `vessels.${selfId}` as Context
    this.db = new Database(path.join(dataDir, dbName))
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS positions (
          timestamp INTEGER,
          lat REAL,
          lon REAL,
          s2cell INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_s2cell ON positions(s2cell);`)
    this.insertStmt = this.db.prepare('INSERT INTO positions (timestamp, lat, lon, s2cell) VALUES (?, ?, ?, ?)')

    this.openValidDatabases(dataDir)
  }

  openValidDatabases(dataDir: string): void {
    const files = fs.readdirSync(dataDir)
    this.databases = files
      .map((file) => {
        if (file.endsWith('.db')) {
          try {
            const dbPath = path.join(dataDir, file)
            const db = new Database(dbPath)

            const tableInfo = db.prepare("SELECT name, type FROM pragma_table_info('positions')").all() as TableInfo[]
            const expectedColumns = {
              timestamp: 'INTEGER',
              lat: 'REAL',
              lon: 'REAL',
              s2cell: 'INTEGER',
            }
            const hasCorrectStructure = Object.entries(expectedColumns).every(([name, type]) => {
              return tableInfo.some((col) => col.name === name && col.type === type)
            })

            if (!hasCorrectStructure) {
              console.warn(`Database ${file} doesn't have the correct table structure, initializing...`)
              return
            }
            return db
          } catch (error) {
            console.error(`Failed to open or verify database ${file}:`, error)
          }
        }
      })
      .filter((db) => db !== undefined) as BetterSqlite3Database[]
  }

  close(): void {
    this.db.close()
    this.databases.forEach((db) => {
      try {
        db.close()
      } catch (error) {
        console.error('Error closing database:', error)
      }
    })
  }

  get(context: Context): Promise<LatLngTuple[]> {
    if (context !== this.selfContext && context !== 'vessels.self' && context !== 'self') {
      return Promise.resolve([])
    }
    //fetch rows that are not older than 1 hour
    const rows = this.db
      .prepare('select lat, lon from positions where timestamp > ?')
      .all(Date.now() - 60 * 60 * 1000) as {
      lat: number
      lon: number
    }[]
    return Promise.resolve(rows.map((row) => [row.lat, row.lon]))
  }

  newPosition(context: Context, position: LatLngTuple, timestamp: number = Date.now()): void {
    const cellId = Number(s2.Cell.fromLatLng(s2.LatLng.fromDegrees(position[0], position[1])).id)
    this.insertStmt.run(timestamp, position[0], position[1], cellId)
  }

  // TODO create v2 api with timestamps for tracks and track points
  getFilteredTracks(params: TrackParams, selfPosition?: LatLngTuple, debug?: Debug): Promise<TrackCollection> {
    debug && debug(params)
    if (!params.bbox && !selfPosition) {
      return Promise.reject()
    }

    const bbox = params.bbox ?? boundingBoxFromGeoLocation(selfPosition || [0, 0], params.radius || 1000)
    debug && debug(JSON.stringify(bbox))

    const tracks = this.queryAllDbs(bbox, debug)

    const result = {} as { [key: Context]: LatLngTuple[][] }
    result[this.selfContext] = tracks
    return Promise.resolve(result)
  }

  private queryAllDbs(bbox: GeoBounds, debug: Debug | undefined) {
    const query = getBBoxQuery(bbox)
    const threshold = bbox !== null ? getSqDist([bbox.sw[0], bbox.sw[1]], [bbox.ne[0], bbox.ne[1]]) / 100000 : 0.001
    debug?.(`Query: ${query}`)
    return this.databases.map((db) => this.queryDb(db, query, threshold, debug)).flat()
  }

  private queryDb(
    db: BetterSqlite3Database,
    query: string,
    threshold: number,
    debug: Debug | undefined,
  ): LatLngTuple[][] {
    const positions = db.prepare(query).all() as DbRow[]
    debug?.(`Found ${positions.length} positions in ${db.name}`)

    // Group tracks into segments with 5 min (300000ms) threshold
    const segments: DbRow[][] = []
    let currentSegment: DbRow[] = []

    positions.forEach((track, index) => {
      if (index === 0) {
        currentSegment.push(track)
        return
      }

      const timeDiff = track.timestamp - positions[index - 1].timestamp
      if (timeDiff > 300000) {
        // 5 minutes in milliseconds
        if (currentSegment.length > 0) {
          segments.push(currentSegment)
        }
        currentSegment = [track]
      } else {
        currentSegment.push(track)
      }
    })

    if (currentSegment.length > 0) {
      segments.push(currentSegment)
    }

    const tracks = segments.reduce<LatLngZTimestamp[][]>((acc, segment) => {
      acc.push(
        simplify(
          segment.map((row) => [row.lat, row.lon, null, row.timestamp]),
          threshold,
        ),
      )
      return acc
    }, [])
    return tracks
  }
}

const getBBoxQuery = ({ sw, ne }: GeoBounds): string => {
  const [s, w] = sw
  const [n, e] = ne
  const coverer = new RegionCoverer({ maxLevel: 30, maxCells: 8 })
  const linestring = {
    type: 'Polygon',
    coordinates: [
      [
        [w, s],
        [w, n],
        [e, n],
        [e, s],
        [w, s],
      ],
    ],
  } as Polygon
  const covering = coverer.covering(linestring)

  // Convert covering to range queries
  const rangeQueries = covering.map((cellId) => {
    const start = lowerBoundForContainedCellIds(cellId)
    const end = upperBoundForContainedCellIds(cellId)
    return `(s2cell BETWEEN ${start} <= s2cell AND ${end})`
  })

  const whereClause = rangeQueries.join('\n OR \n')
  const query = `SELECT * FROM positions WHERE ${whereClause} ORDER BY timestamp`

  // debug?.(`Query: ${query}`)
  return query
}

const earthRadiusM = 6371 * 1000

function boundingBoxFromGeoLocation(position: LatLngTuple, radius: number): GeoBounds {
  const [lat, lon] = position
  const latRad = (lat * Math.PI) / 180
  const lonRad = (lon * Math.PI) / 180

  const angularRadius = radius / earthRadiusM

  const minLat = latRad - angularRadius
  const maxLat = latRad + angularRadius

  const deltaLon = Math.asin(Math.sin(angularRadius) / Math.cos(latRad))

  //Handle edge cases where lats are near poles
  if (minLat > Math.PI / 2 || maxLat < -Math.PI / 2) {
    return {
      sw: [-90, -180],
      ne: [90, 180],
    }
  }

  let minLon = lonRad - deltaLon
  let maxLon = lonRad + deltaLon

  //Handle edge cases where lons wrap around the earth
  if (minLon < -Math.PI) {
    minLon += 2 * Math.PI
  }
  if (maxLon > Math.PI) {
    maxLon -= 2 * Math.PI
  }

  const minLatDeg = (minLat * 180) / Math.PI
  const maxLatDeg = (maxLat * 180) / Math.PI
  const minLonDeg = (minLon * 180) / Math.PI
  const maxLonDeg = (maxLon * 180) / Math.PI

  return {
    sw: [minLatDeg, minLonDeg],
    ne: [maxLatDeg, maxLonDeg],
  }
}

const upperBoundForContainedCellIds = (cellId: bigint) => {
  if (typeof cellId !== 'bigint') {
    throw new Error('Input must be a bigint.')
  }

  if (cellId === 0n) {
    // If input is 0, the "least significant one bit" doesn't exist.
    // We can define the result as having the first two bits set to 1.
    return 2n
  }

  // Find the position of the least significant one bit
  let lsOnePosition = 0
  let temp = cellId
  while ((temp & 1n) === 0n) {
    temp >>= 1n
    lsOnePosition++
  }

  // Set the bit to the left of the LSB one to one
  const bitToLeftMask = 1n << BigInt(lsOnePosition + 1)
  const resultWithLeftBitSet = cellId | bitToLeftMask

  // Set all bits to the right of the LSB to one
  const rightBitsMask = (1n << BigInt(lsOnePosition)) - 1n
  const finalResult = resultWithLeftBitSet | rightBitsMask

  return finalResult
}

const lowerBoundForContainedCellIds = (cellId: bigint) => {
  if (typeof cellId !== 'bigint') {
    throw new Error('Input must be a bigint.')
  }

  if (cellId === 0n) {
    return 0n
  }

  return cellId & (cellId - 1n)
}
