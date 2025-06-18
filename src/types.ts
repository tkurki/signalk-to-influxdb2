import { Context } from '@signalk/server-api'

export type LatLngTuple = [number, number, ...(number | null)[]]
export type LatLngZTimestamp = [...LatLngTuple, null, number]
export type LngLatTuple = [number, number]

export interface Position {
  latitude: number
  longitude: number
}

export interface TrackCollection {
  [key: Context]: LatLngTuple[][]
}

export interface GeoBounds {
  ne: LatLngTuple
  sw: LatLngTuple
}

export interface QueryParameters {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any
}

export interface TrackParams {
  bbox: GeoBounds | null
  radius: number | null
}

export interface Debug {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (...args: any): any
  enabled: boolean
}
