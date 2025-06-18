// to suit your point format, run search/replace for '[0]' and '[1]';
// for 3D version, see 3d branch (configurability would draw significant performance overhead)

import { LatLngTuple, LatLngZTimestamp } from './types'

// square distance between 2 points
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export function getSqDist(p1: LatLngTuple, p2: LatLngTuple) {
  const dx = p1[0] - p2[0]
  const dy = p1[1] - p2[1]

  return dx * dx + dy * dy
}

// square distance from a point to a segment
function getSqSegDist(p: LatLngZTimestamp, p1: LatLngZTimestamp, p2: LatLngZTimestamp) {
  let x = p1[0]
  let y = p1[1]
  let dx = p2[0] - x
  let dy = p2[1] - y

  if (dx !== 0 || dy !== 0) {
    const t = ((p[0] - x) * dx + (p[1] - y) * dy) / (dx * dx + dy * dy)

    if (t > 1) {
      x = p2[0]
      y = p2[1]
    } else if (t > 0) {
      x += dx * t
      y += dy * t
    }
  }

  dx = p[0] - x
  dy = p[1] - y

  return dx * dx + dy * dy
}
// rest of the code doesn't care about point format

// basic distance-based simplification
function simplifyRadialDist(points: LatLngZTimestamp[], sqTolerance: number) {
  let prevPoint = points[0]
  const newPoints = [prevPoint]
  let point

  for (let i = 1, len = points.length; i < len; i++) {
    point = points[i]

    if (getSqDist(point, prevPoint) > sqTolerance) {
      newPoints.push(point)
      prevPoint = point
    }
  }

  if (prevPoint !== point && point) newPoints.push(point)

  return newPoints
}

function simplifyDPStep(
  points: LatLngZTimestamp[],
  first: number,
  last: number,
  sqTolerance: number,
  simplified: LatLngZTimestamp[],
) {
  let maxSqDist = sqTolerance
  let index

  for (let i = first + 1; i < last; i++) {
    const sqDist = getSqSegDist(points[i], points[first], points[last])

    if (sqDist > maxSqDist) {
      index = i
      maxSqDist = sqDist
    }
  }

  if (maxSqDist > sqTolerance && index !== undefined) {
    if (index - first > 1) {
      simplifyDPStep(points, first, index, sqTolerance, simplified)
    }
    simplified.push(points[index])
    if (last - index > 1) {
      simplifyDPStep(points, index, last, sqTolerance, simplified)
    }
  }
}

function simplifyDouglasPeucker(points: LatLngZTimestamp[], sqTolerance: number) {
  const last = points.length - 1

  const simplified = [points[0]]
  simplifyDPStep(points, 0, last, sqTolerance, simplified)
  simplified.push(points[last])

  return simplified
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export function simplify(points: LatLngZTimestamp[], tolerance: number, highestQuality = false) {
  if (points.length <= 2) return points

  const sqTolerance = tolerance !== undefined ? tolerance * tolerance : 1

  points = highestQuality ? points : simplifyRadialDist(points, sqTolerance)
  points = simplifyDouglasPeucker(points, sqTolerance)

  return points
}
