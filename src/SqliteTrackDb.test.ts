import { expect } from 'chai'
import { SqliteTrackDb } from './SqliteTrackDb'
import fs from 'fs'
import path from 'path'
import os from 'os'
import { GeoBounds, LatLngTuple } from './types'
import { Context } from '@signalk/server-api'

const testData = [
  {
    points: [[60.2578, 21.9531]],
    bbox: '59.97455314403678,21.219717207950076,60.31636155920052,23.051687422793826',
  },
  {
    points: [[60.069, 22.0816]],
    bbox: '59.891701394818405,20.24414149979725,60.57348464142283,23.908081929484755',
  },
  {
    points: [[60.252, 21.878]],
    bbox: '59.982389285981775,21.027936307467144,60.32411667222698,22.85990652231089',
  },
  {
    points: [[60.0444, 21.9975]],
    bbox: '59.69002762141289,19.954038066286035,60.37595941801291,23.617978495973535',
  },
  {
    points: [[59.9175, 21.7179]],
    bbox: '59.914801563095466,21.711282622546797,59.91749056837483,21.725594889850264',
  },
]

describe('SqliteTrackDb', () => {
  testData.forEach(({ points, bbox }) => {
    const [south, west, north, east] = bbox.split(',').map(Number)
    const bounds: GeoBounds = {
      sw: [south, west],
      ne: [north, east],
    }

    it(`should find tracks within bbox ${bbox}`, async () => {
      // Create temporary database
      const dbPath = path.join(os.tmpdir(), `test-${Date.now()}.db`)
      const db = new SqliteTrackDb('test', os.tmpdir(), path.basename(dbPath))

      const testContext = 'vessels.test' as Context
      try {
        // Insert test points
        points.forEach((position, index) => {
          const timestamp = Date.now() + index * 1000 // 1 second apart
          db.newPosition(testContext, position as LatLngTuple, timestamp)
        })

        // Get filtered tracks
        const result = await db.getFilteredTracks({ bbox: bounds, radius: 1000 }, [0, 0])

        // Verify results
        expect(result).to.have.property('vessels.test')
        expect(result[testContext]).to.be.an('array')
        expect(result[testContext].flat()).to.have.lengthOf(points.length)
      } catch (error) {
        console.error('Error during test execution:', error)
        throw error
      } finally {
        // Clean up
        db.db.close()
        if (fs.existsSync(dbPath)) {
          fs.unlinkSync(dbPath)
        }
      }
    })
  })
})
