import * as Moleculer from 'moleculer'
import { Service, ServiceBroker } from 'moleculer'

import { markets } from '../src/adapter'

describe(`Adapter`, () => {

  let broker
  let service
  beforeAll(() => {
    broker = new ServiceBroker({ logger: false })
    expect(broker).toBeTruthy()
    // service = broker.createService()
  })
  it(`can connect locally`, async () => {
    const cursor = await markets.all()
    const results = await cursor.all()
    console.log(results)
  })
})
