import { Database } from 'arangojs'

import * as Moleculer from 'moleculer'
import { Service, ServiceBroker } from 'moleculer'

const db = new Database()
db.useDatabase('meta-query')
db.login('root', '5orange5')

export const markets = db.collection('markets')
export const users = db.collection('users')
export const listings = db.collection('listings')

export { db }

interface Id {
  _id: string
}

interface Key {
  _key: string
}

interface Revisable {
  _rev: string
}

interface BasicModel extends Id, Key, Revisable {}

interface Expression {}

interface AdapterOptions {
  databaseName?: string
  username?: string
  password?: string

  collection?: string
}

interface CollectionOptions {
  collection?: string
}

interface AdapterFindOptions extends CollectionOptions {}
interface AdapterFindByIdOptions extends CollectionOptions {}
interface AdapterFindByIdsOptions extends CollectionOptions {}

interface AdapterFindOneOptions extends CollectionOptions {}
interface AdapterCountOptions extends CollectionOptions {}

interface AdapterInsertOptions extends CollectionOptions {
  waitForSync?: boolean
  returnNew?: boolean
  returnOld?: boolean
  silent?: boolean
  overwrite?: boolean
}

interface AdapterInsertManyOptions extends CollectionOptions {}

interface AdapterBasicUpdateOptions {
  waitForSync?: boolean
  keepNull?: boolean
  mergeObjects?: boolean
  returnNew?: boolean
  returnOld?: boolean
  ignoreRevs?: boolean
}

interface AdapterRemoveByIdOptions extends CollectionOptions {}
interface AdapterUpdateByIdOptions
  extends CollectionOptions,
    AdapterBasicUpdateOptions {
  rev?: string
  policy?: string
}
interface AdapterRemoveManyOptions extends CollectionOptions {}
interface AdapterUpdateManyOptions
  extends CollectionOptions,
    AdapterBasicUpdateOptions {}
interface AdapterClearOptions extends CollectionOptions {}

interface Adapter<TEntity extends BasicModel> {
  init(broker: ServiceBroker, service: Service)

  connect(name: string): Promise<Database>

  disconnect(): void

  insert(entity: TEntity, options: AdapterInsertOptions): Promise<TEntity>

  findById(id: string, options: AdapterFindByIdOptions): Promise<TEntity>

  removeById(id: string, options: AdapterRemoveByIdOptions): Promise<any>

  find(predicate: Expression, options: AdapterFindOptions): Promise<TEntity[]>

  findOne(
    predicate: Expression,
    options: AdapterFindOneOptions,
  ): Promise<TEntity>

  findByIds(ids: string[], options: AdapterFindByIdsOptions): Promise<TEntity[]>

  insertMany(
    entities: BasicModel[],
    options: AdapterInsertManyOptions,
  ): Promise<TEntity[]>

  updateMany(
    entities: TEntity | TEntity[],
    options: AdapterUpdateManyOptions,
  ): Promise<any>

  removeMany(
    predicate: Expression,
    options: AdapterRemoveManyOptions,
  ): Promise<any>

  count(predicate: Expression, options: AdapterCountOptions): Promise<number>

  updateById(
    id: string,
    entity: TEntity,
    options: AdapterUpdateByIdOptions,
  ): Promise<TEntity>

  clear(options: AdapterClearOptions): void
}

export default class implements Adapter<BasicModel> {
  #options: AdapterOptions

  db: Database

  broker: ServiceBroker
  service: Service

  get name() {
    return this.#options.databaseName
  }

  get username() {
    return this.#options.username
  }

  get password() {
    return this.#options.password
  }

  constructor(options: AdapterOptions) {
    this.#options = options
  }

  init(broker: ServiceBroker, service: Service) {
    this.broker = broker
    this.service = service
  }

  async connect() {
    const db = (this.db = new Database())
    db.useDatabase(this.name)

    if (this.username && this.password) db.login(this.username, this.password)

    return db
  }

  disconnect() {
    this.db.close()
  }

  async insert(
    entity: BasicModel | BasicModel[],
    options: AdapterInsertOptions,
  ) {
    const collection = this.getCollection(options.collection)
    const document = await collection.save(entity, { ...options })
    return (document as unknown) as BasicModel
  }

  async findById(id: string, options: AdapterFindByIdOptions) {
    const collection = this.getCollection(options.collection)
    return collection.document(id)
  }

  async removeById(id: string, options: AdapterFindByIdOptions) {
    const collection = this.getCollection(options.collection)
    return collection.removeByKeys([...id], { ...options })
  }

  async find(predicate: Expression, options: AdapterFindOptions) {
    const collection = this.getCollection(options.collection)
    const cursor = await collection.byExample(predicate, { ...options })

    return (cursor.all() as unknown) as BasicModel[]
  }

  async findOne(predicate: Expression, options: AdapterFindOneOptions) {
    const collection = this.getCollection(options.collection)
    return collection.firstExample(predicate)
  }

  async findByIds(ids: string[], options: AdapterFindByIdsOptions) {
    const collection = this.getCollection(options.collection)
    return collection.lookupByKeys(ids)
  }

  async insertMany(entities: BasicModel[], options: AdapterInsertManyOptions) {
    return (this.insert(entities, { ...options }) as unknown) as BasicModel[]
  }

  async updateMany(
    entities: BasicModel | BasicModel[],
    options: AdapterUpdateManyOptions,
  ) {
    const collection = this.getCollection(options.collection)
    collection.bulkUpdate(entities, {
      ...options,
      returnNew: true,
    })
  }

  async removeMany(predicate: Expression, options: AdapterRemoveManyOptions) {
    const collection = this.getCollection(options.collection)
    return collection.removeByExample(predicate)
  }

  async count(filters: Expression, options: AdapterCountOptions) {
    const collection = this.getCollection(options.collection)
    const cursor = await collection.byExample(filters)
    return cursor.count
  }

  async updateById(
    id: string,
    entity: BasicModel,
    options: AdapterUpdateByIdOptions,
  ) {
    const collection = this.getCollection(options.collection)
    return collection.update(id, { ...entity }, { ...options })
  }

  async clear(options: AdapterClearOptions) {
    return this.removeMany({}, { ...options })
  }

  getCollection(name: string) {
    return this.db.collection(name)
  }
}
