/* jslint es6 */
"use strict";
const pkg = require('./package.json')
const web3Utils = require('web3-utils')
const url = require('url')
const { send } = require('micro')
const { Exporter } = require('san-exporter')
const rp = require('request-promise-native')
const uuidv1 = require('uuid/v1')
const metrics = require('san-exporter/metrics')
const { logger } = require('./logger')
const exporter = new Exporter(pkg.name)

const CARDANO_GRAPHQL_URL = process.env.CARDANO_GRAPHQL_URL || "http://localhost:3100/graphql"
const DEFAULT_TIMEOUT = parseInt(process.env.DEFAULT_TIMEOUT || "30000")

let lastProcessedPosition = {
  blockNumber: parseInt(process.env.START_BLOCK || "1"),
  primaryKey: parseInt(process.env.START_PRIMARY_KEY || "-1")
}

const request = rp.defaults({
  method: 'POST',
  uri: CARDANO_GRAPHQL_URL,
  timeout: DEFAULT_TIMEOUT,
  time: true,
  gzip: true,
  json: true
})

const sendRequest = (async (query) => {
  metrics.requestsCounter.inc()

  const startTime = new Date()
  return request({
    body: {
      jsonrpc: '2.0',
      id: uuidv1(),
      query: query
    }
  }).then((result, error) => {
    metrics.requestsResponseTime.observe(new Date() - startTime)

    if (error) {
      return Promise.reject(error)
    }

    return result
  })
})

const getCurrentBlock = () => {
  return sendRequest(`{ cardano { tip { number } } }`).then(response => {
    return response.data === null ? null : response.data.cardano.tip.number;
  })
}

const getTransactions = () => {
  return sendRequest(`
  {
    transactions(
      where: {
        block: { epoch: { number: { _is_null: false } } }
        _and: { block: { number: { _gt: ${lastProcessedPosition.blockNumber} } } }
      }
      order_by: { includedAt: asc }
    ) {
      includedAt
      blockIndex
      fee
      hash

      block {
        number
        epochNo
      }

      inputs {
        address
        value
      }
  
      outputs {
        address
        value
      }

    }
  }`).then(response => {
    return response.data === null ? null : response.data.transactions;
  })
}

async function work() {
  const currentBlock = await getCurrentBlock()
  if (!currentBlock) {
    return
  }

  metrics.currentBlock.set(currentBlock)
  logger.info(`Fetching transactions for interval ${lastProcessedPosition.blockNumber}:${currentBlock}`)

  while (lastProcessedPosition.blockNumber < currentBlock) {
    const transactions = await getTransactions();
    if (!transactions) {
      return
    }

    if (transactions.length > 0) {
      for (let i = 0; i < transactions.length; i++) {
        transactions[i].primaryKey = lastProcessedPosition.primaryKey + i + 1;
      }
      const toBlock = transactions[transactions.length - 1].block.number

      logger.info(`Storing and setting primary keys ${transactions.length} transactions for blocks ${lastProcessedPosition.blockNumber + 1}:${toBlock}`)
      await exporter.sendDataWithKey(transactions, "primaryKey")

      lastProcessedPosition.blockNumber = toBlock
      lastProcessedPosition.primaryKey += transactions.length

      await exporter.savePosition(lastProcessedPosition)
      metrics.lastExportedBlock.set(lastProcessedPosition.blockNumber);
      logger.info(`Progressed to block ${toBlock}`)
    }

  }

}

async function fetchTransactions() {
  await work()
    .then(() => {
      logger.info(`Progressed to position ${JSON.stringify(lastProcessedPosition)}`)

      // Look for new events every 30 sec
      setTimeout(fetchTransactions, 30 * 1000)
    })
}

async function initLastProcessedBlock() {
  const lastPosition = await exporter.getLastPosition()

  if (lastPosition) {
    lastProcessedPosition = lastPosition
    logger.info(`Resuming export from position ${JSON.stringify(lastPosition)}`)
  } else {
    await exporter.savePosition(lastProcessedPosition)
    logger.info(`Initialized exporter with initial position ${JSON.stringify(lastProcessedPosition)}`)
  }
}

async function init() {
  await exporter.connect()
  await initLastProcessedBlock()
  await fetchTransactions()
}

init()

const healthcheckKafka = () => {
  return new Promise((resolve, reject) => {
    if (exporter.producer.isConnected()) {
      resolve()
    } else {
      reject("Kafka client is not connected to any brokers")
    }
  })
}

module.exports = async (request, response) => {
  const req = url.parse(request.url, true);

  switch (req.pathname) {
    case '/healthcheck':
      return healthcheckKafka()
        .then(() => send(response, 200, "ok"))
        .catch((err) => send(response, 500, `Connection to kafka failed: ${err}`))

    case '/metrics':
      response.setHeader('Content-Type', metrics.register.contentType);
      return send(response, 200, metrics.register.metrics())

    default:
      return send(response, 404, 'Not found');
  }
}
