const { uploadChunked } = require('swarm-chunked-upload')
const fs = require('fs')
const { Bee } = require('@ethersphere/bee-js')
const { Promises } = require('cafe-utility')
const axios = require('axios')

const chunkSize = 4096
const dataDir = 'data'
const beeUrl = 'http://127.0.0.1:1633'
const errorFile = 'errors.json'

main()

function write(args) {
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(args);
}

function calculateNumberOfChunks(size) {
    const refSize = 32
    let inputSize = size
    let numChunks = 0
    while (inputSize > 0) {
        numChunks += 1

        inputSize -= chunkSize
        inputSize += refSize
    }

    return numChunks + 1 // plus the manifest chunk
}

async function main() {
    const command = process.argv[2]
    switch (command) {
        case 'upload': await upload(); break
        case 'check': await check(); break
        case 'reupload': await reupload(); break
        case 'recheck': await recheck(); break
    }
}

async function upload() {
    const filename = 'The-Book-of-Swarm.pdf'
    const data = fs.readFileSync(filename)
    const numChunks = calculateNumberOfChunks(data.length)
    const dataDir = 'data'
    let succesCount = 0
    let errorCount = 0
    const { bzzReference, context } = await uploadChunked(data, {
        filename,
        stamp: process.env.STAMP || '0000000000000000000000000000000000000000000000000000000000000000',
        beeUrl,
        deferred: false,
        retries: 1,
        onSuccessfulChunkUpload: async (chunk, context) => {
            // console.log('✅', `${context.beeUrl}/chunks/${toHexString(chunk.address())}`)
            const address = toHexString(chunk.address())
            const outputFilename = `${dataDir}/${address}`
            fs.writeFileSync(outputFilename, new Uint8Array([...chunk.span(), ...chunk.payload]))
            succesCount++
            write(`chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}\r`)
        },
        onFailedChunkUpload: async (chunk, context) => {
            console.error('❌', `${context.beeUrl}/chunks/${toHexString(chunk.address())}`)
            errorCount++
            write(`chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}\r`)
        }
    })
    console.log(`chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}\r`)
    console.log('📦', `${context.beeUrl}/bzz/${toHexString(bzzReference)}/`)
}

async function isChunkRetrievable(hash) {
    const response = await axios.get(`${beeUrl}/stewardship/${hash}?traverse=false`)
    const isRetrievable = response.data.isRetrievable
    return isRetrievable
}

async function reupload() {
    const concurrency = 50
    const stamp = process.env.STAMP || '0000000000000000000000000000000000000000000000000000000000000000'
    const queue = Promises.makeAsyncQueue(concurrency)
    const bee = new Bee(beeUrl)
    const errorJSON = fs.readFileSync(errorFile)
    const errorHashes = JSON.parse(errorJSON)
    let succesCount = 0
    let errorCount = 0
    for (const item of errorHashes) {
        queue.enqueue(async () => {
            try {
                const start = Date.now()
                // const isRetrievable = await bee.isReferenceRetrievable(item)
                const fileData = fs.readFileSync(`${dataDir}/${item}`)
                const response = await bee.uploadChunk(stamp, fileData, { deferred: false })
                const elapsed = Date.now() - start
                if (item === response) {
                    const isRetrievable = await isChunkRetrievable(item)
                    if (!isRetrievable) {
                        throw "error"
                    }
                    succesCount++
                    console.log('✅', `${item}: ${elapsed}, chunks: ${errorHashes.length}, success: ${succesCount}, error: ${errorCount}`)
                } else {
                    errorCount++
                    console.log('❌', `${item}: ${elapsed}, chunks: ${errorHashes.length}, success: ${succesCount}, error: ${errorCount}`)
                    // console.error(`content error: ${item}`)
                    console.debug({swarmData, fileData})
                }
            } catch (e) {
                errorCount++
                console.log('❌', `${item}: chunks: ${errorHashes.length}, success: ${succesCount}, error: ${errorCount}`)
                console.error(`retrieve error: ${item}`)
            }

            // console.log(`chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}\r`)
        })
    }
    await queue.drain()
}

async function checkItems(items) {
    const concurrency = 50
    const queue = Promises.makeAsyncQueue(concurrency)
    const bee = new Bee(beeUrl)
    let succesCount = 0
    let errorCount = 0
    const errors = []
    const report = []
    const numChunks = items.length
    for (const item of items) {
        queue.enqueue(async () => {
            try {
                const start = Date.now()
                // const isRetrievable = await bee.isReferenceRetrievable(item)
                const response = await axios.get(`${beeUrl}/stewardship/${item}?traverse=false`)
                const isRetrievable = response.data.isRetrievable
                if (!isRetrievable) {
                    console.debug(response.data)
                    throw "error"
                }
                const swarmData = await bee.downloadChunk(item)
                const elapsed = Date.now() - start
                const fileData = fs.readFileSync(`${dataDir}/${item}`)
                if (bytesEqual(swarmData, fileData)) {
                    succesCount++
                    console.log('✅', `${item}: ${elapsed}, chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}`)
                    report.push([item, elapsed])
                } else {
                    errorCount++
                    console.log('❌', `${item}: ${elapsed}, chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}`)
                    errors.push(item)
                    console.debug({swarmData, fileData})
                }
            } catch (e) {
                console.error({e})
                errorCount++
                console.log('❌', `${item}: chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}`)
                console.error(`retrieve error: ${item}`)
                errors.push(item)
            }

            // console.log(`chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}\r`)
        })
    }
    await queue.drain()
    return {
        report,
        errors,
    }
}

async function check() {
    const dir = fs.readdirSync(dataDir)
    const reportFile = 'report.csv'
    const { report, errors } = await checkItems(dir)
    console.log({errors})
    fs.writeFileSync(errorFile, JSON.stringify(errors), { })
    fs.writeFileSync(reportFile, report.map(([item, elapsed]) => `${item},${elapsed}\n`).join(''))
}

async function recheck() {
    const errorJSON = fs.readFileSync(errorFile)
    const errorChunks = JSON.parse(errorJSON)
    const { report, errors } = await checkItems(errorChunks)
    console.log({errors})
}

function bytesEqual(a, b) {
    if (a.length !== b.length) {
        return false
    }
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) {
            return false
        }
    }
    return true
}

function toHexString(bytes) {
    return bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, '0'), '')
}
