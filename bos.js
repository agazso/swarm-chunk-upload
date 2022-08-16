const { uploadChunked } = require('swarm-chunked-upload')
const fs = require('fs')
const { Bee } = require('@ethersphere/bee-js')
const { Promises } = require('cafe-utility')

const chunkSize = 4096
const dataDir = 'data'
const beeUrl = 'http://127.0.0.1:1633'

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
            fs.writeFileSync(outputFilename, chunk.payload)
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

async function check() {
    const concurrency = 10
    const queue = Promises.makeAsyncQueue(concurrency)
    const bee = new Bee(beeUrl)
    const dir = fs.readdirSync(dataDir)
    let succesCount = 0
    let errorCount = 0
    const numChunks = dir.length
    for (const item of dir) {
        queue.enqueue(async () => {
            try {
                const isRetrievable = await bee.isReferenceRetrievable(item)
                if (!isRetrievable) {
                    throw e
                }
                const swarmData = await bee.downloadChunk(item)
                const fileData = fs.readFileSync(`${dataDir}/${item}`)
                if (bytesEqual(swarmData.slice(8), fileData)) {
                    succesCount++
                } else {
                    console.error(`content error: ${item}`)
                    console.debug({swarmData, fileData})
                    errorCount++
                }
            } catch (e) {
                console.error(`retrieve error: ${item}`)
                errorCount++
            }

            write(`chunks: ${numChunks}, success: ${succesCount}, error: ${errorCount}\r`)
        })
    }
    await queue.drain()
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
