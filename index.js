const { uploadChunked } = require('swarm-chunked-upload')
const crypto = require('crypto')

const chunkSize = 4096

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
    const size = chunkSize * 100_000
    const randomBytes = crypto.randomBytes(size)
    const numChunks = calculateNumberOfChunks(size)
    let succesCount = 0
    let errorCount = 0
    const { bzzReference, context } = await uploadChunked(randomBytes, {
        filename: 'random.dat',
        stamp: process.env.STAMP || '0000000000000000000000000000000000000000000000000000000000000000',
        beeUrl: 'http://127.0.0.1:1633',
        deferred: false,
        retries: 1,
        onSuccessfulChunkUpload: async (chunk, context) => {
            // console.log('✅', `${context.beeUrl}/chunks/${toHexString(chunk.address())}`)
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

function toHexString(bytes) {
    return bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, '0'), '')
}
