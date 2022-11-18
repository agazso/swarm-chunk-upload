/*
 * TODO
 * - [ ] cannot upload zero byte files
 *   - https://github.com/fairDataSociety/bmt-js/issues/17
 *   - added workaround (uploadEmptyChunk)
 * - [ ] deferred upload gets stuck in Bee dev mode
 *   - https://github.com/ethersphere/bee/issues/3227
 * - [ ] mantaray-js misses padding with metadata
 *   - https://github.com/ethersphere/mantaray-js/issues/35
 */

import { Bee } from '@ethersphere/bee-js'
import { Chunk, ChunkAddress, getSpanValue, makeChunk, makeChunkedFile } from '@fairdatasociety/bmt-js'
import { Promises, Strings, Files } from 'cafe-utility'
import { MantarayNode, Reference } from 'mantaray-js'
import { TextEncoder } from 'util'
import { statSync, promises, writeFileSync, createReadStream, ReadStream } from 'fs'
import { join, basename } from 'path'

const EMPTY_CHUNK_ADDRESS = 'b34ca8c22b9e982354f9c7f50b470d66db428d880c8a904d5fe4ec9713171526'
const beeUrl = process.env.BEE_API_URL || 'http://127.0.0.1:1633'
const stamp = process.env.STAMP || '0000000000000000000000000000000000000000000000000000000000000000'

type Options = {
    beeUrl: string
    stamp: string
    deferred?: boolean
    parallelism?: number
    retries?: number
    writeFiles?: boolean
    onSuccessfulChunkUpload?: (chunk: Chunk, context: Context) => Promise<void>
    onFailedChunkUpload?: (chunk: Chunk, context: Context) => Promise<void>
}

type Context = Required<Options> & { bee: Bee }

const noop = () => Promise.resolve()

/**
 * Lists all files recursively in a folder
 * @param path folder path
 * @returns an async generator of path strings
 */
async function* walkTreeAsync(path: string): AsyncGenerator<string> {
    for await (const entry of await promises.opendir(path)) {
      const entryPath = join(path, entry.name)

      if (entry.isDirectory()) {
        yield* walkTreeAsync(entryPath)
      } else if (entry.isFile()) {
        yield entryPath
      }
   }
}

function removeLeadingDirectory(path: string, directory: string) {
    if (directory.startsWith('./')) {
        directory = directory.slice(2)
    }

    if (!directory.endsWith('/')) {
        directory = directory + '/'
    }

    return path.replace(directory, '')
}


/**
 * Lists all files recursively in a folder, considering cwd for the relative path
 * @param path folder path
 * @param cwd for relative paths
 * @returns an async generator of path strings
 */
 export async function readdirDeepAsync(path: string, cwd?: string): Promise<string[]> {
    const entries: string[] = []
    for await (const entry of walkTreeAsync(path)) {
      entries.push(cwd ? removeLeadingDirectory(entry, cwd) : entry)
    }

    return entries
  }

async function getFiles(path: string): Promise<string[]> {
    const stat = statSync(path)

    if (stat.isDirectory()) {
      return await readdirDeepAsync(path, path)
    } else {
      return [path]
    }
}

export async function upload(fileOrDir: string, options: Options): Promise<Reference> {
    const context = makeContext(options)
    const stat = statSync(fileOrDir)
    const files = await getFiles(fileOrDir)
    const queue = Promises.makeAsyncQueue(context.parallelism)
    const node = new MantarayNode()
    for (const file of files) {
        const path = stat.isDirectory() ? join(fileOrDir, file) : fileOrDir
        const reference = await splitAndEnqueueFileChunks(path, queue, context)
        const filename = basename(file)
        const mimeType = detectMime(filename)
        const contentType = { 'Content-Type': mimeType }

        node.addFork(new TextEncoder().encode(file), reference, {
            ...contentType,
            Filename: filename,
        })

        if (files.length === 1) {
            node.addFork(encodePath('/'), new Uint8Array(32) as Reference, {
                'website-index-document': file,
            })
        }
        else if (file === 'index.html') {
            node.addFork(encodePath('/'), new Uint8Array(32) as Reference, {
                'website-index-document': file,
            })
        }

        console.log(toHexString(reference) + ' ' + file)
    }
    const storageSaver = async (data: Uint8Array) => splitAndEnqueueChunks(data, queue, context)
    const manifestReference = await node.save(storageSaver)

    await queue.drain()

    return manifestReference
}

interface Queue {
    enqueue(fn: () => Promise<void>): void;
}

const CHUNK_PAYLOAD_SIZE = 4096

function makeNextLevelChunks(intermediateChunks: Chunk[]): Chunk[] {
    const parentChunks: Chunk[] = []
    let parentChunkPayload = new Uint8Array(CHUNK_PAYLOAD_SIZE)
    let offset = 0
    let size = 0

    for (let i = 0; i < intermediateChunks.length; i++) {
        const chunk = intermediateChunks[i]
        const address = chunk.address()
        parentChunkPayload.set(address, offset)
        offset += address.length
        size += getSpanValue(chunk.span())

        if (offset === CHUNK_PAYLOAD_SIZE || i === intermediateChunks.length - 1) {
            const parentChunk = makeChunk(parentChunkPayload, { startingSpanValue: size })
            parentChunks.push(parentChunk)
            parentChunkPayload.fill(0)
            offset = 0
            size = 0
        }
    }

    return parentChunks
}

async function streamedChunker(read: (size: number) => Promise<Uint8Array | null>, onChunk: (chunk: Chunk) => Promise<void>): Promise<ChunkAddress> {
    let intermediateChunks: Chunk[] = []
    let intermediateChunkPayload = new Uint8Array(CHUNK_PAYLOAD_SIZE)
    let offset = 0
    let size = 0

    while (true) {
        const payload = await read(CHUNK_PAYLOAD_SIZE)
        if (payload == null) {
            const intermediateChunk = makeChunk(intermediateChunkPayload, { startingSpanValue: size })
            intermediateChunks.push(intermediateChunk)
            break
        }

        const chunk = makeChunk(payload)
        await onChunk(chunk)

        const address = chunk.address()
        intermediateChunkPayload.set(address, offset)
        offset += address.length
        size += payload.length

        if (offset === CHUNK_PAYLOAD_SIZE) {
            const intermediateChunk = makeChunk(intermediateChunkPayload, { startingSpanValue: size })
            intermediateChunks.push(intermediateChunk)
            intermediateChunkPayload.fill(0)
            offset = 0
            size = 0
        }
    }


    while (true) {
        for (const chunk of intermediateChunks) {
            await onChunk(chunk)
        }

        if (intermediateChunks.length === 1) {
            const rootChunk = intermediateChunks[0]
            return rootChunk.address()
        }

        intermediateChunks = makeNextLevelChunks(intermediateChunks)
    }
}

function splitAndEnqueueChunks(bytes: Uint8Array, queue: Queue, context: Context): ChunkAddress {
    // if (bytes.length === 0) {
    //     queue.enqueue(async () => {
    //         await uploadEmptyChunk(context)
    //     })
    //     return fromHexString(EMPTY_CHUNK_ADDRESS) as ChunkAddress
    // }
    const chunkedFile = makeChunkedFile(bytes)
    const levels = chunkedFile.bmt()
    for (const level of levels) {
        for (const chunk of level) {
            queue.enqueue(async () => {
                await uploadChunkWithRetries(chunk, context)
                await context.onSuccessfulChunkUpload(chunk, context)
            })
        }
    }
    return chunkedFile.address()
}

function calculateNumberOfChunks(size: number): number {
    let numChunks = 0

    const numLeafChunks = Math.ceil(size / 4096)
    numChunks += numLeafChunks

    let numIntermediateChunks = numLeafChunks
    while (numIntermediateChunks > 128) {
        const numParentChunks = Math.ceil(numIntermediateChunks / 128)
        numChunks += numParentChunks
        numIntermediateChunks = numParentChunks
    }

    numChunks += 1 // root chunk

    return numChunks
}

async function waitReadStreamEvent(readStream: ReadStream, event: 'ready' | 'readable'): Promise<void> {
    return new Promise(resolve => {
        function listener() {
            readStream.removeListener(event, listener)
            resolve()
        }
        readStream.addListener(event, listener)
    })
}

async function writeFileChunk(dataDir: string, path: string, chunk: Chunk) {
    const chunkFileDir = join(dataDir, path)
    await Files.mkdirp(chunkFileDir)
    const chunkFileName = toHexString(chunk.address())
    const content = Uint8Array.from([...chunk.span(), ...chunk.payload])
    writeFileSync(join(chunkFileDir, chunkFileName), content)
}

function log(...msg: any[]) {
    const output = msg.map(s => s.toString()).join('')
    process.stdout.write(output)
}

async function splitAndEnqueueFileChunks(path: string, queue: Queue, context: Context): Promise<ChunkAddress> {
    const stats = statSync(path)
    const size = stats.size
    const numChunks = calculateNumberOfChunks(size)

    const readStream = createReadStream(path, { highWaterMark: CHUNK_PAYLOAD_SIZE })
    await waitReadStreamEvent(readStream, 'ready')

    const read = async (size: number) => {
        await waitReadStreamEvent(readStream, 'readable')
        return readStream.read(size)
    }

    let numQueued = 0
    let numUploadedChunks = 0
    const startTime = Date.now()
    const listeners: ((_: unknown) => void)[] = []
    const chunkAddress = await streamedChunker(read, async (chunk: Chunk) => {
        numQueued++
        if (numQueued > context.parallelism) {
            await new Promise(resolve => listeners.push(resolve))
        }
        queue.enqueue(async () => {
            await uploadChunkWithRetries(chunk, context)
            await context.onSuccessfulChunkUpload(chunk, context)

            numQueued--
            if (numQueued <= context.parallelism) {
                const listener = listeners.shift()
                if (listener) {
                    listener(undefined)
                }
            }

            if (context.writeFiles) {
                await writeFileChunk('./chunk-data', path, chunk)
            }

            numUploadedChunks++

            const elapsedTime = Date.now() - startTime
            const uploadedKBs = (numUploadedChunks * 4096) / 1024
            const kbps = Math.floor(uploadedKBs / (elapsedTime / 1000.0))
            const total = uploadedKBs > 1024 ? `${Math.floor(uploadedKBs / 1024)} MB` : `${uploadedKBs} KB`
            const percentage = Math.ceil(numUploadedChunks / numChunks * 100.0)
            const makePad = (n: number, padding: string = ' ') => new Array(n).fill(padding).join('')
            const pad = (s: string, num: number) => s.length <= num ? makePad(num - s.length) + s : s
            const padNum = (n: number, num: number) => pad(n.toString(), num)
            log(` ${pad(percentage.toString(), 2)}%  uploaded chunks ${padNum(numUploadedChunks, (numChunks.toString().length))} / ${numChunks}, total: ${pad(total, 6)}, ${pad(kbps.toString(), 8)} kB/s                \r`)
        })
    })

    return chunkAddress
}

async function uploadChunkWithRetries(chunk: Chunk, context: Context) {
    let lastError = null
    for (let attempts = 0; attempts < context.retries; attempts++) {
        try {
            return await uploadChunk(chunk, context)
        } catch (error) {
            lastError = error
            await context.onFailedChunkUpload(chunk, context)
        }
    }
    throw lastError
}

async function uploadEmptyChunk(context: Context) {
    const expectedReference = EMPTY_CHUNK_ADDRESS
    const actualReference = await context.bee.uploadChunk(
        context.stamp,
        new Uint8Array(8),
        {
            deferred: context.deferred
        }
    )
    if (actualReference !== expectedReference) {
        throw Error(`Expected ${expectedReference} but got ${actualReference}`)
    }
    return actualReference
}

async function uploadChunk(chunk: Chunk, context: Context) {
    const expectedReference = toHexString(chunk.address())
    const actualReference = await context.bee.uploadChunk(
        context.stamp,
        Uint8Array.from([...chunk.span(), ...chunk.payload]),
        {
            deferred: context.deferred
        }
    )
    if (actualReference !== expectedReference) {
        throw Error(`Expected ${expectedReference} but got ${actualReference}`)
    }
    return actualReference
}

function makeContext(options: Options): Context {
    return {
        beeUrl: options.beeUrl,
        stamp: options.stamp,
        bee: new Bee(options.beeUrl),
        deferred: options.deferred ?? true,
        parallelism: options.parallelism ?? 8,
        retries: options.retries ?? 5,
        writeFiles: options.writeFiles ?? false,
        onSuccessfulChunkUpload: options.onSuccessfulChunkUpload ?? noop,
        onFailedChunkUpload: options.onFailedChunkUpload ?? noop
    }
}

function encodePath(path: string) {
    return new TextEncoder().encode(path)
}

function fromHexString(hexString: string): Uint8Array {
    const matches = hexString.match(/.{1,2}/g)
    if (!matches) {
        throw Error(`Invalid hex string: ${hexString}`)
    }
    return Uint8Array.from(matches.map(byte => parseInt(byte, 16)))
}

function toHexString(bytes: Uint8Array): string {
    return bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, '0'), '')
}

function detectMime(filename: string): string {
    const extension = Strings.getExtension(filename)
    return (
        {
            aac: 'audio/aac',
            abw: 'application/x-abiword',
            ai: 'application/postscript',
            arc: 'application/octet-stream',
            avi: 'video/x-msvideo',
            azw: 'application/vnd.amazon.ebook',
            bin: 'application/octet-stream',
            bz: 'application/x-bzip',
            bz2: 'application/x-bzip2',
            csh: 'application/x-csh',
            css: 'text/css',
            csv: 'text/csv',
            doc: 'application/msword',
            dll: 'application/octet-stream',
            eot: 'application/vnd.ms-fontobject',
            epub: 'application/epub+zip',
            gif: 'image/gif',
            htm: 'text/html',
            html: 'text/html; charset=utf-8',
            ico: 'image/x-icon',
            // ics: 'text/calendar',
            ics: '',
            jar: 'application/java-archive',
            jpeg: 'image/jpeg',
            jpg: 'image/jpeg',
            js: 'application/javascript',
            json: 'application/json',
            mid: 'audio/midi',
            midi: 'audio/midi',
            mp2: 'audio/mpeg',
            mp3: 'audio/mpeg',
            mp4: 'video/mp4',
            mpa: 'video/mpeg',
            mpe: 'video/mpeg',
            mpeg: 'video/mpeg',
            mpkg: 'application/vnd.apple.installer+xml',
            odp: 'application/vnd.oasis.opendocument.presentation',
            ods: 'application/vnd.oasis.opendocument.spreadsheet',
            odt: 'application/vnd.oasis.opendocument.text',
            oga: 'audio/ogg',
            ogv: 'video/ogg',
            ogx: 'application/ogg',
            otf: 'font/otf',
            png: 'image/png',
            pdf: 'application/pdf',
            ppt: 'application/vnd.ms-powerpoint',
            rar: 'application/x-rar-compressed',
            rtf: 'application/rtf',
            sh: 'application/x-sh',
            svg: 'image/svg+xml',
            swf: 'application/x-shockwave-flash',
            tar: 'application/x-tar',
            tif: 'image/tiff',
            tiff: 'image/tiff',
            ts: 'application/typescript',
            ttf: 'font/ttf',
            txt: 'text/plain',
            vsd: 'application/vnd.visio',
            wav: 'audio/x-wav',
            weba: 'audio/webm',
            webm: 'video/webm',
            webp: 'image/webp',
            woff: 'font/woff',
            woff2: 'font/woff2',
            xhtml: 'application/xhtml+xml',
            xls: 'application/vnd.ms-excel',
            xlsx: 'application/vnd.ms-excel',
            xml: 'application/xml',
            xul: 'application/vnd.mozilla.xul+xml',
            zip: 'application/zip',
            '3gp': 'video/3gpp',
            '3gp2': 'video/3gpp2',
            '7z': 'application/x-7z-compressed'
        }[extension] || ''
    )
}

async function main() {
    const ref = await upload(process.argv[2], {
        stamp,
        beeUrl,
        deferred: false,
        retries: 5,
        parallelism: 50,
        writeFiles: false,
        onSuccessfulChunkUpload: async (chunk, context) => {
            // console.log('✅', `${context.beeUrl}/chunks/${toHexString(chunk.address())}`)
        },
        onFailedChunkUpload: async (chunk, context) => {
            console.error('❌', `${context.beeUrl}/chunks/${toHexString(chunk.address())}`)
        }
    })

    console.log('✅', `manifest: ${beeUrl}/bzz/${toHexString(ref)}`)
}

main().catch(console.error)
