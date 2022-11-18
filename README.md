## Installation

Install the dependencies with `npm`:

`npm install`


## Upload

The script assumes that there is Bee running at `http://localhost:1633` or specified with the `BEE_API_URL` environment variable.

You have to set the `STAMP` env variable to a valid postage stamp id.

Then you can upload a file or a folder with the following command:

```
npx ts-node upload.ts <file-or-folder>
```

Alternatively with npm:
```
npm run upload -- <file-or-folder>
```

The script automatically creates a manifest and prints the manifest link after the upload was successful. If there is an `index.html` file in the uploaded folder then it will automatically make it the [index document](https://docs.ethswarm.org/docs/access-the-swarm/upload-a-directory#upload-the-directory-containing-your-website). If only a single file is uploaded then it will make that the index document.


## Running the Book of Swarm test

You you have to create a `data` folder where the chunks will be stored.

Then `node bos.js upload` uploads the BOS.

`node bos.js check` goes through the chunks in the data folder and checks retrievability

