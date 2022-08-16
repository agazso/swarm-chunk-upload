## Installation

`npm i`

## RUNNING

The script assumes that there is Bee running at `http://localhost:1633`
You have to set the `STAMP` env variable to a valid postage stamp id.
You you have to create a `data` folder where the chunks will be stored.

Then `node bos.js upload` uploads the BOS.

`node bos.js check` goes through the chunks in the data folder and checks retrievability
