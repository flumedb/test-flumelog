# test-flumelog

reusable test suite that a flumelog implementation must pass

install this as a devdep on your [flumelog](https://github.com/flumedb/flumelog-memory) implementation,
and then have a test file like this:

``` js
//test.js
require('test-flumelog')(require(path_to_my_flumelog)(setup_args...))
```
then in your package.json, just have

``` js
"scripts": {
  "test": "node test.js"
},
```


## License

MIT
