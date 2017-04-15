
var pull = require('pull-stream')
//var tape = require('tape')
var Spec = require('pull-spec')
var assert = require('assert')


function FastForward () {
  var tests = [], started = false
  function next () {
    var called = false
    assert.end = function () {
      if(called) throw new Error('end called twice in:'+name)
      called = true
      next()
    }
    if(tests.length) {
      var fn = tests.shift()
      console.log('# '+fn._name)
      fn(assert)
    }
    else
      started = false
  }
  return function tape (name, fn) {
    if(!fn) next()
    else {
      fn._name = name
      tests.push(fn)
    }
    if(!started) {
      started = true
      next()
    }
  }
}

module.exports = function (createLog, cb) {
  var tape = FastForward()
  var log = createLog()

  function assertStream(t, next) {
    return function assertStream (opts, expected) {
      pull(
        log.stream(opts), pull.collect(function (err, ary) {
        if(err) throw err
        t.deepEqual(ary, expected) //, 'test:'+JSON.stringify(opts))
        next()
      }))
    }
  }


  function values (ary) {
    return ary.map(function (e) { return e.value })
  }
  function seqs (ary) {
    return ary.map(function (e) { return e.seq })
  }
  console.log('START')
  tape('empty stream', function (t) {
    assertStream(t, next)({}, [])
    function next () { t.end() }
  })

  tape('since is null - to represent empty', function (t) {
    t.equal(log.since.value, -1)
    t.end()
  })

  tape('stream with 3 items', function (t) {
    //t.plan(19)

    //since it's a batch, update at once.
    var _since
    log.since.once(function (v) {
      t.ok(v > -1)
      _since = v
    }, false)

    log.append(['a','b','c'], function (err, seq) {
      if(err) throw err
      t.equal(log.since.value, seq)
      t.equal(_since, log.since.value)

      pull(
        log.stream({gte: 0, lte: seq, seqs:true, values:true}),
        pull.collect(function (err, ary) {
          if(err) throw err
          var expected = ary

          var _0 = expected[0].seq
          var _2 = expected[2].seq
          var n = 16

          console.log('--------------------------------------')
          var test = assertStream(t, next)

          test({seqs: false}, values(expected))
          test({gt: _0, seqs: false}, values(expected.slice(1)))
          test({gt: _0, reverse: true, seqs: false}, values(expected.slice(1)).reverse())
          test({gte: _0, seqs:false}, values(expected))
          test({lt: _2, seqs: false}, values(expected.slice(0, 2)))

          test( {},                     (expected))
          test({gt: _0},                (expected.slice(1)))
          test({gt: _0, reverse: true}, (expected.slice(1)).reverse())
          test({gte: _0},               (expected))
          test({lt: _2},                (expected.slice(0, 2)))

          test({values: false},                       seqs(expected))
          test({gt: _0, values: false},                seqs(expected.slice(1)))
          test({gt: _0, reverse: true, values: false}, seqs(expected.slice(1)).reverse())
          test({gte: _0, values: false},               seqs(expected))
          test({lt: _2, values: false},                seqs(expected.slice(0, 2)))

          test({gt: _2}, [])

          function next () {
            if(--n) return
            t.end()
          }

        })
      )
    })
  })

  tape('get', function (t) {
    pull(
      log.stream({seqs: true, values: false}),
      pull.asyncMap(function (seq, cb) {
        log.get(seq, cb)
      }),
      pull.collect(function (err, ary) {
        if(err) throw err
        t.deepEqual(ary, ['a', 'b', 'c'])
        t.end()
      })
    )

  })

  tape('live', function (t) {
    var seen = [], ended = 0

    pull(
      Spec(log.stream({live: true, seqs: false})),
      pull.drain(function (a) {
        console.log('drain', a)
        seen.push(a)
        if(seen.length === 4) {
          t.deepEqual(seen, ['a', 'b', 'c', 'd'])

          //abort the stream
          return false
        }
      }, function () {
        ended ++
        if(ended > 1) throw new Error('ended twice')
        log.append('e', function (err, _seq) {
          t.equal(ended, 1, 'ended only once')
          //check that it did not read the 'e'
          t.deepEqual(seen, ['a', 'b', 'c', 'd'])
          pull(
            log.stream({seqs: false}),
            pull.collect(function (err, ary) {
              if(err) throw err
              t.deepEqual(ary, ['a', 'b', 'c', 'd', 'e'])
              t.end()
            })
          )
        })
      })
    )

    tape('done', function (t) {
      console.log("DONE")
      //t.end()
      cb()
    })

    var last = log.since.value
    log.append(['d'], function (err, seq) {
      if(err) throw err
      console.log('d', seq)
      t.ok(seq > last)
      seq = last
    })
  })

}


