
var pull = require('pull-stream')
var tape = require('tape')

module.exports = function (log) {

  function assertStream(t, next) {
    return function assertStream (opts, expected) {
      pull(log.stream(opts), pull.collect(function (err, ary) {
        if(err) throw err
        t.deepEqual(ary, expected)
        next()
      }))
    }
  }


  tape('stream forward and backward', function (t) {

    function values (ary) {
      return ary.map(function (e) { return e.value })
    }
    function seqs (ary) {
      return ary.map(function (e) { return e.seq })
    }

    t.test('empty stream', function (t) {
      assertStream(t, next)({}, [])
      function next () { t.end() }
    })

    t.test('since is null - to represent empty', function (t) {
      t.equal(log.since.value, -1)
      t.end()
    })

    t.test('stream with 3 items', function (t) {
      t.plan(18)

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
            var n = 15

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
      var seen = [], source, ended = 0
      pull(
        source = log.stream({live: true, seqs: false}),
        pull.drain(function (a) {
          seen.push(a)
        }, function () {
          ended ++
        })
      )

      var last = log.since.value
      log.append(['d'], function (err, seq) {
        if(err) throw err
        t.ok(seq > last)
        t.deepEqual(seen, ['a', 'b', 'c', 'd'])

        source(true, function () {
          t.equal(ended, 1)
          log.append('e', function (err, _seq) {
            t.equal(ended, 1, 'ended only once')
            t.ok(_seq > seq)
            t.end()
          })

        })
      })
    })
  })

}







