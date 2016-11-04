
var pull = require('pull-stream')
var tape = require('tape')

module.exports = function (log) {

  tape('stream forward and backward', function (t) {

    function assertStream (opts, expected) {
      pull(log.stream(opts), pull.collect(function (err, ary) {
        if(err) throw err
        t.deepEqual(ary, expected)
      }))
    }

    function values (ary) {
      return ary.map(function (e) { return e.value })
    }
    function seqs (ary) {
      return ary.map(function (e) { return e.seq })
    }

    var expected = [
      {seq: 0, value: 'a'},
      {seq: 1, value: 'b'},
      {seq: 2, value: 'c'}
    ]

    t.test('empty stream', function (t) {
      assertStream({}, [])
      t.end()
    })

    t.test('since is null - to represent empty', function (t) {
      t.equal(log.since.value, -1)
      t.end()
    })

    t.test('stream with 3 items', function (t) {

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
            t.plan(15)

            assertStream({seqs: false}, values(expected))
            assertStream({gt: _0, seqs: false}, values(expected.slice(1)))
            assertStream({gt: _0, reverse: true, seqs: false}, values(expected.slice(1)).reverse())
            assertStream({gte: _0, seqs:false}, values(expected))
            assertStream({lt: _2, seqs: false}, values(expected.slice(0, 2)))

            assertStream({},                     (expected))
            assertStream({gt: _0},                (expected.slice(1)))
            assertStream({gt: _0, reverse: true}, (expected.slice(1)).reverse())
            assertStream({gte: _0},               (expected))
            assertStream({lt: _2},                (expected.slice(0, 2)))

            assertStream({values: false},                       seqs(expected))
            assertStream({gt: _0, values: false},                seqs(expected.slice(1)))
            assertStream({gt: _0, reverse: true, values: false}, seqs(expected.slice(1)).reverse())
            assertStream({gte: _0, values: false},               seqs(expected))
            assertStream({lt: _2, values: false},                seqs(expected.slice(0, 2)))
          })
        )



//        t.equal(log.since.value, 2)

        t.end()
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

