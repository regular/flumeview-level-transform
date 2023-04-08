//jshint esversion: 11
//jshint -W033
const test = require('tape')
const View = require('.')
const tmp = require('tmp')
const {join} = require('path')
const pull = require('pull-stream')

test('stream data to index', t=>{
  const log = {
    filename: join(tmp.dirSync({unsafeCleanup: true}).name, 'xxx')
  }
  const name = 'foo'
  const sv = View(1, transform)(log, name)
  let count = 0

  function transform(meta) {
    if (count++ == 0) {
      t.equal(meta, undefined, 'meta should be null')
    } else {
      t.deepEqual(meta, {
        version: 1,
        since: 0,
        continuation: { key: 1, value: { foo: 'bar', bar: 'foo' }, type: 'put' }
      })
    }
    return pull.map(x => {
      x.value.bar = 'foo'
      return x
    })
  }

  sv.since.once( ()=>{
    pull(
      pull.values([{
        keys: [1],
        seq: 0,
        value: {foo: 'bar'}
      }]),
      sv.createSink((err)=>{
        t.notOk(err, 'createSink() does not error')
        pull(
          sv.read({
            upto: true,
            keys: true,
            values: false
          }),
          pull.collect((err, data)=>{
            t.notOk(err, 'read() does not error')
            t.deepEqual(data, [
              {
                key: 1, seq: { foo: 'bar', bar: 'foo' } 
              },
              {
                key: [ undefined ],
                seq: { since: 0 } 
              }
            ], 'data is as expected')

            // calling createSink again
            sv.createSink()
            t.end()
          })
        )
      })
    )
  })
})
