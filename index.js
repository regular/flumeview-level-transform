//jshint -W033
//jshint -W018
//jshint -W014
//jshint  esversion: 11
const pull = require('pull-stream')
const Level = require('level')
const charwise = require('charwise')
const Write = require('pull-write')
const pl = require('pull-level')
const Obv = require('obz')
const path = require('path')
const Paramap = require('pull-paramap')
const ltgt = require('ltgt')

const noop = () => {}
const META = '\x00'
const META2 = [undefined]

module.exports = function(version, transform) {
  return function (log, name) {
    const dir = path.dirname(log.filename)
    const since = Obv()

    let db, writer
    let closed, outdated
    let meta = null

    db = create()

    db.get(META, { keyEncoding: 'utf8' }, function (err, value) {
      meta = value
      if (err) since.set(-1)
      else if (value.version === version) since.set(value.since)
      else {
        // version has changed, wipe db and start over.
        outdated = true
        destroy()
      }
    })

    return {
      methods: { get: 'async', read: 'source' },
      since,
      createSink,
      get,
      read, 
      close,
      destroy
    }

    function createSink(cb) {
      writer = createWriter(cb)
      return pull(
        transform(meta),
        writer
      )
    }
    
  
    function create() {
      closed = false
      if (!log.filename) {
        throw new Error(
          'flumeview-reduce-cascade can only be used with a log that provides a directory'
        )
      }
      return Level(path.join(dir, name), {
        keyEncoding: charwise,
        valueEncoding: 'json'
      })
    }

    function close (cb) {
      if (typeof cb !== 'function') {
        cb = noop
      }

      closed = true
      // todo: move this bit into pull-write
      if (outdated) {
        db.close(cb)
        return
      }
      if (writer) {
        writer.abort( ()=> db.close(cb) )
        return 
      }
      if (!db) return cb()
      since.once(() => db.close(cb) )
    }

    function destroy (cb) {
      // FlumeDB restarts the stream as soon as the stream is cancelled, so the
      // rebuild happens before `writer.abort()` calls back. This means that we
      // must run `since.set(-1)` before aborting the stream.
      since.set(-1)

      // The `clear()` method must run before the stream is closed, otherwise
      // you can have a situation where you:
      //
      // 1. Flumeview-Level closes the stream
      // 2. FlumeDB restarts the stream
      // 3. Flumeview-Level processes a message
      // 4. Flumeview-Level runs `db.clear()` and deletes that message.
      db.clear(cb)
    }


    function createWriter(cb) {
      return Write(
        function (batch, cb) {
          if (closed) {
            return cb(new Error('database closed while index was building'))
          }
          if (batch.length>1) {
            meta.continuation = batch.slice(-1)[0]
          }
          db.batch(batch.concat([{
            key: META2,
            value: { since: meta.since },
            type: 'put'
          }]), function (err) {
            if (err) return cb(err)
            since.set(batch[0].value.since)
            // callback to anyone waiting for this point.
            cb()
          })
        },
        function reduce (batch, data) {
          if (data.sync) return batch
          var seq = data.seq

          if (!batch) {
            meta = meta || {
              version: version,
              since: seq,
              continuation: null
            }

            batch = [
              {
                key: META,
                value: meta,
                valueEncoding: 'json',
                keyEncoding: 'utf8',
                type: 'put'
              }
            ]
          }

          meta.since = Math.max(meta.since, seq)

          // keys must be an array (like flatmap) with zero or more values
          const keys = data.keys
          batch = batch.concat(
            keys.map(function (key) {
              return { key: key, value: data.value || seq, type: 'put' }
            }) 
          )
          return batch
        },
        512,
        cb
      )
    }

    function get(key, cb) {
      // wait until the log has been processed up to the current point.
      db.get(key, function (err, seq) {
        if (err && err.name === 'NotFoundError') return cb(err)
        if (err) {
          return cb(
            explain(err, 'flumeview-reduce-cascade.get: key not found:' + key)
          )
        }

        log.get(seq, function (err, value) {
          if (err) {
            if (err.code === 'flumelog:deleted') {
              return db.del(key, (delErr) => {
                if (delErr) {
                  return cb(
                    explain(
                      delErr,
                      'when trying to delete:' +
                      key +
                      'at since:' +
                      log.since.value
                    )
                  )
                }

                cb(err, null, seq)
              })
            }

            return cb(
              explain(
                err,
                'flumeview-level.get: index for: ' +
                key +
                'pointed at:' +
                seq +
                'but log error'
              )
            )
          } else {
            cb(null, value, seq)
          }
        })
      })
    }

    function read(opts) {
      var keys = opts.keys !== false
      var values = opts.values !== false
      var seqs = opts.seqs !== false
      const upto = opts.upto || false

      opts.keys = true
      opts.values = true
      // TODO: preserve whatever the user passed in on opts...

      var lower = ltgt.lowerBound(opts)
      if (lower == null) opts.gt = null

      function format (key, seq, value) {
        return keys && values && seqs
          ? { key: key, seq: seq, value: value }
          : keys && values
          ? { key: key, value: value }
          : keys && seqs
          ? { key: key, seq: seq }
          : seqs && values
          ? { seq: seq, value: value }
          : keys
          ? key
          : seqs
          ? seq
          : value
      }

      return pull(
        pl.read(db, opts),
        pull.filter(function (op) {
          if (op.key == META) return false
          if (!upto && op.key.length == 1 && op.key[0] == undefined) return false
          return true
        }),
        values
        ? pull(
          Paramap(function (data, cb) {
            if (data.sync) return cb(null, data)
            if (data.type === 'del') return cb(null, null)

            log.get(data.value, function (err, value) {
              if (err) {
                if (err.code === 'flumelog:deleted') {
                  return db.del(data.key, (delErr) => {
                    if (delErr) {
                      return cb(
                        explain(
                          err,
                          'when trying to delete:' +
                          data.key +
                          'at since:' +
                          log.since.value
                        )
                      )
                    }

                    cb(null, null)
                  })
                }

                cb(
                  explain(
                    err,
                    'when trying to retrive:' +
                    data.key +
                    'at since:' +
                    log.since.value
                  )
                )
              } else cb(null, format(data.key, data.value, value))
            })
          }),
          pull.filter()
        )
        : pull.map(function (data) {
          return format(data.key, data.value, null)
        })
      )
    }

  }

}
