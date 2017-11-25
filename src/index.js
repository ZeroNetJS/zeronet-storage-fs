/* eslint-disable max-nested-callbacks */

'use strict'

const fs = require('fs')
const path = require('path')
const mkdirp = require('mkdirp')
const jsonfile = require('jsonfile')
const series = require('async/series')
const waterfall = require('async/waterfall')

const toPull = require('stream-to-pull-stream')

/**
 * Bare filesystem storage for ZeroNetJS
 * @param {string} folder - directory to store files
 * @namespace StorageFS
 * @constructor
 */
module.exports = function ZeroNetStorageFS (folder) {
  // simple storage provider using the bare filesystem
  // NOTE2SELF: new providers will have "folder" and optional file

  /**
   * @param {...string} arguments - List of strings to join with the root folder using path.json
   * @returns {string} path
   * @private
   */
  function getPath () {
    const a = [...arguments]
    a.unshift(folder)
    return path.join.apply(path, a)
  }

  const self = this

  self.file = {
    // NOTE2SELF: version will be some kind of thing used in updating zites
    /**
     * @param {string} zite - Address of the zite
     * @param {integer} version - Version/Timestamp of the file
     * @param {string} innerPath - Path of the file relative to the zite
     * @param {function} cb - `err`: the filesystem error, `exists`: if the file exists
     * @returns {undefined}
     */
    exists: (zite, version, innerPath, cb) => cb(null, fs.existsSync(getPath(zite, innerPath))),
    /**
     * @param {string} zite - Address of the zite
     * @param {integer} version - Version/Timestamp of the file
     * @param {string} innerPath - Path of the file relative to the zite
     * @param {function} cb - `err`: the filesystem error, `data`: the data of the file as buffer
     * @returns {undefined}
     */
    read: (zite, version, innerPath, cb) => fs.readFile(getPath(zite, innerPath), cb),
    /**
     * @param {string} zite - Address of the zite
     * @param {integer} version - Version/Timestamp of the file
     * @param {string} innerPath - Path of the file relative to the zite
     * @param {data} data - The data to be written
     * @param {function} cb - `err`: the filesystem error
     * @returns {undefined}
     */
    write: (zite, version, innerPath, data, cb) => series([
      cb => mkdirp(path.dirname(getPath(zite, innerPath)), cb),
      cb => fs.writeFile(getPath(zite, innerPath), data, cb)
    ], cb),
    /**
     * @param {string} zite - Address of the zite
     * @param {integer} version - Version/Timestamp of the file
     * @param {string} innerPath - Path of the file relative to the zite
     * @param {function} cb - `err`: the filesystem error
     * @returns {undefined}
     */
    remove: (zite, version, innerPath, cb) => fs.unlink(getPath(zite, innerPath), cb),
    /**
     * NOTE: the function will return an error if the file does not exist
     * @param {string} zite - Address of the zite
     * @param {integer} version - Version/Timestamp of the file
     * @param {string} innerPath - Path of the file relative to the zite
     * @param {function} cb - `err`: the filesystem error, 'stream': the read stream
     * @returns {undefined}
     */
    readStream: (zite, version, innerPath, cb) => {
      try {
        cb(null, toPull.source(fs.createReadStream(getPath(zite, innerPath))))
      } catch (e) {
        cb(e)
      }
    },
    /**
     * NOTE: the function will return an error if the file does not exist
     * @param {string} zite - Address of the zite
     * @param {integer} version - Version/Timestamp of the file
     * @param {string} innerPath - Path of the file relative to the zite
     * @param {function} cb - `err`: the filesystem error, 'stream': the write stream
     * @returns {undefined}
     */
    writeStream: (zite, version, innerPath, cb) => waterfall([
      cb => mkdirp(path.dirname(getPath(zite, innerPath)), cb),
      (_, cb) => cb(null, toPull.sink(fs.createWriteStream(getPath(zite, innerPath))))
    ], cb)
  }

  self.json = {
    exists: (key, cb) => cb(null, fs.existsSync(getPath('json', key))),
    read: (key, cb, ig) => {
      jsonfile.readFile(getPath('json', key), (Err, data) => {
        if (Err && ig) return cb(Err)
        if (Err) {
          self.json.exists(key + '.bak', (err, res) => { // backup exists. something happend.
            if (err) return cb(err)
            if (res) {
              self.json.exists(key, (err, res2) => {
                if (err) return cb(err)
                if (res2) { // orig file exists too - corrupt
                  console.warn('STORAGE WARNGING: JSON FILE %s POTENTIALLY GOT CORRUPTED! CREATING BACKUP %s!', getPath('json', key), getPath('json', key + '.corrupt'))
                  fs.rename(getPath('json', key), getPath('json', key + '.corrupt'), err => {
                    if (err) return cb(err)
                    self.json.read(key + '.bak', (err, data) => {
                      if (err) {
                        console.warn('UNRECOVEREABLE!')
                        return cb(err)
                      } else {
                        console.warn('READING BACKUP %s SUCCEDED!', getPath('json', key + '.bak'))
                        self.json.write(key, data, err => {
                          if (err) return cb(err)
                          cb(null, data)
                        })
                      }
                    }, true)
                  })
                } else { // just didn't rename
                  fs.rename(getPath('json', key + '.bak'), getPath('json', key), err => {
                    if (err) return cb(err)
                    self.json.read(key, cb)
                  })
                }
              })
            } else return cb(Err) // just ENOTFOUND or permissions
          })
        } else return cb(null, data)
      })
    },
    write: (key, data, cb) => {
      series([
        cb => self.json.exists(getPath('json', key + '.bak'), (err, res) => {
          if (err) return cb(err)
          if (res) fs.unlink(getPath('json', key + '.bak'), cb)
          else cb()
        }),
        cb => self.json.exists(getPath('json', key), (err, res) => {
          if (err) return cb(err)
          if (res) fs.rename(getPath('json', key), getPath('json', key + '.bak'), cb)
          else cb()
        }),
        cb => jsonfile.writeFile(getPath('json', key), data, cb)
      ], cb)
    },
    remove: (key, cb) => fs.unlink(getPath('json', key), cb)
  }

  self.start = cb => mkdirp(getPath('json'), cb)

  self.stop = cb => cb() // Normally this would unload any dbs
}
