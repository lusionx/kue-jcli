request = require 'request'
async   = require 'async'
_       = require 'lodash'
program = require 'commander'
log4js  = require 'log4js'

logger  = log4js.getLogger 'default'

uri = [
  'http://page.socialmaster.com.cn/kue'
  'http://127.0.0.1:8501'
  'http://kue.x.socialmaster.cn'
]

ADDR = null

defaultOption =
  state: 'complete'
  slice: '0..999'
  order: 'asc'

queryJobs = (opt, callback) ->
  opt = _.extend {}, defaultOption, opt
  uri = [ADDR, "jobs", opt.type, opt.state, opt.slice, opt.order]
  uri = _.compact(uri).join '/'
  logger.info 'request %s', uri
  request uri, (err, resp, body) ->
    logger.error err if err
    logger.warn 'resp %d %s', resp.statusCode, body if resp.statusCode isnt 200
    try obj = JSON.parse body
    return callback null if not obj
    callback null, obj

deleteJob = (job, callback) ->
  par =
    method: 'DELETE'
    uri: [ADDR, 'job', job.id].join '/'
  request par, (err, resp, body) ->
    logger.info body
    callback null

changeState = (state) ->
  (job, callback) ->
    par =
      method: 'PUT'
      uri: [ADDR, 'job', job.id, 'state', state].join '/'
    request par, (err, resp, body) ->
      logger.info body
      callback null

copyJob = (url) ->
  (job, callback) ->
    par =
      method: 'POST'
      uri: url
      json: _.pick job, ['type', 'data']
    request par, (err, resp, body) ->
      logger.info body
      callback null


main = () ->
  program.version '0.0.1'
    .option '-d --database <root>', 'json api root'
    .option '-s --state [name]', 'one of active inactive failed complete delayed'
    .option '-t --type [name]', 'kue type name'
    .option '-q --query [json]', 'filter jobs eg. {"data.uid": 12312321}'
    .option '--slice [x..y]', 'default 0..999'
    .option '--delete', 'delete job by id'
    .option '--change [state]', 'change job state'
    .option '--copy [url]', 'create job on other kue endWith "/job"'
    .parse process.argv

  if not program.database
    return program.help()
  if /^\d+$/.test program.database
    ADDR = uri[+program.database]
  else
    ADDR = program.database
  opt = _.pick program, ['state', 'type', 'slice']
  queryJobs opt, (err, list) ->
    if program.query
      q = JSON.parse program.query
      list = _.filter list, (e) ->
        _.all _.map q, (v, k) -> v is _.get e, k.split('.')
    if program.delete
      async.eachLimit list, 50, deleteJob, ->
    else if program.change
      async.eachLimit list, 50, changeState(program.change), ->
    else if program.copy
      async.eachLimit list, 20, copyJob(program.copy), ->
    else
      _.each list, (e) ->
        logger.info 'get job %d %s %j', e.id, e.state, e.data

module.exports = {main}
