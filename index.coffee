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

fetchStats = (opt) ->
  opt = _.extend {}, defaultOption, opt
  request program.database + '/stats', (err, resp, body) ->
    return logger.error err if err
    logger.info body
  request program.database + '/job/types', (err, resp, body) ->
    return logger.error err if err
    s = ['inactive']
    s.push v if v = opt.state
    _.each s, (ss) ->
      _.each JSON.parse(body), (tt) ->
        request program.database + "/jobs/#{tt}/#{ss}/stats", (err, resp, body) ->
          logger.info ss, tt, body if body

main = () ->
  program.version '0.1.1'
    .option '-d --database <root>', 'json api root'
    .option '-s --state [name]', 'active inactive failed complete delayed'
    .option '-t --type [name]', 'kue type name'
    .option '--slice [x..y]', 'default 0..999'
    .option '-q --query [json]', 'after get jobs, filter eg. {"data.uid": 12312321}'
    .option '--stats', 'get stats info'
    .option '--delete', 'after query, delete job by id'
    .option '--change [state]', 'after query, change job state'
    .option '--copy [url]', 'clone job to other kue(endWith "/job")'
    .parse process.argv

  if not ADDR = program.database
    return program.help()
  opt = _.pick program, ['state', 'type', 'slice']
  return fetchStats(opt) if program.stats
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
