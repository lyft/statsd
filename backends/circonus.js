/*
 * Flush stats to circonus
 *
 * To enable this backend, include 'circonus' in the backends
 * configuration array:
 *
 *   backends: ['circonus']
 *
 * This backend supports the following config options:
 *
 *   circonusHttpTrapUrl: url to submit data to
 */

var https = require('https'),
    util = require('util'),
    url_parse = require('url').parse;

var debug;
var flushInterval;

var circonusHttpTrapUrl;
var circonusStats = {};

var post_stats = function circonus_post_stats(payload) {
  var last_flush = circonusStats.last_flush || 0;
  var last_exception = circonusStats.last_exception || 0;

  var parsed_host = url_parse(circonusHttpTrapUrl);

  if (debug) {
      util.log('Parsed Host: ' + JSON.stringify(parsed_host) + "\n");
  }
  var options = {
      host: parsed_host["hostname"],
      port: parsed_host["port"] || 443,
      path: parsed_host["pathname"],
      method: 'PUT',
      headers: {
          "Content-Type": "application/json",
          "User-Agent" : "StatsdCirconusBackend/1"
      }
  };
  var req = https.request(options, function(res) {
      if (debug) {
          util.log('RES STATUS: ' + res.statusCode);
          util.log('RES HEADERS: ' + JSON.stringify(res.headers));
          res.setEncoding('utf8');
          res.on('data', function (chunk) {
              util.log('RES BODY: ' + chunk);
          });
      }
  });
  req.on('error', function(e) {
      util.log('problem with request: ' + e.message);
  });
  payload = JSON.stringify(payload);
  util.log('Request Body: ' + payload + '\n');
  req.write(payload);
  req.end();
  circonusStats.last_flush = Math.round(new Date().getTime() / 1000);
  circonusStats.last_exception = Math.round(new Date().getTime() / 1000);
}

var flush_stats = function circonus_flush(ts, metrics) {
  var starttime = Date.now();
  var stats = {};
  var numStats = 0;
  var key;

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var pctThreshold = metrics.pctThreshold;

  for (key in counters) {
    var value = counters[key];
    var valuePerSecond = value / (flushInterval / 1000); // calculate "per second" rate

    stats[key + '.counter.rate'] = valuePerSecond;
    stats[key + '.counter.counts'] = value;

    numStats += 1;
  }

  for (key in timers) {
    if (timers[key].length > 0) {
      var values = timers[key].sort(function (a,b) { return a-b; });
      var count = values.length;
      var min = values[0];
      var max = values[count - 1];

      var cumulativeValues = [min];
      for (var i = 1; i < count; i++) {
          cumulativeValues.push(values[i] + cumulativeValues[i-1]);
      }

      var sum = min;
      var mean = min;
      var maxAtThreshold = max;

      var key2;

      for (key2 in pctThreshold) {
        var pct = pctThreshold[key2];
        if (count > 1) {
          var thresholdIndex = Math.round(((100 - pct) / 100) * count);
          var numInThreshold = count - thresholdIndex;

          maxAtThreshold = values[numInThreshold - 1];
          sum = cumulativeValues[numInThreshold - 1];
          mean = sum / numInThreshold;
        }

        var clean_pct = '' + pct;
        clean_pct.replace('.', '_');
        stats[key + '.timer.mean.'  + clean_pct] = mean;
        stats[key + '.timer.upper.' + clean_pct] =  maxAtThreshold;
        stats[key + '.timer.sum.' + clean_pct] = sum;
      }

      sum = cumulativeValues[count-1];
      mean = sum / count;

      var sumOfDiffs = 0;
      for (var i = 0; i < count; i++) {
         sumOfDiffs += (values[i] - mean) * (values[i] - mean);
      }
      var stddev = Math.sqrt(sumOfDiffs / count);

      stats[key + '.timer.std'] = stddev;
      stats[key + '.timer.upper'] = max;
      stats[key + '.timer.lower'] = min;
      stats[key + '.timer.count'] = count;
      stats[key + '.timer.sum'] =  sum;
      stats[key + '.timer.mean'] =  mean;

      numStats += 1;
    }
  }

  for (key in gauges) {
    stats[key + '.gauge'] =  gauges[key];
    numStats += 1;
  }

  for (key in sets) {
    stats[key + '.set.count'] = sets[key].values().length;
    numStats += 1;
  }

  stats['statsd.numStats'] = numStats;
  stats['statsd.circonusStats.calculationTime'] =  + (Date.now() - starttime);
  post_stats(stats);
};

var backend_status = function circonus_status(writeCb) {
  for (stat in circonusStats) {
    writeCb(null, 'circonus', stat, circonusStats[stat]);
  }
};

exports.init = function circonus_init(startup_time, config, events) {
  debug = config.debug;

  circonusHttpTrapUrl = config.circonusHttpTrapUrl;

  circonusStats.last_flush = startup_time;
  circonusStats.last_exception = startup_time;

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
