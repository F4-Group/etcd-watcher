var util = require('util');
var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');
var async = require('async');
var http = require('http');

// increase the socket limit used in node-etcd
http.globalAgent.maxSockets = Infinity;

module.exports = {
    watcher: watcher,
};

function watcher(etcd, keys) {
    return new Watcher(etcd, keys);
}

util.inherits(Watcher, EventEmitter);
function Watcher(etcd, options) {
    this.etcd = etcd;
    if (_.isArray(options))
        options = convertLegacyArrayOptions(options);
    this.options = options;
    this.watchers = null;
}

function convertLegacyArrayOptions(keys) {
    return _.object(keys, _.map(keys, function (key) {
        return {
            required: true,
        };
    }));
}

function isKeyRequired(keyOptions, key) {
    return keyOptions.required === true;
}

function getEtcdKey(keyOptions, key) {
    return keyOptions.etcd || key;
}

Watcher.prototype.wait = function (cb) {
    var etcd = this.etcd;
    var options = this.options;
    var watcher = this;
    waitKeys();

    function waitKeys() {
        var tasks = _.object(_.keys(options), _.map(options, function (keyOptions, key) {
            var etcdKey = getEtcdKey(keyOptions, key);
            if (isKeyRequired(keyOptions, key)) {
                return function (cb) {
                    waitSingleKey(etcdKey, cb);
                };
            } else {
                return function (cb) {
                    etcd.get(etcdKey, function (err, value) {
                        if (!err) {
                            cb(null, value);
                        } else if (err.errorCode == 100 || (err.error && err.error.errorCode == 100)) { //key not found
                            cb(null, err);
                        } else {
                            cb(err);
                        }
                    });
                };
            }
        }));
        async.parallel(tasks, onSet);
    }

    function waitSingleKey(etcdKey, waitKeyCallback) {
        etcd.get(etcdKey, function (err, res) {
            if (!err) {
                waitKeyCallback(null, res);
            } else if (err.error.errorCode == 100) { //key not found
                watcher.emit('missing', etcdKey, err);
                var watchIndex = 1;
                if ('index' in err.error)
                    watchIndex = err.error.index + 1;
                etcd.watchIndex(etcdKey, watchIndex, function (err, res) {
                    if (!err && res.action == 'set') {
                        waitKeyCallback(null, res);
                    } else {
                        waitSingleKey(etcdKey, waitKeyCallback);
                    }
                });
            } else {
                watcher.emit('error', err);
                process.nextTick(_.partial(waitSingleKey, etcdKey, waitKeyCallback));
            }
        });
    }

    function onSet(err, results) {
        if (err) {
            cb(err);
        } else {
            var values = _.object(_.map(results, function (res, key) {
                var value = res.node && res.node.value || options[key].default;
                return [key, value];
            }));
            cb(null, values);
            watcher.watchers = _.map(options, function (keyOptions, key) {
                var etcdKey = getEtcdKey(keyOptions, key);
                var result = results[key];
                var prevIndex = (result.node && result.node.modifiedIndex)
                    || (result.error && result.error.index);
                var etcdWatcher = etcd.watcher(etcdKey, prevIndex + 1);
                etcdWatcher.on("change", function (val) {
                    values[key] = val.node.value;
                    watcher.emit('change', values);
                });
                return etcdWatcher;
            });
        }
    }

};

Watcher.prototype.stop = function () {
    if (this.watchers) {
        _.each(this.watchers, function (w) {
            w.stop();
        });
    }
};
