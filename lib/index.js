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
function Watcher(etcd, keys) {
    this.etcd = etcd;
    this.keys = keys;
    this.watchers = null;
}

Watcher.prototype.wait = function (cb) {
    var etcd = this.etcd;
    var keys = this.keys;
    var watcher = this;
    waitKeys();

    function waitKeys() {
        async.map(keys, function (key, perKeyCallback) {
            waitSingleKey(key, perKeyCallback);
        }, function (err, res) {
            onSet(err, _.object(keys, res));
        });
    }

    function waitSingleKey(key, waitKeyCallback) {
        etcd.get(key, function (err, res) {
            if (!err) {
                waitKeyCallback(null, res);
            } else if (err.error.errorCode == 100) { //key not found
                watcher.emit('missing', key, err);
                var watchIndex = 1;
                if ('index' in err.error)
                    watchIndex = err.error.index + 1;
                etcd.watchIndex(key, watchIndex, function (err, res) {
                    if (!err && res.action == 'set') {
                        waitKeyCallback(null, res);
                    } else {
                        waitSingleKey(key, waitKeyCallback);
                    }
                });
            } else {
                watcher.emit('error', err);
                process.nextTick(_.partial(waitSingleKey, key, waitKeyCallback));
            }
        });
    }

    function onSet(err, results) {
        if (err) {
            cb(err);
        } else {
            var values = _.object(_.map(results, function (value, key) {
                return [key, value.node.value];
            }));
            cb(null, values);
            watcher.watchers = _.map(keys, function (key) {
                var watcher = etcd.watcher(key, results[key].node.modifiedIndex + 1);
                watcher.on("change", function (val) {
                    values[key] = val.node.value;
                    watcher.emit('change', values);
                });
                return watcher;
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
